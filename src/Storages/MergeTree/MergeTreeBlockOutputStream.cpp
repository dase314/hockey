#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Interpreters/PartLog.h>


namespace DB
{

Block MergeTreeBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}


void MergeTreeBlockOutputStream::writePrefix()
{
    /// Only check "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded();
}


void MergeTreeBlockOutputStream::write(const Block & block)
{
    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot);
    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;
        const auto max_log_part_rows = storage.getSettings()->max_log_part_rows;
        const auto enable_log_part = storage.getSettings()->enable_log_part;
        
        /// Rows threshold for writing to log part is temporarily setting for SSB.
        if (!enable_log_part || 
        (enable_log_part && current_block.block.rows() >= max_log_part_rows)) {
            MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, metadata_snapshot, optimize_on_insert);
            storage.renameTempPartAndAdd(part, &storage.increment);

            PartLog::addNewPart(storage.global_context, part, watch.elapsed());
        } else { /// dump to log part
            MergeTreePartition partition(std::move(current_block.partition));

            auto log_part = storage.getOrCreateActiveLogPart(partition, metadata_snapshot);
            log_part->appendBlock(current_block.block);

            // check if need to flush to normal part, after append block.
            if(log_part->rows() >= max_log_part_rows)
            {
                flushDeltaData(log_part);
                storage.dropLogPart(partition, metadata_snapshot);
            }
        }

        /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
        storage.background_executor.triggerTask();
    }
}

void MergeTreeBlockOutputStream::flushDeltaData(MergeTreeLogPartPtr & log_part)
{
    Stopwatch watch;
    log_part->state = MergeTreeLogPart::LogState::Flushing;
    for(auto & it : log_part->data)
    {
        auto mut_col = it.column->assumeMutable();
        mut_col->insertMemRangeFrom(0, log_part->rows());
    }
    auto tmp_block = BlockWithPartition(std::move(log_part->data),std::move(log_part->partition.value));
    MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(tmp_block, metadata_snapshot, optimize_on_insert);
    storage.renameTempPartAndAdd(part, &storage.increment);
    log_part->shutdown();

    PartLog::addNewPart(storage.global_context, part, watch.elapsed());
    log_part->state = MergeTreeLogPart::LogState::Completed;
}

}
