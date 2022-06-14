#include <Storages/MergeTree/MergeTreeLogPart.h>

namespace DB
{
class LogPartSource : public SourceWithProgress
{
public:
    LogPartSource(const Names & column_names_, MergeTreeLogPart & log_part_, const StorageMetadataPtr & metadata_snapshot)
        : SourceWithProgress(
            metadata_snapshot->getSampleBlockForColumns(column_names_, log_part_.storage.getVirtuals(), log_part_.storage.getStorageID()))
        , column_names(column_names_.begin(), column_names_.end())
        , log_part(log_part_) {}

    String getName() const override { return "LogPartSource"; }

protected:
    Chunk generate() override
    {
        Chunk res;

        if (has_been_read)
            return res;
        has_been_read = true;


        if (!log_part.rows())
            return res;

        Columns columns;
        {
            std::unique_lock<std::mutex> lock(log_part.log_mutex);
            if(!log_part.close)
            {
                columns.resize(column_names.size());

                for (size_t i = 0; i < column_names.size(); i++)
                {
                    const auto & name = column_names[i];
                    if(columns[i] == nullptr)
                        columns[i] = log_part.data.getByName(name).type->createColumn();;
                    
                    const auto & block_column = log_part.data.getByName(name).column;
                    auto mutable_column = columns[i]->assumeMutable();
                    mutable_column->insertRangeFromPmem(*block_column, 0, log_part.rows());
                    columns[i] = std::move(mutable_column);

                }
            } else {
                return res;
            }
        }
        

        UInt64 size = columns.at(0)->size();
        res.setColumns(std::move(columns), size);

        return res;
    }

private:
    Names column_names;
    MergeTreeLogPart & log_part;
    bool has_been_read = false;
};

}