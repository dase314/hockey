#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeLogPart.h>


namespace DB
{

class Block;
class StorageMergeTree;


class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    MergeTreeBlockOutputStream(StorageMergeTree & storage_, const StorageMetadataPtr metadata_snapshot_, size_t max_parts_per_block_, bool optimize_on_insert_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , max_parts_per_block(max_parts_per_block_)
        , optimize_on_insert(optimize_on_insert_)
    {
    }

    Block getHeader() const override;
    void write(const Block & block) override;
    void writePrefix() override;
    void flushDeltaData(MergeTreeLogPartPtr & log_part);

private:
    StorageMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t max_parts_per_block;
    bool optimize_on_insert;
};

}
