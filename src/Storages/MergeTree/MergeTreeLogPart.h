#pragma once

#include <DataStreams/IBlockInputStream.h>

#include <Core/Row.h>
#include <Core/Block.h>
#include <common/types.h>
#include <Core/NamesAndTypes.h>
#include <Columns/IColumn.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/IStorage.h>
#include <Processors/Sources/SourceWithProgress.h>

#include <Poco/Path.h>

#include <shared_mutex>

#include <libpmemobj++/transaction.hpp>

namespace DB
{

struct ColumnSize;
class MergeTreeData;
class IReservation;
using ReservationPtr = std::unique_ptr<IReservation>;



namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// MergeTreeLogPart is independent of the IMergeTreePart.
class MergeTreeLogPart : public std::enable_shared_from_this<MergeTreeLogPart>
{
public:
    using ValueSizeMap = std::map<std::string, double>;

    using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;
    using NameToPosition = std::unordered_map<std::string, size_t>;

    MergeTreeLogPart(const MergeTreeData & storage_, MergeTreePartition & partition_, String partition_id_):
    storage(storage_),
    partition(std::move(partition_)),
    partition_id(partition_id_)
    {
        rows_count.store(0, std::memory_order_relaxed);
    }

    ~MergeTreeLogPart() {}

    bool isEmpty() const { return rows_count.load(std::memory_order_relaxed) == 0; }
    size_t rows() const { return rows_count.load(std::memory_order_relaxed); }

    const MergeTreeData & storage;
    mutable String relative_path;
 
    std::atomic<size_t> rows_count;
    MergeTreePartition partition;
    String partition_id;
    std::vector<pmem::obj::persistent_ptr<pmem::obj::string>> strs;
    std::mutex log_mutex;
    bool close = false;
    enum class LogState
    {
        Active,// can use appendBlock
        Flushing,// Flushing to mergetree data part
        Completed,// can be drop
    };

    LogState state{LogState::Active};

    bool is_flush = false;

    struct MinMaxIndex
    {
        /// A direct product of ranges for each key column. See Storages/MergeTree/KeyCondition.cpp for details.
        std::vector<Range> hyperrectangle;
        bool initialized = false;

    public:
        MinMaxIndex() = default;

        /// For month-based partitioning.
        MinMaxIndex(DayNum min_date, DayNum max_date)
            : hyperrectangle(1, Range(min_date, true, max_date, true))
            , initialized(true)
        {
        }

        void load(const MergeTreeData & data, const DiskPtr & disk_, const String & part_path);
        void loadPmem(const MergeTreeData & data, std::vector<std::pair<String,String>> raw_data);
        std::vector<std::pair<std::string,std::string>> toString(const MergeTreeData & data) const;
        void update(const Block & block, const Names & column_names);
        void merge(const MinMaxIndex & other);
    };
    
    /// TODO impl in log part.
    MinMaxIndex minmax_idx;

    /// Returns path to part dir relatively to disk mount point
    String getFullRelativePath() const;

    void appendBlock(const Block & from);
    void shutdown();

    Block data;

private:
    mutable std::mutex mutex;
    
};

using MergeTreeLogPartPtr = std::shared_ptr<MergeTreeLogPart>;
using MergeTreeLogPartPtrs = std::list<MergeTreeLogPartPtr>;

}
