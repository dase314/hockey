#include <Storages/MergeTree/MergeTreeReaderPMCS.h>
#include <Storages/MergeTree/MergeTreeDataPartPMCS.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <Columns/ColumnArray.h>
#include <Poco/File.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


MergeTreeReaderPMCS::MergeTreeReaderPMCS(
    DataPartPMCSPtr data_part_,
    NamesAndTypesList columns_,
    const StorageMetadataPtr & metadata_snapshot_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_)
    : IMergeTreeReader(data_part_, std::move(columns_), metadata_snapshot_,
        nullptr, nullptr, std::move(mark_ranges_),
        std::move(settings_), {})
    , part_in_pmcs(std::move(data_part_))
{
}

/// TODO: check podarray release memory, and how to use continue_reading.
/// future work: more test, may be have problem. restart pmcs support. do we need checksum support?
size_t MergeTreeReaderPMCS::readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
    if (!continue_reading)
        total_rows_read = 0;
    //assert(!continue_reading);
    size_t total_marks = data_part->index_granularity.getMarksCount();
    if (from_mark >= total_marks)
        throw Exception("Mark " + toString(from_mark) + " is out of bound. Max mark: "
            + toString(total_marks), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    size_t num_columns = res_columns.size();
    checkNumberOfColumns(num_columns);

    size_t part_rows = part_in_pmcs->block.rows();
    if (total_rows_read >= part_rows)
        throw Exception("Cannot read data in MergeTreeReaderPMCS. Rows already read: "
            + toString(total_rows_read) + ". Rows in part: " + toString(part_rows), ErrorCodes::CANNOT_READ_ALL_DATA);

    size_t index_granularity_for_block = part_in_pmcs->storage.getSettings()->index_granularity;
    size_t off_rows = from_mark * index_granularity_for_block;
    assert(part_rows - off_rows > 0);
    size_t rows_to_read = std::min(max_rows_to_read, part_rows - off_rows);
    auto column_it = columns.begin();
    for (size_t i = 0; i < num_columns; ++i, ++column_it)
    {
        auto [name, type] = getColumnFromPart(*column_it);
        
        if (part_in_pmcs->block.has(name))
        {
            const auto & block_column = part_in_pmcs->block.getByName(name).column;
            if (rows_to_read == part_rows)
            {
                res_columns[i] = block_column;
            }
            else
            {
                if (res_columns[i] == nullptr)
                    res_columns[i] = type->createColumn();

                auto mutable_column = res_columns[i]->assumeMutable();
                mutable_column->insertRangeFrom(*block_column, off_rows, rows_to_read);
                res_columns[i] = std::move(mutable_column);
            }
        }
    }

    total_rows_read += rows_to_read;
    return rows_to_read;
}

}
