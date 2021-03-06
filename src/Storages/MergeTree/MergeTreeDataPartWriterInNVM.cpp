#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressionFactory.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterInNVM.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}

namespace
{
    /// Get granules for block using index_granularity
    Granules getGranulesToWrite(
        const MergeTreeIndexGranularity & index_granularity, size_t block_rows, size_t current_mark, size_t rows_written_in_last_mark)
    {
        if (current_mark >= index_granularity.getMarksCount())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Request to get granules from mark {} but index granularity size is {}",
                current_mark,
                index_granularity.getMarksCount());

        Granules result;
        size_t current_row = 0;
        /// When our last mark is not finished yet and we have to write in rows into it
        if (rows_written_in_last_mark > 0)
        {
            size_t rows_left_in_last_mark = index_granularity.getMarkRows(current_mark) - rows_written_in_last_mark;
            size_t rows_left_in_block = block_rows - current_row;
            result.emplace_back(Granule{
                .start_row = current_row,
                .rows_to_write = std::min(rows_left_in_block, rows_left_in_last_mark),
                .mark_number = current_mark,
                .mark_on_start = false, /// Don't mark this granule because we have already marked it
                .is_complete = (rows_left_in_block >= rows_left_in_last_mark),
            });
            current_row += result.back().rows_to_write;
            current_mark++;
        }

        /// Calculating normal granules for block
        while (current_row < block_rows)
        {
            size_t expected_rows_in_mark = index_granularity.getMarkRows(current_mark);
            size_t rows_left_in_block = block_rows - current_row;
            /// If we have less rows in block than expected in granularity
            /// save incomplete granule
            result.emplace_back(Granule{
                .start_row = current_row,
                .rows_to_write = std::min(rows_left_in_block, expected_rows_in_mark),
                .mark_number = current_mark,
                .mark_on_start = true,
                .is_complete = (rows_left_in_block >= expected_rows_in_mark),
            });
            current_row += result.back().rows_to_write;
            current_mark++;
        }

        return result;
    }

}
void MergeTreeDataPartWriterInNVM::NVMStream::finalize()
{
    compressed.next();
    /// 'compressed_buf' doesn't call next() on underlying buffer ('plain_hashing'). We should do it manually.
    plain_hashing.next();
    marks.next();

    plain_file->finalize();
    marks_file->finalize();
}

void MergeTreeDataPartWriterInNVM::NVMStream::sync() const
{
    plain_file->sync();
    marks_file->sync();
}
std::unique_ptr<WriteBufferFromNVM> MergeTreeDataPartWriterInNVM::NVMStream::createNVMFile(const String & path, size_t file_size)
{
    return std::make_unique<WriteBufferFromNVM>(path, file_size);
}
MergeTreeDataPartWriterInNVM::NVMStream::NVMStream(
    const String & escaped_column_name_,
    DiskPtr disk_,
    const String & data_path_,
    const std::string & data_file_extension_,
    const std::string & marks_path_,
    const std::string & marks_file_extension_,
    const CompressionCodecPtr & compression_codec_)
    : escaped_column_name(escaped_column_name_)
    , data_file_extension{data_file_extension_}
    , marks_file_extension{marks_file_extension_}
    ,
    //plain_file(disk_->writeFile(data_path_ + data_file_extension, max_compress_block_size_, WriteMode::Rewrite, estimated_size_, aio_threshold_)),
    plain_file(createNVMFile(disk_->getPath() + data_path_ + data_file_extension, DBMS_DEFAULT_BUFFER_SIZE * 8))
    , plain_hashing(*plain_file)
    , compressed_buf(plain_hashing, compression_codec_)
    , compressed(compressed_buf)
    ,
    //marks_file(disk_->writeFile(marks_path_ + marks_file_extension, 4096, WriteMode::Rewrite)), marks(*marks_file)
    marks_file(createNVMFile(disk_->getPath() + marks_path_ + marks_file_extension, DBMS_DEFAULT_BUFFER_SIZE/128))
    , marks(*marks_file)
{ //TODO
}

void MergeTreeDataPartWriterInNVM::NVMStream::addToChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    String name = escaped_column_name;

    checksums.files[name + data_file_extension].is_compressed = true;
    checksums.files[name + data_file_extension].uncompressed_size = compressed.count();
    checksums.files[name + data_file_extension].uncompressed_hash = compressed.getHash();
    checksums.files[name + data_file_extension].file_size = plain_hashing.count();
    checksums.files[name + data_file_extension].file_hash = plain_hashing.getHash();

    checksums.files[name + marks_file_extension].file_size = marks.count();
    checksums.files[name + marks_file_extension].file_hash = marks.getHash();
}

MergeTreeDataPartWriterInNVM::MergeTreeDataPartWriterInNVM(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : MergeTreeDataPartWriterOnDisk(
        data_part_,
        columns_list_,
        metadata_snapshot_,
        indices_to_recalc_,
        marks_file_extension_,
        default_codec_,
        settings_,
        index_granularity_)
{
    const auto & columns = metadata_snapshot->getColumns();
    for (const auto & it : columns_list)
        addStreams(it.name, *it.type, columns.getCodecDescOrDefault(it.name, default_codec));
}

void MergeTreeDataPartWriterInNVM::addStreams(const String & name, const IDataType & type, const ASTPtr & effective_codec_desc)
{
    IDataType::StreamCallback callback = [&](const IDataType::SubstreamPath & substream_path, const IDataType & substream_type) {
        String stream_name = IDataType::getFileNameForStream(name, substream_path);
        /// Shared offsets for Nested type.
        if (column_streams.count(stream_name))
            return;

        CompressionCodecPtr compression_codec;
        /// If we can use special codec then just get it
        if (IDataType::isSpecialCompressionAllowed(substream_path))
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, &substream_type, default_codec);
        else /// otherwise return only generic codecs and don't use info about the data_type
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, nullptr, default_codec, true);

        column_streams[stream_name] = std::make_unique<NVMStream>(
            stream_name,
            data_part->volume->getDisk(),
            part_path + stream_name,
            DATA_FILE_EXTENSION,
            part_path + stream_name,
            marks_file_extension,
            compression_codec);
    };

    IDataType::SubstreamPath stream_path;
    type.enumerateStreams(callback, stream_path);
}


IDataType::OutputStreamGetter
MergeTreeDataPartWriterInNVM::createStreamGetter(const String & name, WrittenOffsetColumns & offset_columns) const
{
    return [&, this](const IDataType::SubstreamPath & substream_path) -> WriteBuffer * {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;

        String stream_name = IDataType::getFileNameForStream(name, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.count(stream_name))
            return nullptr;

        return &column_streams.at(stream_name)->compressed;
    };
}


void MergeTreeDataPartWriterInNVM::shiftCurrentMark(const Granules & granules_written)
{
    auto last_granule = granules_written.back();
    /// If we didn't finished last granule than we will continue to write it from new block
    if (!last_granule.is_complete)
    {
        /// Shift forward except last granule
        setCurrentMark(getCurrentMark() + granules_written.size() - 1);
        bool still_in_the_same_granule = granules_written.size() == 1;
        /// We wrote whole block in the same granule, but didn't finished it.
        /// So add written rows to rows written in last_mark
        if (still_in_the_same_granule)
            rows_written_in_last_mark += last_granule.rows_to_write;
        else
            rows_written_in_last_mark = last_granule.rows_to_write;
    }
    else
    {
        setCurrentMark(getCurrentMark() + granules_written.size());
        rows_written_in_last_mark = 0;
    }
}

void MergeTreeDataPartWriterInNVM::write(const Block & block, const IColumn::Permutation * permutation)
{
    /// Fill index granularity for this block
    /// if it's unknown (in case of insert data or horizontal merge,
    /// but not in case of vertical merge)
    if (compute_granularity)
    {
        size_t index_granularity_for_block = computeIndexGranularity(block);
        if (rows_written_in_last_mark > 0)
        {
            size_t rows_left_in_last_mark = index_granularity.getMarkRows(getCurrentMark()) - rows_written_in_last_mark;
            /// Previous granularity was much bigger than our new block's
            /// granularity let's adjust it, because we want add new
            /// heavy-weight blocks into small old granule.
            if (rows_left_in_last_mark > index_granularity_for_block)
            {
                /// We have already written more rows than granularity of our block.
                /// adjust last mark rows and flush to disk.
                if (rows_written_in_last_mark >= index_granularity_for_block)
                    adjustLastMarkIfNeedAndFlushToDisk(rows_written_in_last_mark);
                else /// We still can write some rows from new block into previous granule.
                    adjustLastMarkIfNeedAndFlushToDisk(index_granularity_for_block - rows_written_in_last_mark);
            }
        }

        fillIndexGranularity(index_granularity_for_block, block.rows());
    }

    auto granules_to_write = getGranulesToWrite(index_granularity, block.rows(), getCurrentMark(), rows_written_in_last_mark);

    auto offset_columns = written_offset_columns ? *written_offset_columns : WrittenOffsetColumns{};
    Block primary_key_block;
    if (settings.rewrite_primary_key)
        primary_key_block = getBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), permutation);

    Block skip_indexes_block = getBlockAndPermute(block, getSkipIndicesColumns(), permutation);

    auto it = columns_list.begin();
    for (size_t i = 0; i < columns_list.size(); ++i, ++it)
    {
        const ColumnWithTypeAndName & column = block.getByName(it->name);

        if (permutation)
        {
            if (primary_key_block.has(it->name))
            {
                const auto & primary_column = *primary_key_block.getByName(it->name).column;
                writeColumn(column.name, *column.type, primary_column, offset_columns, granules_to_write);
            }
            else if (skip_indexes_block.has(it->name))
            {
                const auto & index_column = *skip_indexes_block.getByName(it->name).column;
                writeColumn(column.name, *column.type, index_column, offset_columns, granules_to_write);
            }
            else
            {
                /// We rearrange the columns that are not included in the primary key here; Then the result is released - to save RAM.
                ColumnPtr permuted_column = column.column->permute(*permutation, 0);
                writeColumn(column.name, *column.type, *permuted_column, offset_columns, granules_to_write);
            }
        }
        else
        {
            writeColumn(column.name, *column.type, *column.column, offset_columns, granules_to_write);
        }
    }

    if (settings.rewrite_primary_key)
        calculateAndSerializePrimaryIndex(primary_key_block, granules_to_write);

    calculateAndSerializeSkipIndices(skip_indexes_block, granules_to_write);

    shiftCurrentMark(granules_to_write);
}

void MergeTreeDataPartWriterInNVM::writeSingleMark(
    const String & name,
    const IDataType & type,
    WrittenOffsetColumns & offset_columns,
    size_t number_of_rows,
    DB::IDataType::SubstreamPath & path)
{
    StreamsWithMarks marks = getCurrentMarksForColumn(name, type, offset_columns, path);
    for (const auto & mark : marks)
        flushMarkToFile(mark, number_of_rows);
}

void MergeTreeDataPartWriterInNVM::flushMarkToFile(const StreamNameAndMark & stream_with_mark, size_t rows_in_mark)
{
    NVMStream & stream = *column_streams[stream_with_mark.stream_name];
    writeIntBinary(stream_with_mark.mark.offset_in_compressed_file, stream.marks);
    writeIntBinary(stream_with_mark.mark.offset_in_decompressed_block, stream.marks);
    if (settings.can_use_adaptive_granularity)
        writeIntBinary(rows_in_mark, stream.marks);
}

StreamsWithMarks MergeTreeDataPartWriterInNVM::getCurrentMarksForColumn(
    const String & name, const IDataType & type, WrittenOffsetColumns & offset_columns, DB::IDataType::SubstreamPath & path)
{
    StreamsWithMarks result;
    type.enumerateStreams(
        [&](const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */) {
            bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;

            String stream_name = IDataType::getFileNameForStream(name, substream_path);

            /// Don't write offsets more than one time for Nested type.
            if (is_offsets && offset_columns.count(stream_name))
                return;

            NVMStream & stream = *column_streams[stream_name];

            /// There could already be enough data to compress into the new block.
            if (stream.compressed.offset() >= settings.min_compress_block_size)
                stream.compressed.next();

            StreamNameAndMark stream_with_mark;
            stream_with_mark.stream_name = stream_name;
            stream_with_mark.mark.offset_in_compressed_file = stream.plain_hashing.count();
            stream_with_mark.mark.offset_in_decompressed_block = stream.compressed.offset();

            result.push_back(stream_with_mark);
        },
        path);

    return result;
}

void MergeTreeDataPartWriterInNVM::writeSingleGranule(
    const String & name,
    const IDataType & type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    IDataType::SerializeBinaryBulkStatePtr & serialization_state,
    IDataType::SerializeBinaryBulkSettings & serialize_settings,
    const Granule & granule)
{
    type.serializeBinaryBulkWithMultipleStreams(column, granule.start_row, granule.rows_to_write, serialize_settings, serialization_state);

    /// So that instead of the marks pointing to the end of the compressed block, there were marks pointing to the beginning of the next one.
    type.enumerateStreams(
        [&](const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */) {
            bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;

            String stream_name = IDataType::getFileNameForStream(name, substream_path);

            /// Don't write offsets more than one time for Nested type.
            if (is_offsets && offset_columns.count(stream_name))
                return;

            column_streams[stream_name]->compressed.nextIfAtEnd();
        },
        serialize_settings.path);
}

/// Column must not be empty. (column.size() !== 0)
void MergeTreeDataPartWriterInNVM::writeColumn(
    const String & name, const IDataType & type, const IColumn & column, WrittenOffsetColumns & offset_columns, const Granules & granules)
{
    if (granules.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Empty granules for column {}, current mark {}", backQuoteIfNeed(name), getCurrentMark());

    auto [it, inserted] = serialization_states.emplace(name, nullptr);

    if (inserted)
    {
        IDataType::SerializeBinaryBulkSettings serialize_settings;
        serialize_settings.getter = createStreamGetter(name, offset_columns);
        type.serializeBinaryBulkStatePrefix(serialize_settings, it->second);
    }

    const auto & global_settings = storage.global_context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.getter = createStreamGetter(name, offset_columns);
    serialize_settings.low_cardinality_max_dictionary_size = global_settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = global_settings.low_cardinality_use_single_dictionary_for_part != 0;

    for (const auto & granule : granules)
    {
        data_written = true;

        if (granule.mark_on_start)
        {
            if (last_non_written_marks.count(name))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "We have to add new mark for column, but already have non written mark. Current mark {}, total marks {}, offset {}",
                    getCurrentMark(),
                    index_granularity.getMarksCount(),
                    rows_written_in_last_mark);
            last_non_written_marks[name] = getCurrentMarksForColumn(name, type, offset_columns, serialize_settings.path);
        }

        writeSingleGranule(name, type, column, offset_columns, it->second, serialize_settings, granule);

        if (granule.is_complete)
        {
            auto marks_it = last_non_written_marks.find(name);
            if (marks_it == last_non_written_marks.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No mark was saved for incomplete granule for column {}", backQuoteIfNeed(name));

            for (const auto & mark : marks_it->second)
                flushMarkToFile(mark, index_granularity.getMarkRows(granule.mark_number));
            last_non_written_marks.erase(marks_it);
        }
    }

    type.enumerateStreams(
        [&](const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */) {
            bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
            if (is_offsets)
            {
                String stream_name = IDataType::getFileNameForStream(name, substream_path);
                offset_columns.insert(stream_name);
            }
        },
        serialize_settings.path);
}


void MergeTreeDataPartWriterInNVM::validateColumnOfFixedSize(const String & name, const IDataType & type)
{
    if (!type.isValueRepresentedByNumber() || type.haveSubtypes())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot validate column of non fixed type {}", type.getName());

    auto disk = data_part->volume->getDisk();
    String mrk_path = fullPath(disk, part_path + name + marks_file_extension);
    String bin_path = fullPath(disk, part_path + name + DATA_FILE_EXTENSION);
    DB::ReadBufferFromFile mrk_in(mrk_path);
    DB::CompressedReadBufferFromFile bin_in(bin_path, 0, 0, 0);
    bool must_be_last = false;
    UInt64 offset_in_compressed_file = 0;
    UInt64 offset_in_decompressed_block = 0;
    UInt64 index_granularity_rows = data_part->index_granularity_info.fixed_index_granularity;

    size_t mark_num;

    for (mark_num = 0; !mrk_in.eof(); ++mark_num)
    {
        if (mark_num > index_granularity.getMarksCount())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Incorrect number of marks in memory {}, on disk (at least) {}",
                index_granularity.getMarksCount(),
                mark_num + 1);

        DB::readBinary(offset_in_compressed_file, mrk_in);
        DB::readBinary(offset_in_decompressed_block, mrk_in);
        if (settings.can_use_adaptive_granularity)
            DB::readBinary(index_granularity_rows, mrk_in);
        else
            index_granularity_rows = data_part->index_granularity_info.fixed_index_granularity;

        if (must_be_last)
        {
            if (index_granularity_rows != 0)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "We ran out of binary data but still have non empty mark #{} with rows number {}",
                    mark_num,
                    index_granularity_rows);

            if (!mrk_in.eof())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark #{} must be last, but we still have some to read", mark_num);

            break;
        }

        if (index_granularity_rows == 0)
        {
            auto column = type.createColumn();

            type.deserializeBinaryBulk(*column, bin_in, 1000000000, 0.0);

            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Still have {} rows in bin stream, last mark #{} index granularity size {}, last rows {}",
                column->size(),
                mark_num,
                index_granularity.getMarksCount(),
                index_granularity_rows);
        }

        if (index_granularity_rows != index_granularity.getMarkRows(mark_num))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Incorrect mark rows for part {} for mark #{} (compressed offset {}, decompressed offset {}), in-memory {}, on disk {}, "
                "total marks {}",
                data_part->getFullPath(),
                mark_num,
                offset_in_compressed_file,
                offset_in_decompressed_block,
                index_granularity.getMarkRows(mark_num),
                index_granularity_rows,
                index_granularity.getMarksCount());

        auto column = type.createColumn();

        type.deserializeBinaryBulk(*column, bin_in, index_granularity_rows, 0.0);

        if (bin_in.eof())
        {
            must_be_last = true;
        }

        /// Now they must be equal
        if (column->size() != index_granularity_rows)
        {
            if (must_be_last && !settings.can_use_adaptive_granularity)
                break;

            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Incorrect mark rows for mark #{} (compressed offset {}, decompressed offset {}), actually in bin file {}, in mrk file {}",
                mark_num,
                offset_in_compressed_file,
                offset_in_decompressed_block,
                column->size(),
                index_granularity.getMarkRows(mark_num));
        }
    }

    if (!mrk_in.eof())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Still have something in marks stream, last mark #{} index granularity size {}, last rows {}",
            mark_num,
            index_granularity.getMarksCount(),
            index_granularity_rows);
    if (!bin_in.eof())
    {
        auto column = type.createColumn();

        type.deserializeBinaryBulk(*column, bin_in, 1000000000, 0.0);

        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Still have {} rows in bin stream, last mark #{} index granularity size {}, last rows {}",
            column->size(),
            mark_num,
            index_granularity.getMarksCount(),
            index_granularity_rows);
    }
}

void MergeTreeDataPartWriterInNVM::finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool sync)
{
    const auto & global_settings = storage.global_context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.low_cardinality_max_dictionary_size = global_settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = global_settings.low_cardinality_use_single_dictionary_for_part != 0;
    WrittenOffsetColumns offset_columns;
    if (rows_written_in_last_mark > 0)
        adjustLastMarkIfNeedAndFlushToDisk(rows_written_in_last_mark);

    bool write_final_mark = (with_final_mark && data_written);

    {
        auto it = columns_list.begin();
        for (size_t i = 0; i < columns_list.size(); ++i, ++it)
        {
            if (!serialization_states.empty())
            {
                serialize_settings.getter = createStreamGetter(it->name, written_offset_columns ? *written_offset_columns : offset_columns);
                it->type->serializeBinaryBulkStateSuffix(serialize_settings, serialization_states[it->name]);
            }

            if (write_final_mark)
                writeFinalMark(it->name, it->type, offset_columns, serialize_settings.path);
        }
    }
    for (auto & stream : column_streams)
    {
        stream.second->finalize();
        stream.second->addToChecksums(checksums);
        if (sync)
            stream.second->sync();
    }

    column_streams.clear();
    serialization_states.clear();

#ifndef NDEBUG
    /// Heavy weight validation of written data. Checks that we are able to read
    /// data according to marks. Otherwise throws LOGICAL_ERROR (equal to about in debug mode)
    for (const auto & column : columns_list)
    {
        if (column.type->isValueRepresentedByNumber() && !column.type->haveSubtypes())
            validateColumnOfFixedSize(column.name, *column.type);
    }
#endif
}

void MergeTreeDataPartWriterInNVM::finish(IMergeTreeDataPart::Checksums & checksums, bool sync)
{
    finishDataSerialization(checksums, sync);
    if (settings.rewrite_primary_key)
        finishPrimaryIndexSerialization(checksums, sync);

    finishSkipIndicesSerialization(checksums, sync);
}

void MergeTreeDataPartWriterInNVM::writeFinalMark(
    const std::string & column_name,
    const DataTypePtr column_type,
    WrittenOffsetColumns & offset_columns,
    DB::IDataType::SubstreamPath & path)
{
    writeSingleMark(column_name, *column_type, offset_columns, 0, path);
    /// Memoize information about offsets
    column_type->enumerateStreams(
        [&](const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */) {
            bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
            if (is_offsets)
            {
                String stream_name = IDataType::getFileNameForStream(column_name, substream_path);
                offset_columns.insert(stream_name);
            }
        },
        path);
}

static void fillIndexGranularityImpl(
    MergeTreeIndexGranularity & index_granularity, size_t index_offset, size_t index_granularity_for_block, size_t rows_in_block)
{
    for (size_t current_row = index_offset; current_row < rows_in_block; current_row += index_granularity_for_block)
        index_granularity.appendMark(index_granularity_for_block);
}

void MergeTreeDataPartWriterInNVM::fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block)
{
    if (getCurrentMark() < index_granularity.getMarksCount() && getCurrentMark() != index_granularity.getMarksCount() - 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Trying to add marks, while current mark {}, but total marks {}",
            getCurrentMark(),
            index_granularity.getMarksCount());

    size_t index_offset = 0;
    if (rows_written_in_last_mark != 0)
        index_offset = index_granularity.getLastMarkRows() - rows_written_in_last_mark;

    fillIndexGranularityImpl(index_granularity, index_offset, index_granularity_for_block, rows_in_block);
}


void MergeTreeDataPartWriterInNVM::adjustLastMarkIfNeedAndFlushToDisk(size_t new_rows_in_last_mark)
{
    /// We can adjust marks only if we computed granularity for blocks.
    /// Otherwise we cannot change granularity because it will differ from
    /// other columns
    if (compute_granularity && settings.can_use_adaptive_granularity)
    {
        if (getCurrentMark() != index_granularity.getMarksCount() - 1)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Non last mark {} (with {} rows) having rows offset {}, total marks {}",
                getCurrentMark(),
                index_granularity.getMarkRows(getCurrentMark()),
                rows_written_in_last_mark,
                index_granularity.getMarksCount());

        index_granularity.popMark();
        index_granularity.appendMark(new_rows_in_last_mark);
    }

    /// Last mark should be filled, otherwise it's a bug
    if (last_non_written_marks.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "No saved marks for last mark {} having rows offset {}, total marks {}",
            getCurrentMark(),
            rows_written_in_last_mark,
            index_granularity.getMarksCount());

    if (rows_written_in_last_mark == new_rows_in_last_mark)
    {
        for (const auto & [name, marks] : last_non_written_marks)
        {
            for (const auto & mark : marks)
                flushMarkToFile(mark, index_granularity.getMarkRows(getCurrentMark()));
        }

        last_non_written_marks.clear();

        if (compute_granularity && settings.can_use_adaptive_granularity)
        {
            /// Also we add mark to each skip index because all of them
            /// already accumulated all rows from current adjusting mark
            for (size_t i = 0; i < skip_indices.size(); ++i)
                ++skip_index_accumulated_marks[i];

            /// This mark completed, go further
            setCurrentMark(getCurrentMark() + 1);
            /// Without offset
            rows_written_in_last_mark = 0;
        }
    }
}

}
