#include <Storages/MergeTree/MergeTreeDataPartWriterPMCS.h>
#include <Storages/MergeTree/MergeTreeDataPartPMCS.h>
#include <Storages/MergeTree/MergeTreeWriteAheadLog.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void MergeTreeDataPartWriterPMCS::ColumnsBuffer::add(MutableColumns && columns)
{
    if (accumulated_columns.empty())
        accumulated_columns = std::move(columns);
    else
    {
        for (size_t i = 0; i < columns.size(); ++i)
            accumulated_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
    }
}

Columns MergeTreeDataPartWriterPMCS::ColumnsBuffer::releaseColumns()
{
    Columns res(std::make_move_iterator(accumulated_columns.begin()),
        std::make_move_iterator(accumulated_columns.end()));
    accumulated_columns.clear();
    return res;
}

size_t MergeTreeDataPartWriterPMCS::ColumnsBuffer::size() const
{
    if (accumulated_columns.empty())
        return 0;
    return accumulated_columns.at(0)->size();
}

MergeTreeDataPartWriterPMCS::MergeTreeDataPartWriterPMCS(
    const DataPartPMCSPtr & part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeWriterSettings & settings_)
    : IMergeTreeDataPartWriter(part_, columns_list_, metadata_snapshot_, settings_)
    , part_in_pmcs(part_) {
    // String full_path = part_in_pmcs->getFullPath() + META_FILE_NAME;
	// 	pop = pool<root>::create(full_path.c_str(), LAYOUT, POOL_SIZE, S_IWUSR | S_IRUSR);
    pmem::obj::transaction::run(part_in_pmcs->storage.pop_meta, [&] {
        meta = pmem::obj::make_persistent<Meta>();
    });
}

void MergeTreeDataPartWriterPMCS::write(
    const Block & block, const IColumn::Permutation * permutation)
{
    // if (part_in_memory->block)
    //     throw Exception("DataPartWriterPMCS supports only one write", ErrorCodes::LOGICAL_ERROR);

    // Block primary_key_block;
    // if (settings.rewrite_primary_key)
    //     primary_key_block = getBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), permutation);

    Block result_block;

    /// sort
    if (permutation)
    {
        for (const auto & col : columns_list)
        {
            auto permuted = block.getByName(col.name);
            permuted.column = permuted.column->permute(*permutation, 0);
            result_block.insert(permuted);
        }
    }
    else
    {
        for (const auto & col : columns_list)
            result_block.insert(block.getByName(col.name));
    }

    if (!header)
        header = result_block.cloneEmpty();

    columns_buffer.add(result_block.mutateColumns());

    // index_granularity.appendMark(result_block.rows());
    // if (with_final_mark)
    //     index_granularity.appendMark(0);
    // part_in_memory->block = std::move(result_block);

    // if (settings.rewrite_primary_key)
    //     calculateAndSerializePrimaryIndex(primary_key_block);
}

void MergeTreeDataPartWriterPMCS::calculatePrimaryIndex(const Block & primary_index_block)
{
    size_t rows = primary_index_block.rows();
    if (!rows)
        return;

    size_t primary_columns_num = primary_index_block.columns();
    index_columns.resize(primary_columns_num);
    index_types = primary_index_block.getDataTypes();
    index_names = primary_index_block.getNames();
    for (size_t i = 0; i < primary_columns_num; ++i)
    {
        const auto & primary_column = *primary_index_block.getByPosition(i).column;
        index_columns[i] = primary_column.cloneEmpty();
        size_t marks_cnt = index_granularity.getMarksCount();
        for(size_t pos=0; pos < marks_cnt; ++pos)
        index_columns[i]->insertFrom(primary_column, index_granularity.getMarkStartingRow(pos));
        // index_columns[i]->insertFrom(primary_column, 0);
        // if (with_final_mark)
        //     index_columns[i]->insertFrom(primary_column, rows - 1);
    }
}

void MergeTreeDataPartWriterPMCS::finish(IMergeTreeDataPart::Checksums & checksums, bool /* sync */)
{
    /// If part is empty we still need to initialize block by empty columns.
    // if (!part_in_memory->block)
    //     for (const auto & column : columns_list)
    //         part_in_memory->block.insert(ColumnWithTypeAndName{column.type, column.name});

    // checksums.files["data.bin"] = part_in_memory->calculateBlockChecksum();

    // fix cnt calculate
    auto& proot = meta;
    proot->cnt = columns_buffer.size();

    Block flushed_block = header.cloneWithColumns(columns_buffer.releaseColumns());

    /// write index_granularity
    size_t index_granularity_for_block = storage.getSettings()->index_granularity;
    size_t rows_in_block = flushed_block.rows();
    for (size_t current_row = 0; current_row < rows_in_block; current_row += index_granularity_for_block)
        index_granularity.appendMark(index_granularity_for_block);

    /// write data
    writeDataBlock(flushed_block);

    /// write primary index
    if (settings.rewrite_primary_key)
    {
        Block primary_key_block = getBlockAndPermute(flushed_block, metadata_snapshot->getPrimaryKeyColumns(), nullptr);
        calculatePrimaryIndex(primary_key_block);
        finishPrimaryIndexSerialization();
    }
    part_in_pmcs->data_size = std::move(mapped_len);
    part_in_pmcs->cols_and_idxs = std::move(cols_and_idxs);
    /// no checksum
    checksums.addFile(DATA_FILE_NAME,mapped_len,std::make_pair(0,0));

}

void MergeTreeDataPartWriterPMCS::writeDataBlock(const Block & block)
{
    String path = part_in_pmcs->getFullPath()+DATA_FILE_NAME;
    size_t data_size = 0;
    size_t pre_size = 0;
    auto& proot = meta;

    /// caculate data_size and cols with offset, length
    for (const auto & col : block)
    {
        pre_size = data_size;
        data_size += col.column->byteSizePmem();
        cols_and_idxs.cols.emplace_back(col.name, col_off_and_len(pre_size, data_size - pre_size));
        cols_and_idxs.types.emplace_back(col.type);
        transaction::run(part_in_pmcs->storage.pop_meta, [&] {
            proot->cols_off_and_len.emplace_back(col.name, col_off_and_len(pre_size, data_size - pre_size));
		});
        
    }
    pmem_addr = static_cast<char *>(pmem_map_file(path.c_str(), data_size, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem));
    if (pmem_addr == NULL)
    {
        perror("pmem_map_file");
        exit(1);
    }
    pmem_offset = pmem_addr;

    for (const auto & col : block)
    {
        (*(col.type)).serializeToPmem(*(col.column),pmem_offset);
        pmem_offset += col.column->byteSizePmem();
    }
    pmem_drain();
    pmem_unmap(pmem_addr, mapped_len);

    
}

///TODO
void MergeTreeDataPartWriterPMCS::finishPrimaryIndexSerialization()
{
    String path = part_in_pmcs->getFullPath()+IDX_FILE_NAME;
    size_t idx_size = 0;
    size_t pre_size = 0;
    auto& proot = meta;

    /// caculate data_size and cols with offset, length
    size_t i = 0;
    for (const auto & col : index_columns)
    {
        pre_size = idx_size;
        idx_size += col->byteSize();
        cols_and_idxs.idxs.emplace_back(index_names[i], col_off_and_len(pre_size, idx_size - pre_size));
        transaction::run(part_in_pmcs->storage.pop_meta, [&] {
            proot->idxs_off_and_len.emplace_back(index_names[i++], col_off_and_len(pre_size, idx_size - pre_size));
		});
        

    }
    idx_addr = static_cast<char *>(pmem_map_file(path.c_str(), idx_size, PMEM_FILE_CREATE, 0666, &idx_len, &is_pmem));
    if (idx_addr == NULL)
    {
        perror("pmem_map_file");
        exit(1);
    }
    idx_offset = idx_addr;

    i = 0;
    for (const auto & col : index_columns)
    {
        (*(index_types[i++])).serializeToPmem(*col,idx_offset);
        idx_offset += col->byteSize();
    }
    pmem_drain();
    pmem_unmap(idx_addr, idx_len);
    //getColsOffLen(part_in_pmcs->cols_off_and_len);
    //getIdxsOffLen(part_in_pmcs->idxs_off_and_len);
}

void MergeTreeDataPartWriterPMCS::writeMetaToPmem()
{
    /// early calculate count
    auto& proot = meta;

    transaction::run(part_in_pmcs->storage.pop_meta, [&] {
    // columns.txt
    for (const auto & it : columns_list)
    {
        proot->columns.emplace_back(it.name, it.type->getName());
    }

    // parition
    proot->partition = part_in_pmcs->partition.toStringData(part_in_pmcs->storage);

    // minmax index 
    if (part_in_pmcs->minmax_idx.initialized)
    {
        auto minmax_ = part_in_pmcs->minmax_idx.toString(storage);
        for(size_t i = 0; i < minmax_.size(); i++)
        {
            proot->minmax.emplace_back(minmax_[i].first, minmax_[i].second);
        }
    }

    // index_cnt
    proot->index_cnt = index_granularity.getMarksCount();       
	});
    part_in_pmcs->storage.InsertMetaData(part_in_pmcs->name, meta);

}

}
