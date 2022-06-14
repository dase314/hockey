#include "MergeTreeDataPartPMCS.h"
#include <Storages/MergeTree/MergeTreeReaderPMCS.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterPMCS.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
}


MergeTreeDataPartPMCS::MergeTreeDataPartPMCS(
       MergeTreeData & storage_,
        const String & name_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, volume_, relative_path_, Type::PMCS)
{
    default_codec = CompressionCodecFactory::instance().get("NONE", {});
}

MergeTreeDataPartPMCS::MergeTreeDataPartPMCS(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, info_, volume_, relative_path_, Type::PMCS)
{
    default_codec = CompressionCodecFactory::instance().get("NONE", {});
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartPMCS::getReader(
    const NamesAndTypesList & columns_to_read,
    const StorageMetadataPtr & metadata_snapshot,
    const MarkRanges & mark_ranges,
    UncompressedCache * /* uncompressed_cache */,
    MarkCache * /* mark_cache */,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & /* avg_value_size_hints */,
    const ReadBufferFromFileBase::ProfileCallback & /* profile_callback */) const
{
    auto ptr = std::static_pointer_cast<const MergeTreeDataPartPMCS>(shared_from_this());
    return std::make_unique<MergeTreeReaderPMCS>(
        ptr, columns_to_read, metadata_snapshot, mark_ranges, reader_settings);
}

IMergeTreeDataPart::MergeTreeWriterPtr MergeTreeDataPartPMCS::getWriter(
    const NamesAndTypesList & columns_list,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MergeTreeIndexPtr> & /* indices_to_recalc */,
    const CompressionCodecPtr & /* default_codec */,
    const MergeTreeWriterSettings & writer_settings,
    const MergeTreeIndexGranularity & /* computed_index_granularity */) const
{
    auto ptr = std::static_pointer_cast<const MergeTreeDataPartPMCS>(shared_from_this());
    return std::make_unique<MergeTreeDataPartWriterPMCS>(
        ptr, columns_list, metadata_snapshot, writer_settings);
}

void MergeTreeDataPartPMCS::flushToDisk(const String & base_path, const String & new_relative_path, const StorageMetadataPtr & metadata_snapshot) const
{
    const auto & disk = volume->getDisk();
    String destination_path = base_path + new_relative_path;

    auto new_type = storage.choosePartTypeOnDisk(block.bytes(), rows_count);
    auto new_data_part = storage.createPart(name, new_type, info, volume, new_relative_path);

    new_data_part->uuid = uuid;
    new_data_part->setColumns(columns);
    new_data_part->partition.value.assign(partition.value);
    new_data_part->minmax_idx = minmax_idx;

    if (disk->exists(destination_path))
    {
        throw Exception("Could not flush part " + quoteString(getFullPath())
            + ". Part in " + fullPath(disk, destination_path) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);
    }

    disk->createDirectories(destination_path);

    auto compression_codec = storage.global_context.chooseCompressionCodec(0, 0);
    auto indices = MergeTreeIndexFactory::instance().getMany(metadata_snapshot->getSecondaryIndices());
    MergedBlockOutputStream out(new_data_part, metadata_snapshot, columns, indices, compression_codec);
    out.writePrefix();
    out.write(block);
    out.writeSuffixAndFinalizePart(new_data_part);
}

void MergeTreeDataPartPMCS::makeCloneInDetached(const String & prefix, const StorageMetadataPtr & metadata_snapshot) const
{
    String detached_path = getRelativePathForDetachedPart(prefix);
    flushToDisk(storage.getRelativeDataPath(), detached_path, metadata_snapshot);
}


void MergeTreeDataPartPMCS::calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const
{
    total_size.data_uncompressed = data_size;
    for (const auto col : cols_and_idxs.cols)
        each_columns_size[col.first].data_uncompressed = col.second.len;
}

IMergeTreeDataPart::Checksum MergeTreeDataPartPMCS::calculateBlockChecksum() const
{
    //checksum is missing in test phase
    IMergeTreeDataPart::Checksum checksum(data_size, std::make_pair(0,0));
    
    return checksum;
}
void MergeTreeDataPartPMCS::getBlockFromPmem() const
{
    String path = getFullRelativePath()+DATA_FILE_NAME;
    data_size = volume->getDisk()->getFileSize(path);
    path = getFullPath() + DATA_FILE_NAME;
    pmem_addr = static_cast<char *>(pmem_map_file(path.c_str(), data_size, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem));
    if (pmem_addr == NULL)
    {
        perror("pmem_map_file");
        exit(1);
    }
    pmem_offset = pmem_addr;
    int i=0;
    for (auto it:cols_and_idxs.cols)
    {
        auto column = cols_and_idxs.types[i]->createColumn();
        pmem_offset = pmem_addr + it.second.offset;
        cols_and_idxs.types[i]->deserializeFromPmem(*column, pmem_offset, it.second.len);
        ColumnWithTypeAndName col(std::move(column), cols_and_idxs.types[i++], it.first);
        block.insert(std::move(col));
    }
}

void MergeTreeDataPartPMCS::loadColumnsChecksumsIndexes(bool , bool)
{
    meta = storage.GetMetaFromDataPart(name);
    transaction::run(storage.pop_meta, [&] {
    loadColumns();
    loadIndexGranularity();
    getBlockFromPmem();
    checksums = checkDataPart(shared_from_this(), false);
    calculateColumnsSizesOnDisk();
    loadIndex();     
    loadRowsCount(); 
    loadPartitionAndMinMaxIndex();
    });
    
}
void MergeTreeDataPartPMCS::loadColumns()
{
    // auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    // columns.readText(*volume->getDisk()->readFile(path));
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
    auto& proot = meta;
    // LOG_TRACE(log, "{}", proot->columns[0].first.c_str());
    //for(auto it:proot->columns)
    for(auto it = proot->columns.begin(); it != proot->columns.end(); it++)
    {
        String first(it->first.c_str());
        String second(it->second.c_str());

        columns.emplace_back(first,data_type_factory.get(second));
    }
    size_t pos = 0;
    for (const auto & column : columns)
    {
        column_name_to_position.emplace(column.name, pos++);
        cols_and_idxs.types.emplace_back(column.type);
    }
    for(auto it = proot->cols_off_and_len.begin(); it != proot->cols_off_and_len.end(); it++)
    {
        cols_and_idxs.cols.emplace_back(it->first,it->second);
    }
    for(auto it = proot->idxs_off_and_len.begin(); it != proot->idxs_off_and_len.end(); it++)
    {
        cols_and_idxs.idxs.emplace_back(it->first,it->second);
    }


}

void MergeTreeDataPartPMCS::loadIndexGranularity()
{
    auto& proot = meta;
    // String full_path = getFullRelativePath();
    index_granularity_info.setNonAdaptivePmem();;

    index_granularity.resizeWithFixedGranularity(proot->index_cnt, index_granularity_info.fixed_index_granularity); /// all the same
    
    index_granularity.setInitialized();
}

void MergeTreeDataPartPMCS::loadIndex()
{
    if (!index_granularity.isInitialized())
        throw Exception("Index granularity is not loaded before index loading", ErrorCodes::LOGICAL_ERROR);

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    size_t key_size = primary_key.column_names.size();

    if (key_size)
    {
        MutableColumns loaded_index;
        loaded_index.resize(key_size);

        for (size_t i = 0; i < key_size; ++i)
        {
            loaded_index[i] = primary_key.data_types[i]->createColumn();
            loaded_index[i]->reserve(index_granularity.getMarksCount());
        }

        // String index_path = getFullRelativePath() + "primary.idx";
        // auto index_file = openForReading(volume->getDisk(), index_path);
        String path = getFullRelativePath()+IDX_FILE_NAME;
        size_t idx_size = volume->getDisk()->getFileSize(path);
        path = getFullPath() + IDX_FILE_NAME;
        pmem_idx_addr = static_cast<char *>(pmem_map_file(path.c_str(), idx_size, PMEM_FILE_CREATE, 0666, &idx_mapped_len, &is_pmem));
        if (pmem_idx_addr == NULL)
        {
            perror("pmem_map_file");
            exit(1);
        }
        pmem_idx_offset = pmem_idx_addr;

        // size_t marks_count = index_granularity.getMarksCount();
        assert(cols_and_idxs.idxs.size()==key_size);
        size_t i=0;
        for (auto it:cols_and_idxs.idxs)
        {
            pmem_idx_offset = pmem_idx_addr + it.second.offset;
            primary_key.data_types[i]->deserializeFromPmemCpy(*loaded_index[i], pmem_idx_offset, it.second.len);
            i++;
        }
        for (i = 0; i < key_size; ++i)
        {
            loaded_index[i]->protect();
        }

        index.assign(std::make_move_iterator(loaded_index.begin()), std::make_move_iterator(loaded_index.end()));
    }
}

void MergeTreeDataPartPMCS::loadRowsCount()
{
    auto& proot = meta;
    rows_count = proot->cnt;
}

void MergeTreeDataPartPMCS::loadPartitionAndMinMaxIndex()
{
    String path = getFullRelativePath();
    auto& proot = meta;
    String partition_str(proot->partition.c_str(),proot->partition.size());
    partition_str.resize(proot->partition.size());
    partition.loadPmem(storage,partition_str);
    std::vector<std::pair<std::string,std::string>> raw_data;
    for (auto it = proot->minmax.begin(); it != proot->minmax.end(); it++)
    {
        String minmax_str(it->second.c_str(),it->second.size());
        raw_data.emplace_back(it->first.c_str(),minmax_str);
    }
    if (!isEmpty())
        minmax_idx.loadPmem(storage, raw_data);
    

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    String calculated_partition_id = partition.getID(metadata_snapshot->getPartitionKey().sample_block);
    if (calculated_partition_id != info.partition_id)
        throw Exception(
            "While loading part " + getFullPath() + ": calculated partition ID: " + calculated_partition_id
            + " differs from partition ID in part name: " + info.partition_id,
            ErrorCodes::CORRUPTED_DATA);
}


DataPartPMCSPtr asPMCSPart(const MergeTreeDataPartPtr & part)
{
    return std::dynamic_pointer_cast<const MergeTreeDataPartPMCS>(part);
}
}
