#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/container/vector.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmem.h>

#define LAYOUT "META"

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
using namespace pmem::obj;
class UncompressedCache;
// class MergeTreeDataPartWriterPMCS;
// struct col_off_and_len
// {
//     col_off_and_len(size_t offset_, size_t len_)
//         :offset(offset_),len(len_)
//     {
//     }
//     size_t offset;
//     size_t len;
// };
// struct off_and_lens
// {
//     std::vector<std::pair<std::string,col_off_and_len>> cols;
//     std::vector<std::pair<std::string,col_off_and_len>> idxs;
//     std::vector<DataTypePtr> types;
// };
// struct root
// {
//     pmem::obj::vector<std::pair<pmem::obj::string,col_off_and_len>> cols_off_and_len;
//     pmem::obj::vector<std::pair<pmem::obj::string,col_off_and_len>> idxs_off_and_len;
//     pmem::obj::vector<std::pair<pmem::obj::string,pmem::obj::string>> columns;
//     pmem::obj::string partition;
//     pmem::obj::vector<std::pair<pmem::obj::string,pmem::obj::string>> minmax;
//     p<int> cnt;
//     p<int> index_cnt;
// };
constexpr static auto META_FILE_NAME = "meta";
constexpr static auto DATA_FILE_NAME = "data.bin";
constexpr static auto IDX_FILE_NAME = "idx.bin";
constexpr static size_t POOL_SIZE = 1024 * 1024 * 10;
/// PMCS: persistent memory columnar storage
class MergeTreeDataPartPMCS : public IMergeTreeDataPart
{
public:
    MergeTreeDataPartPMCS(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_ = {});

    MergeTreeDataPartPMCS(
        MergeTreeData & storage_,
        const String & name_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_ = {});

    ~MergeTreeDataPartPMCS()
    {
        if(mapped_len)
            pmem_unmap(pmem_addr, mapped_len);
        if(idx_mapped_len)
            pmem_unmap(pmem_idx_addr, idx_mapped_len);
    }

    MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns,
        const StorageMetadataPtr & metadata_snapshot,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const MergeTreeReaderSettings & reader_settings_,
        const ValueSizeMap & avg_value_size_hints,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback) const override;
    MergeTreeWriterPtr getWriter(
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity) const override;

    bool isStoredOnDisk() const override { return false; }
    bool hasColumnFiles(const String & column_name, const IDataType & /* type */) const override { return !!getColumnPosition(column_name); }
    String getFileNameForColumn(const NameAndTypePair & /* column */) const override { return ""; }
    
    void makeCloneInDetached(const String & prefix, const StorageMetadataPtr & metadata_snapshot) const override;

    void flushToDisk(const String & base_path, const String & new_relative_path, const StorageMetadataPtr & metadata_snapshot) const;

    /// Returns hash of parts's block
    Checksum calculateBlockChecksum() const;

    void getBlockFromPmem() const;

    void loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency) override;

    mutable Block block;
    
    
    //using off_and_lens = MergeTreeDataPartWriterPMCS::off_and_lens;
    mutable off_and_lens cols_and_idxs;

    // std::vector<std::pair<String,col_off_and_len>> cols_off_and_len;
    // std::vector<std::pair<String,col_off_and_len>> idxs_off_and_len;
    mutable size_t data_size;

private:
    mutable std::condition_variable is_merged;
    /// Calculates uncompressed sizes in memory.
    void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const override;
    void loadColumns();
    void loadIndexGranularity() override;
    void loadIndex() override; 
    void loadRowsCount() override;
    void loadPartitionAndMinMaxIndex() override;

    // constexpr static auto DATA_FILE_NAME = "data.bin";
    mutable int is_pmem;
    mutable char * pmem_addr;
    mutable char * pmem_offset;
    mutable size_t mapped_len = 0;
    mutable char * pmem_idx_addr;
    mutable char * pmem_idx_offset;
    mutable size_t idx_mapped_len = 0;
    persistent_ptr<Meta> meta;
    Poco::Logger * log = &Poco::Logger::get("xxxx");
};

using DataPartPMCSPtr = std::shared_ptr<const MergeTreeDataPartPMCS>;

DataPartPMCSPtr asPMCSPart(const MergeTreeDataPartPtr & part);

}