#pragma once
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/MergeTreeDataPartPMCS.h>

using namespace pmem::obj;

//#define POOL_SIZE ((size_t)(1024 * 1024 * 10))

namespace DB
{
/// Writes data part in pmcs.
class MergeTreeDataPartWriterPMCS : public IMergeTreeDataPartWriter
{
public:
    MergeTreeDataPartWriterPMCS(
        const DataPartPMCSPtr & part_,
        const NamesAndTypesList & columns_list_,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeWriterSettings & settings_);
    
    ~MergeTreeDataPartWriterPMCS() = default;

    /// write() will be call mutil times when execute merge. 
    void write(const Block & block, const IColumn::Permutation * permutation) override;

    void finish(IMergeTreeDataPart::Checksums & checksums, bool sync) override;

    void writeMetaToPmem() override;

    // void getColsOffLen(std::vector<std::pair<String,col_off_and_len>> & res)
    // {
    //     //std::vector<std::pair<String,col_off_and_len>> res;
    //     auto proot = pop.root();
    //     for (auto it : proot->cols_off_and_len)
    //     res.emplace_back(std::make_pair(it.first.c_str(),col_off_and_len(it.second.offset,it.second.len)));
    //     //return res;
    // }

    // void getIdxsOffLen(std::vector<std::pair<String,col_off_and_len>> & res)
    // {
    //     //std::vector<std::pair<String,col_off_and_len>> res;
    //     auto proot = pop.root();
    //     for (auto it : proot->idxs_off_and_len)
    //     res.emplace_back(std::make_pair(it.first.c_str(),col_off_and_len(it.second.offset,it.second.len)));
    //     //return res;
    // }

private:
    void calculatePrimaryIndex(const Block & primary_index_block);

    void writeDataBlock(const Block & block);

    void finishPrimaryIndexSerialization();

    Block header;

    /// use for mutil call write()
    class ColumnsBuffer
    {
    public:
        void add(MutableColumns && columns);
        size_t size() const;
        Columns releaseColumns();


    private:
        MutableColumns accumulated_columns;
    };

    ColumnsBuffer columns_buffer;

    // struct root
    // {
	//     pmem::obj::vector<std::pair<pmem::obj::string,col_off_and_len>> cols_off_and_len;
    //     pmem::obj::vector<std::pair<pmem::obj::string,col_off_and_len>> idxs_off_and_len;
    //     pmem::obj::vector<std::pair<pmem::obj::string,pmem::obj::string>> columns;
    //     pmem::obj::string partition;
    //     pmem::obj::vector<std::pair<pmem::obj::string,pmem::obj::string>> minmax;
    //     p<int> cnt;
    // };
    persistent_ptr<Meta> meta;


    int is_pmem;
    char * pmem_addr;
    char * pmem_offset;
    size_t idx_len;
    char * idx_addr;
    char * idx_offset;

    DataPartPMCSPtr part_in_pmcs;
    // NamesAndTypesList names_and_types;
    DataTypes index_types;
    Names index_names;
public:
    size_t mapped_len;
    off_and_lens cols_and_idxs;


};

}
