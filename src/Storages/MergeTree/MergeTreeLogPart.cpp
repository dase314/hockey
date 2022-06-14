#include <Storages/MergeTree/MergeTreeLogPart.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/MergeTreeData.h>

using namespace pmem::obj;
namespace DB
{

void MergeTreeLogPart::appendBlock(const Block & from)
{
    if (!data)
    {   
        int pm_arr_cnt = from.columns();
        
        strs.reserve(pm_arr_cnt);

        transaction::run(storage.pop, [&] {
            for(int i = 0; i < pm_arr_cnt; i++) {
                strs.emplace_back(pmem::obj::make_persistent<pmem::obj::string>());
            }
        });
        data = from.cloneEmptyFromPmem(strs);
    }


    from.checkNumberOfRows();

    size_t rows = from.rows();
    // size_t bytes = from.bytes();

    // size_t old_rows = data.rows();
    
    MutableColumnPtr last_col;
    try
    {   
        std::unique_lock<std::mutex> mu(log_mutex);
        for (size_t column_no = 0, columns = data.columns(); column_no < columns; ++column_no)
        {
            const IColumn & col_from = *from.getByPosition(column_no).column.get();
            last_col = IColumn::mutate(std::move(data.getByPosition(column_no).column));

            // insert data into pmem
            last_col->insertPmemRangeFrom(col_from, 0, rows);

            data.getByPosition(column_no).column = std::move(last_col);
        }
    }
    catch (...)
    {
        throw Exception("Cannot append to block", ErrorCodes::LOGICAL_ERROR);
    }
    
    rows_count.fetch_add(rows, std::memory_order_relaxed);

}
void MergeTreeLogPart::shutdown() {
    std::unique_lock<std::mutex> mu(log_mutex);
    pmem::obj::transaction::run(storage.pop, [&] {
        for(auto it : strs)
        {
            pmem::obj::delete_persistent<pmem::obj::string>(it);
        }
    });
    LOG_INFO(&Poco::Logger::get("MergeTreeLogPart"), "Call MergeTreeLogPart shutdown");
    close = true;
}

}