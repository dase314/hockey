#include <string>
#include <libpmem.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>


namespace DB
{
class WriteBufferFromNVM : public WriteBufferFromFileBase
{
public:
    WriteBufferFromNVM(
        const String & path_,
        size_t file_len,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);
    ~WriteBufferFromNVM() override;

    //void sync() override;
    void close();
    void finalize() override { close(); }
    void sync() override;
    std::string getFileName() const override;

protected:
    String path;
    char * pmem_addr;
    char * pmem_write_offset;
    size_t mapped_len;
    int is_pmem;
    size_t pmem_offset = 0;
    Poco::Logger * log = &Poco::Logger::get("writebuffer");

    void nextImpl() override;
};

}