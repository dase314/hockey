#include <string>
#include <libpmem.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/SeekableReadBuffer.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{
class ReadBufferFromNVM : public ReadBufferFromFileBase
{
public:
    ReadBufferFromNVM(
        const String & path_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, char * existing_memory = nullptr, size_t alignment = 0);
    ~ReadBufferFromNVM() override;

    //void sync() override;
    void close();
    off_t getPosition() override { return reinterpret_cast<off_t>(pmem_read_offset - pmem_addr); }

    /// If 'offset' is small enough to stay in buffer after seek, then true seek in file does not happen.
    off_t seek(off_t off, int whence) override;
    std::string getFileName() const override;

protected:
    String path;
    char * pmem_addr;
    char * pmem_read_offset;
    size_t mapped_len;
    int is_pmem;
    size_t file_len;
    size_t buffer_read;
    size_t file_offset_of_buffer_end;
    Poco::Logger * log = &Poco::Logger::get("readbuffer");

    bool nextImpl() override;
    int getFileSize(const char * fname);
};

}