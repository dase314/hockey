#include <IO/WriteBufferFromNVM.h>

#include <sys/mman.h>

namespace DB
{
WriteBufferFromNVM::WriteBufferFromNVM(const String & path_, size_t file_len, size_t buf_size, char * existing_memory, size_t alignment)
    : WriteBufferFromFileBase(buf_size, existing_memory, alignment)
{
    //call_count++;
    path = path_;
    //int fd = ::open(path.c_str(),  O_WRONLY | O_TRUNC | O_CREAT | O_CLOEXEC , 0666);
    //pmem_addr = static_cast<char *>(mmap(0,file_len,PROT_WRITE,MAP_SHARED,fd,0));
    pmem_addr = static_cast<char *>(pmem_map_file(path_.c_str(), file_len, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem));
    //memset(pmem_addr,0,mapped_len);
    if (pmem_addr == NULL)
    {
        perror("pmem_map_file");
        exit(1);
    }
    pmem_write_offset = pmem_addr;
};
WriteBufferFromNVM::~WriteBufferFromNVM()
{
    //next();
    //pmem_drain();
    //pmem_unmap(pmem_addr, mapped_len);
}
void WriteBufferFromNVM::close()
{
    next();
    pmem_drain();

    pmem_unmap(pmem_addr, mapped_len);
    pmem_addr = static_cast<char *>(pmem_map_file(path.c_str(), pmem_offset, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem));
    if (pmem_addr == NULL)
    {
        perror("pmem_map_file");
        exit(1);
    }
    pmem_unmap(pmem_addr, mapped_len);
}
void WriteBufferFromNVM::sync()
{
    /// If buffer has pending data - write it.
    next();
}
std::string WriteBufferFromNVM::getFileName() const
{
    return path;
}
void WriteBufferFromNVM::nextImpl()
{
    if (!offset())
        return;
    size_t bytes_written = 0;
    while (bytes_written != offset())
    {
        size_t write_bytes=std::min(offset() - bytes_written, mapped_len - pmem_offset);
        //LOG_TRACE(log, "WriteBufferFromNVM mapped_len: {},buffer_offset: {} write_bytes: {} pmem_offset: {} pmem_addr: {}, pmem_write_offset: {}.",mapped_len,offset(),write_bytes,pmem_offset,uint64_t(pmem_addr),uint64_t(pmem_write_offset));
        //LOG_TRACE(log, "pmem_addr: {}, pmem_write_offset: {}",uint64_t(pmem_addr),uint64_t(pmem_write_offset));
        pmem_memcpy_nodrain(pmem_write_offset, working_buffer.begin() + bytes_written, write_bytes);
        pmem_offset += write_bytes;
        pmem_write_offset += write_bytes;
        bytes_written += write_bytes;
        if (pmem_offset == mapped_len)
        {
            //pmem_drain();
            pmem_unmap(pmem_addr, mapped_len);
            //LOG_TRACE(log, "pmem_addr: {}",uint64_t(pmem_addr));
            pmem_addr = static_cast<char *>(pmem_map_file(path.c_str(), mapped_len * 2, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem));
            //LOG_TRACE(log, "WriteBufferFromNVM remap {}.",mapped_len );
            pmem_write_offset=pmem_addr+pmem_offset;
            //LOG_TRACE(log, "pmem_addr: {}",uint64_t(pmem_addr));
            if (pmem_addr == NULL)
            {
                perror("pmem_map_file");
                exit(1);
            }
        }
    }
    
}

}
