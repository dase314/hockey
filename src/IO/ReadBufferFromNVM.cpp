#include <IO/ReadBufferFromNVM.h>
#include <sys/stat.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_SELECT;
}

int ReadBufferFromNVM::getFileSize(const char * fname)
{
    struct stat statbuf;
    if (stat(fname, &statbuf) == 0)
        return statbuf.st_size;
    return 0;
}
std::string ReadBufferFromNVM::getFileName() const
{
    return path;
}

ReadBufferFromNVM::ReadBufferFromNVM(const String & path_, size_t buf_size, char * existing_memory, size_t alignment)
    : ReadBufferFromFileBase(buf_size, existing_memory, alignment)
{
    file_offset_of_buffer_end = 0;
    buffer_read = 0;
    path = path_;
    file_len = getFileSize(path.c_str());
    //LOG_TRACE(log, "ReadBufferFromNVM path {}.", path);
    pmem_addr = static_cast<char *>(pmem_map_file(path_.c_str(), file_len, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem));
    //char * test_addr =new char[mapped_len];
    //::memcpy(test_addr,pmem_addr,mapped_len);
    //delete[] test_addr;
    if (pmem_addr == NULL)
    {
        perror("pmem_map_file");
        exit(1);
    }
    pmem_read_offset=pmem_addr;
};
ReadBufferFromNVM::~ReadBufferFromNVM()
{
    //next();
    //pmem_drain();
    pmem_unmap(pmem_addr, mapped_len);
}
void ReadBufferFromNVM::close()
{
    //next();
    //pmem_unmap(pmem_addr, mapped_len);
}
bool ReadBufferFromNVM::nextImpl()
{
    //LOG_TRACE(log,"call nextimpl pmem_addr: {} , pmem_read_offset: {}, mmap_len: {},internal_buffer.size(): {} ",pmem_addr,pmem_read_offset,mapped_len,internal_buffer.size());
    size_t bytes_read = 0;
    while (!bytes_read)
    {
        ssize_t res = 0;
        {
            if (buffer_read + internal_buffer.size() <= file_len)
            {
                ::memcpy(internal_buffer.begin(), pmem_addr+buffer_read, internal_buffer.size());
                res += internal_buffer.size();
            }
            else if (buffer_read == file_len)
            {
                res = 0;
            }
            else
            {
                internal_buffer.resize(file_len - buffer_read);
                ::memcpy(internal_buffer.begin(), pmem_addr+buffer_read, internal_buffer.size());
                res += internal_buffer.size();
            }
        }

        if (!res)
            break;

        if (res > 0)
            bytes_read += res;
        buffer_read += bytes_read;
    }

    file_offset_of_buffer_end += bytes_read;

    if (bytes_read)
    {
        working_buffer.resize(bytes_read);
    }
    else
        return false;

    return true;
}

off_t ReadBufferFromNVM::seek(off_t offset, int whence)
{
    size_t new_pos;
    if (whence == SEEK_SET)
    {
        assert(offset >= 0);
        new_pos = offset;
    }
    else if (whence == SEEK_CUR)
    {
        new_pos = file_offset_of_buffer_end - (working_buffer.end() - pos) + offset;
    }
    else
    {
        throw Exception("ReadBufferFromFileDescriptor::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    /// Position is unchanged.
    if (new_pos + (working_buffer.end() - pos) == file_offset_of_buffer_end)
        return new_pos;

    // file_offset_of_buffer_end corresponds to working_buffer.end(); it's a past-the-end pos,
    // so the second inequality is strict.
    if (file_offset_of_buffer_end - working_buffer.size() <= static_cast<size_t>(new_pos) && new_pos < file_offset_of_buffer_end)
    {
        /// Position is still inside buffer.
        pos = working_buffer.end() - file_offset_of_buffer_end + new_pos;
        assert(pos >= working_buffer.begin());
        assert(pos < working_buffer.end());

        return new_pos;
    }
    else
    {
        pos = working_buffer.end();
        //off_t res = ::lseek(fd, new_pos, SEEK_SET);
        pmem_read_offset = pmem_addr + new_pos;
        buffer_read = new_pos;
        off_t res = new_pos;
        file_offset_of_buffer_end = new_pos;
        return res;
    }
}

}
