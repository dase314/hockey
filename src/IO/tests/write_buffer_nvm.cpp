#include <string>

#include <iostream>

#include <IO/WriteBufferFromNVM.h>
#include <IO/WriteHelpers.h>

int main(int, char **)
{
    const char* path="/home/nauta/pmem1/nauta/test/pmem_file";
    DB::WriteBufferFromNVM out(path,4,4);
    DB::Int64 a = -123456;
        DB::Float64 b = 123.456;
        DB::String c = "вася пе\tтя";
        DB::String d = "'xyz\\";
        for(size_t i=0;i<1024*1024*1024;i++)
        {
            DB::writeIntText(a, out);
        }
        {

            DB::writeIntText(a, out);
            DB::writeChar(' ', out);

            DB::writeFloatText(b, out);
            DB::writeChar(' ', out);

            DB::writeEscapedString(c, out);
            DB::writeChar('\t', out);

            DB::writeQuotedString(d, out);
            DB::writeChar('\n', out);
        }
        out.finalize();
    
    return 0;
}
