#include <Core/ColumnsWithTypeAndName.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

ColumnWithTypeAndName ColumnWithTypeAndName::cloneEmpty() const
{
    ColumnWithTypeAndName res;

    res.name = name;
    res.type = type;
    if (column)
        res.column = column->cloneEmpty();

    return res;
}

ColumnWithTypeAndName ColumnWithTypeAndName::cloneEmptyFromPmem(pmem::obj::persistent_ptr<pmem::obj::string> & str) const
{
    ColumnWithTypeAndName res;

    res.name = name;
    res.type = type;
    if (column)
        res.column = column->cloneEmptyFromPmem(str);

    return res;
}

// ColumnWithTypeAndName ColumnWithTypeAndName::cloneEmptyFromPmem(pmem::obj::persistent_ptr<pmem::obj::string> & str_off, pmem::obj::persistent_ptr<pmem::obj::string> & str_data) const
// {
//     ColumnWithTypeAndName res;

//     res.name = name;
//     res.type = type;
//     if (column)
//         res.column = column->cloneEmptyFromPmem(str_off,str_data);

//     return res;
// }


bool ColumnWithTypeAndName::operator==(const ColumnWithTypeAndName & other) const
{
    return name == other.name
        && ((!type && !other.type) || (type && other.type && type->equals(*other.type)))
        && ((!column && !other.column) || (column && other.column && column->getName() == other.column->getName()));
}


void ColumnWithTypeAndName::dumpNameAndType(WriteBuffer & out) const
{
    out << name;

    if (type)
        out << ' ' << type->getName();
    else
        out << " nullptr";
}

void ColumnWithTypeAndName::dumpStructure(WriteBuffer & out) const
{
    dumpNameAndType(out);

    if (column)
        out << ' ' << column->dumpStructure();
    else
        out << " nullptr";
}

String ColumnWithTypeAndName::dumpStructure() const
{
    WriteBufferFromOwnString out;
    dumpStructure(out);
    return out.str();
}

}
