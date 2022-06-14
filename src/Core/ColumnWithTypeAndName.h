#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

namespace DB
{

class WriteBuffer;


/** Column data along with its data type and name.
  * Column data could be nullptr - to represent just 'header' of column.
  * Name could be either name from a table or some temporary generated name during expression evaluation.
  */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnull-dereference"
struct ColumnWithTypeAndName
{
    ColumnPtr column;
    DataTypePtr type;
    String name;

    ColumnWithTypeAndName() {}
    ColumnWithTypeAndName(const ColumnPtr & column_, const DataTypePtr & type_, const String & name_)
        : column(column_), type(type_), name(name_) {}

    /// Uses type->createColumn() to create column
    ColumnWithTypeAndName(const DataTypePtr & type_, const String & name_)
        : column(type_->createColumn()), type(type_), name(name_) {}

    ColumnWithTypeAndName cloneEmpty() const;
    ColumnWithTypeAndName cloneEmptyFromPmem(pmem::obj::persistent_ptr<pmem::obj::string> & str) const;
    // ColumnWithTypeAndName cloneEmptyFromPmem(pmem::obj::persistent_ptr<pmem::obj::string> & str, pmem::obj::persistent_ptr<pmem::obj::string> & str) const;
    bool operator==(const ColumnWithTypeAndName & other) const;

    void dumpNameAndType(WriteBuffer & out) const;
    void dumpStructure(WriteBuffer & out) const;
    String dumpStructure() const;
};
#pragma GCC diagnostic pop

}
