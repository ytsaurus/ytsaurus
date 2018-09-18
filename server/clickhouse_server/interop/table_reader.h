#pragma once

#include "column_builder.h"
#include "system_columns.h"
#include "table_schema.h"

#include <memory>
#include <vector>

namespace NInterop {

////////////////////////////////////////////////////////////////////////////////

struct TTableReaderOptions
{
    bool Unordered = false;
};

////////////////////////////////////////////////////////////////////////////////

class ITableReader
{
public:
    virtual ~ITableReader() = default;

    virtual TColumnList GetColumns() const = 0;

    virtual const TTableList& GetTables() const = 0;

    /// Reads bunch of values and appends it to the column buffers.
    virtual bool Read(const TColumnBuilderList& columns) = 0;
};

using ITableReaderPtr = std::shared_ptr<ITableReader>;
using TTableReaderList = std::vector<ITableReaderPtr>;

}   // namespace NInterop
