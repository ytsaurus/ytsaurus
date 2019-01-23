#pragma once

#include "public.h"
#include "table.h"

#include <Columns/IColumn.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct IColumnBuilder
{
    EClickHouseColumnType Type = EClickHouseColumnType::Invalid;

    virtual ~IColumnBuilder() = default;

    /// Gets the column data type.
    EClickHouseColumnType GetType() const
    {
        return Type;
    }

    /// You should use one of the derived IXxxTColumnBuilder interfaces.
};

using TColumnBuilderList = std::vector<IColumnBuilderPtr>;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct ITypedColumnBuilder
    : public IColumnBuilder
{
public:
    /// Appends bunch of values to the column buffer.
    virtual void Add(const T* values, size_t count) = 0;
};

using IStringColumnBuilder = ITypedColumnBuilder<TStringBuf>;

////////////////////////////////////////////////////////////////////////////////

IColumnBuilderPtr CreateColumnBuilder(
    EClickHouseColumnType type,
    DB::MutableColumnPtr column);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
