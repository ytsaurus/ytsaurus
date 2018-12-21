#pragma once

#include "public.h"

#include "table_schema.h"

namespace NYT::NClickHouseServer::NNative {

////////////////////////////////////////////////////////////////////////////////

struct IColumnBuilder
{
    EColumnType Type = EColumnType::Invalid;

    virtual ~IColumnBuilder() = default;

    /// Gets the column data type.
    EColumnType GetType() const
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

} // namespace NYT::NClickHouseServer::NNative
