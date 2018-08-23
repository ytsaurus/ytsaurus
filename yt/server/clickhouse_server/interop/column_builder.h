#pragma once

#include "table_schema.h"

#include <util/generic/strbuf.h>

#include <memory>
#include <utility>
#include <vector>

namespace NInterop {

////////////////////////////////////////////////////////////////////////////////

class IColumnBuilder
{
protected:
    EColumnType Type = EColumnType::Invalid;

public:
    virtual ~IColumnBuilder() = default;

    /// Gets the column data type.
    EColumnType GetType() const
    {
        return Type;
    }

    /// You should use one of the derived IXxxTColumnBuilder interfaces.
};

using IColumnBuilderPtr = std::shared_ptr<IColumnBuilder>;
using TColumnBuilderList = std::vector<IColumnBuilderPtr>;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class ITypedColumnBuilder
    : public IColumnBuilder
{
public:
    /// Appends bunch of values to the column buffer.
    virtual void Add(const T* values, size_t count) = 0;
};

using IStringColumnBuilder = ITypedColumnBuilder<TStringBuf>;

}   // namespace NInterop
