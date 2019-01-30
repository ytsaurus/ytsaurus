#pragma once

#include "public.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/core/yson/public.h>

#include <util/generic/string.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

enum class EClickHouseColumnType
{
    /// Invalid type.
    Invalid = 0,

    /// Signed integer value.
    Int8,
    Int16,
    Int32,
    Int64,

    /// Unsigned integer value.
    UInt8,
    UInt16,
    UInt32,
    UInt64,

    /// Floating point value.
    Float,
    Double,

    /// Boolean value.
    Boolean,

    /// DateTime value.
    Date,
    DateTime,

    /// String value.
    String,
};

////////////////////////////////////////////////////////////////////////////////

enum class EColumnFlags
{
    None = 0,
    Sorted = 0x01,
    Nullable = 0x02,
};

////////////////////////////////////////////////////////////////////////////////

struct TColumn
{
    TString Name;
    EClickHouseColumnType Type = EClickHouseColumnType::Invalid;
    int Flags = 0;

    TColumn() = default;

    TColumn(TString name, EClickHouseColumnType type, int flags = 0)
        : Name(std::move(name))
        , Type(type)
        , Flags(flags)
    {}

    bool IsSorted() const;
    bool IsNullable() const;

    void SetSorted();
    void SetNullable();
};

bool operator == (const TColumn& lhs, const TColumn& rhs);
bool operator != (const TColumn& lhs, const TColumn& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TTable
{
    TString Name;
    std::vector<TColumn> Columns;

    TTable() = default;

    TTable(TString name, std::vector<TColumn> columns = {})
        : Name(std::move(name))
        , Columns(std::move(columns))
    {}
};

////////////////////////////////////////////////////////////////////////////////

TTablePtr CreateTable(
    const TString& name,
    const NTableClient::TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
