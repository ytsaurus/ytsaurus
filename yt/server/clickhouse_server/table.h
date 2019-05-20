#pragma once

#include "private.h"

#include <yt/client/table_client/schema.h>

#include <yt/client/ypath/rich.h>

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

struct TClickHouseColumn
{
    TString Name;
    EClickHouseColumnType Type = EClickHouseColumnType::Invalid;
    int Flags = 0;

    TClickHouseColumn() = default;

    TClickHouseColumn(TString name, EClickHouseColumnType type, int flags = 0)
        : Name(std::move(name))
        , Type(type)
        , Flags(flags)
    {}

    bool IsSorted() const;
    bool IsNullable() const;

    void SetSorted();
    void DropSorted();
    void SetNullable();

    static std::optional<TClickHouseColumn> FromColumnSchema(const NTableClient::TColumnSchema& columnSchema);
};

bool operator == (const TClickHouseColumn& lhs, const TClickHouseColumn& rhs);
bool operator != (const TClickHouseColumn& lhs, const TClickHouseColumn& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TClickHouseTable
{
    NYPath::TRichYPath Path;
    std::vector<TClickHouseColumn> Columns;
    NTableClient::TTableSchema TableSchema;

    TClickHouseTable() = default;

    TClickHouseTable(const NYPath::TRichYPath& path, const NTableClient::TTableSchema& tableSchema);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
