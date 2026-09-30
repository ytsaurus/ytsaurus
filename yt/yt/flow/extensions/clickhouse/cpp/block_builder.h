#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/client/table_client/public.h>

#include <contrib/libs/clickhouse-cpp/clickhouse/block.h>

#include <library/cpp/yt/misc/enum.h>

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EClickHouseBaseType,
    (Int8)
    (Int16)
    (Int32)
    (Int64)
    (UInt8)
    (UInt16)
    (UInt32)
    (UInt64)
    (Float32)
    (Float64)
    (String)
    (FixedString)
    (Bool)
    (Date)
    (DateTime)
);

struct TClickHouseColumnType
{
    EClickHouseBaseType Base = {};
    bool Nullable = false;
    bool LowCardinality = false;
    std::optional<int> FixedStringLength;
};

TClickHouseColumnType ParseClickHouseType(std::string_view type);

////////////////////////////////////////////////////////////////////////////////

struct TClickHouseTableColumn
{
    std::string Name;
    std::string Type;
    std::string DefaultKind;
    std::string DefaultExpression;

    friend bool operator==(const TClickHouseTableColumn&, const TClickHouseTableColumn&) = default;
};

struct TResolvedColumn
{
    std::string Name;
    TClickHouseColumnType Type;
};

//! Returns INSERT columns in table order, omitting MATERIALIZED/ALIAS columns
//! and columns absent from the stream (ClickHouse applies their defaults).
//! Throws when a produced column has an incompatible type.
std::vector<TResolvedColumn> ResolveColumns(
    const std::vector<TClickHouseTableColumn>& tableColumns,
    const NTableClient::TTableSchemaPtr& streamSchema);

////////////////////////////////////////////////////////////////////////////////

class TClickHouseBlockBuilder
{
public:
    explicit TClickHouseBlockBuilder(std::vector<TResolvedColumn> columns);

    clickhouse::Block Build(const std::vector<TOutputMessageConstPtr>& messages) const;

    const std::vector<TResolvedColumn>& Columns() const;

private:
    const std::vector<TResolvedColumn> Columns_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
