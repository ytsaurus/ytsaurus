#pragma once

#include "private.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/client/api/options.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

bool ReorderAndSaveRows(
    NTableClient::TRowBufferPtr rowBuffer,
    NTableClient::TNameTablePtr sourceNameTable,
    NTableClient::TNameTablePtr targetNameTable,
    TRange<NTableClient::TUnversionedRow> rows,
    std::vector<NTableClient::TUnversionedRow>& resultRows,
    std::optional<i64> rowCountLimit = std::nullopt);

struct TYqlRef
    : public NYTree::TYsonStruct
{
    std::vector<TString> Reference;
    std::optional<std::vector<std::string>> Columns;

    REGISTER_YSON_STRUCT(TYqlRef);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_STRUCT(TYqlRef)
DEFINE_REFCOUNTED_TYPE(TYqlRef)

struct TYqlRowset
{
    NTableClient::TTableSchemaPtr TargetSchema;
    std::vector<NTableClient::TUnversionedRow> ResultRows;
    NTableClient::TRowBufferPtr RowBuffer;
    bool Incomplete = false;
    TYqlRefPtr References;
};

////////////////////////////////////////////////////////////////////////////////

struct TWireYqlRowset
{
    TError Error;
    TSharedRef WireRowset;
    bool Incomplete = false;
    TYqlRefPtr References;
};

std::vector<TWireYqlRowset> BuildRowsets(
    const std::vector<std::pair<TString, TString>>& clusters,
    const NApi::TClientOptions& clientOptions,
    const TString& yqlYsonResults,
    i64 rowCountLimit);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
