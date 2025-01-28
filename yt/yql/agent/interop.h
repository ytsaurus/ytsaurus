#pragma once

#include "private.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

void ReorderAndSaveRows(
    NTableClient::TRowBufferPtr rowBuffer,
    NTableClient::TNameTablePtr sourceNameTable,
    NTableClient::TNameTablePtr targetNameTable,
    TRange<NTableClient::TUnversionedRow> rows,
    std::vector<NTableClient::TUnversionedRow>& resultRows);

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

TYqlRowset BuildRowsetFromSkiff(
    const NHiveClient::TClientDirectoryPtr& clientDirectory,
    const NYTree::INodePtr& resultNode,
    int resultIndex,
    i64 rowCountLimit);

////////////////////////////////////////////////////////////////////////////////

struct TWireYqlRowset
{
    TError Error;
    TSharedRef WireRowset;
    bool Incomplete = false;
    TYqlRefPtr References;
};

std::vector<TWireYqlRowset> BuildRowsets(
    const NHiveClient::TClientDirectoryPtr& clientDirectory,
    const TString& yqlYsonResults,
    i64 rowCountLimit);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
