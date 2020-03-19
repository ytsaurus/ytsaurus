#pragma once

#include "private.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/client/table_client/schema.h>

#include <yt/client/ypath/public.h>

#include <yt/core/logging/public.h>

#include <Core/Field.h>
#include <Core/Block.h>
#include <Storages/ColumnsDescription.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TTableObject
    : public NChunkClient::TUserObject
{
    int ChunkCount = 0;
    bool Dynamic = false;
    NTableClient::TTableSchema Schema;
};

////////////////////////////////////////////////////////////////////////////////

DB::KeyCondition CreateKeyCondition(
    const DB::Context& context,
    const DB::SelectQueryInfo& queryInfo,
    const TClickHouseTableSchema& schema);

DB::Field ConvertToField(const NTableClient::TUnversionedValue& value);

//! `value` should have Type field filled.
void ConvertToUnversionedValue(const DB::Field& field, NTableClient::TUnversionedValue* value);

void ConvertToFieldRow(const NTableClient::TUnversionedRow& row, DB::Field* field);
void ConvertToFieldRow(const NTableClient::TUnversionedRow& row, int count, DB::Field* field);

////////////////////////////////////////////////////////////////////////////////

TClickHouseTablePtr FetchClickHouseTableFromCache(
    TBootstrap* bootstrap,
    const NApi::NNative::IClientPtr& client,
    const NYPath::TRichYPath& path,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchema ConvertToTableSchema(
    const DB::ColumnsDescription& columns,
    const NTableClient::TKeyColumns& keyColumns);

////////////////////////////////////////////////////////////////////////////////

// Converts unsupported types to compatible, e.g. int8 -> int64, etc.
NTableClient::TTableSchema AdaptSchemaToClickHouse(const NTableClient::TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

// Returns the schema with all common collumns.
// If the column is missed in any tables or the type of the column mismatch in different schemas, the column will be ommited.
// If at least in one schema the column doesn't have "required" flag, the column will be not required.
// Key columns are maximum prefix of key collumns in all schemas.
NTableClient::TTableSchema GetCommonSchema(const std::vector<NTableClient::TTableSchema>& schemas);

////////////////////////////////////////////////////////////////////////////////

// Truncate ytSubquery(<long base64-encoded stuff>) to make it human-readable.
TString MaybeTruncateSubquery(TString query);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

namespace DB {

/////////////////////////////////////////////////////////////////////////////

TString ToString(const IAST& ast);

void Serialize(const QueryStatusInfo& queryStatusInfo, NYT::NYson::IYsonConsumer* consumer);
void Serialize(const ProcessListForUserInfo& processListForUserInfo, NYT::NYson::IYsonConsumer* consumer);

TString ToString(const Block& block);

/////////////////////////////////////////////////////////////////////////////

} // namespace DB

namespace std {

/////////////////////////////////////////////////////////////////////////////

TString ToString(const std::shared_ptr<DB::IAST>& astPtr);

/////////////////////////////////////////////////////////////////////////////

} // namespace std
