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

DB::Field ConvertToField(const NTableClient::TUnversionedValue& value);

//! `value` should have Type field filled.
void ConvertToUnversionedValue(const DB::Field& field, NTableClient::TUnversionedValue* value);

void ConvertToFieldRow(const NTableClient::TUnversionedRow& row, DB::Field* field);
void ConvertToFieldRow(const NTableClient::TUnversionedRow& row, int count, DB::Field* field);

////////////////////////////////////////////////////////////////////////////////

// Returns the schema with all common columns.
// If the column is missed in any tables or the type of the column mismatch in different schemas, the column will be ommited.
// If at least in one schema the column doesn't have "required" flag, the column will be not required.
// Key columns are maximum prefix of key collumns in all schemas.
NTableClient::TTableSchema InferCommonSchema(const std::vector<TTablePtr>& tables, const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

// Truncate ytSubquery(<long base64-encoded stuff>) to make it human-readable.
TString MaybeTruncateSubquery(TString query);

////////////////////////////////////////////////////////////////////////////////

//! Leaves only some of the "significant" profile counters.
THashMap<TString, size_t> GetBriefProfileCounters(const ProfileEvents::Counters& profileCounters);

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
