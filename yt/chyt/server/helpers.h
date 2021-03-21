#pragma once

#include "private.h"

#include "format.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/logging/public.h>

#include <Core/Field.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Storages/ColumnsDescription.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TGuid ToGuid(DB::UUID uuid);

void RegisterNewUser(DB::AccessControlManager& accessControlManager, TString userName);

////////////////////////////////////////////////////////////////////////////////

// These functions help to convert YT key bounds to CH key bounds.

// CH doesn't have EValueType::Min, so we replace it with the minimum possible value.
//! Retruns the minimum possible value for given dataType.
//! Throws an error if dataType is unexpected for YT-tables.
DB::Field TryGetMinimumTypeValue(const DB::DataTypePtr& dataType);

// Same as above, but replaces EValueType::Max.
//! Retruns the maximum possible value for given dataType.
//! If the maximum value is unrepresentable, returns some big value.
//! Throws an error if dataType is unexpected for YT-tables.
DB::Field TryGetMaximumTypeValue(const DB::DataTypePtr& dataType);

//! It helps to convert an exclusive right bound to inclusive in simple cases (e.g. [x, y) to [x, y - 1]).
//! It's guaranteed that there are no values greater than returned one and less than provided one.
//! Returns std::nullopt if the decremented value is unrepresentable or unsupported yet (e.g. field=0 and dataType=UInt64).
//! Throws an error if dataType is unexpected for YT-tables.
std::optional<DB::Field> TryDecrementFieldValue(const DB::Field& field, const DB::DataTypePtr& dataType);

////////////////////////////////////////////////////////////////////////////////

TQuerySettingsPtr ParseCustomSettings(
    TQuerySettingsPtr baseSettings,
    const DB::Settings::Range& customSettings,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

// Returns the schema with all common columns.
// If the column is missed in any tables or the type of the column mismatch in different schemas, the column will be ommited.
// If at least in one schema the column doesn't have "required" flag, the column will be not required.
// Key columns are maximum prefix of key collumns in all schemas.
NTableClient::TTableSchemaPtr InferCommonSchema(const std::vector<TTablePtr>& tables, const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

//! Leaves only some of the "significant" profile counters.
THashMap<TString, size_t> GetBriefProfileCounters(const ProfileEvents::Counters& profileCounters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

namespace DB {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const QueryStatusInfo& queryStatusInfo, NYT::NYson::IYsonConsumer* consumer);
void Serialize(const ProcessListForUserInfo& processListForUserInfo, NYT::NYson::IYsonConsumer* consumer);

TString ToString(const Field& field);

TString ToString(const Block& block);

void PrintTo(const Field& field, ::std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

} // namespace DB
