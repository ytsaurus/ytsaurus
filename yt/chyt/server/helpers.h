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
#include <Core/QueryProcessingStage.h>
#include <Storages/ColumnsDescription.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TGuid ToGuid(DB::UUID uuid);

void RegisterNewUser(DB::AccessControl& accessControl, TString userName);

////////////////////////////////////////////////////////////////////////////////

// These functions help to convert YT key bounds to CH key bounds.

//! Retruns the minimum possible value for given dataType.
//! Throws an error if dataType is unexpected for YT-tables.
DB::Field GetMinimumTypeValue(const DB::DataTypePtr& dataType);

//! Retruns the maximum possible value for given dataType.
//! If the maximum value is unrepresentable, returns some big value.
//! Throws an error if dataType is unexpected for YT-tables.
DB::Field GetMaximumTypeValue(const DB::DataTypePtr& dataType);

// It helps to convert an exclusive right bound to inclusive in simple cases (e.g. [x, y) to [x, y - 1]).
//! Returns previous value of the type in sort order.
//! It's guaranteed that there are no values greater than returned one and less than provided one.
//! Returns std::nullopt if the decremented value is unrepresentable or not implemented yet (e.g. field=0 and dataType=UInt64).
//! Throws an error if dataType is unexpected for YT-tables.
std::optional<DB::Field> TryDecrementFieldValue(const DB::Field& field, const DB::DataTypePtr& dataType);
//! Returns next value of the type in sort order.
//! It's guaranteed that there are no values less than returned one and greater than provided one.
//! Returns std::nullopt if the decremented value is unrepresentable or not implemented yet (e.g. field=0 and dataType=UInt64).
//! Throws an error if dataType is unexpected for YT-tables.
std::optional<DB::Field> TryIncrementFieldValue(const DB::Field& field, const DB::DataTypePtr& dataType);

////////////////////////////////////////////////////////////////////////////////

TQuerySettingsPtr ParseCustomSettings(
    TQuerySettingsPtr baseSettings,
    const DB::Settings::Range& customSettings,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

//! Leaves only some of the "significant" profile counters.
THashMap<TString, size_t> GetBriefProfileCounters(const ProfileEvents::Counters::Snapshot& profileCounters);

////////////////////////////////////////////////////////////////////////////////

// Helpers to compare query processing stage.
int GetQueryProcessingStageRank(DB::QueryProcessingStage::Enum stage);
int GetDistributedInsertStageRank(EDistributedInsertStage stage);

////////////////////////////////////////////////////////////////////////////////

//! Create ASTTableExpression, which contains subquery with following structure:
//! ( SELECT <columnNames> FROM <tableExpression> WHERE <whereCondition> )
//! If |columnNames| is std::nullopt, asterisk (*) is used.
//! If |whereCondition| is nullptr, no where condition is appended.
DB::ASTPtr WrapTableExpressionWithSubquery(
    DB::ASTPtr tableExpression,
    std::optional<std::vector<TString>> columnNames = std::nullopt,
    DB::ASTPtr whereCondition = nullptr);

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
