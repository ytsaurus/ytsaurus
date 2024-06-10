#include "yt.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/helpers.h>

namespace NRoren::NPrivate
{

////////////////////////////////////////////////////////////////////////////////

static NYT::NTableClient::TTableSchemaPtr MakeTimerSchema()
{
    NYT::NTableClient::TTableSchemaPtr schema = NYT::New<NYT::NTableClient::TTableSchema>(
        std::vector<NYT::NTableClient::TColumnSchema>({
            NYT::NTableClient::TColumnSchema("Key", NYT::NTableClient::ESimpleLogicalValueType::String, NYT::NTableClient::ESortOrder::Ascending).SetRequired(true),
            NYT::NTableClient::TColumnSchema("TimerId", NYT::NTableClient::ESimpleLogicalValueType::String, NYT::NTableClient::ESortOrder::Ascending).SetRequired(true),
            NYT::NTableClient::TColumnSchema("CallbackId", NYT::NTableClient::ESimpleLogicalValueType::String, NYT::NTableClient::ESortOrder::Ascending).SetRequired(true),
            NYT::NTableClient::TColumnSchema("Timestamp", NYT::NTableClient::ESimpleLogicalValueType::Uint64).SetRequired(true),
            NYT::NTableClient::TColumnSchema("UserData", NYT::NTableClient::ESimpleLogicalValueType::String).SetRequired(false),
        }),
        true,  // strict
        true  // uniqueKeys
    );
    return schema;
}

static NYT::NTableClient::TTableSchemaPtr MakeTimerIndexSchema()
{
    NYT::NTableClient::TTableSchemaPtr schema = NYT::New<NYT::NTableClient::TTableSchema>(
        std::vector<NYT::NTableClient::TColumnSchema>({
            NYT::NTableClient::TColumnSchema("ShardId", NYT::NTableClient::ESimpleLogicalValueType::Uint64, NYT::NTableClient::ESortOrder::Ascending).SetRequired(true),
            NYT::NTableClient::TColumnSchema("Timestamp", NYT::NTableClient::ESimpleLogicalValueType::Uint64, NYT::NTableClient::ESortOrder::Ascending).SetRequired(true),
            NYT::NTableClient::TColumnSchema("Key", NYT::NTableClient::ESimpleLogicalValueType::String, NYT::NTableClient::ESortOrder::Ascending).SetRequired(true),
            NYT::NTableClient::TColumnSchema("TimerId", NYT::NTableClient::ESimpleLogicalValueType::String, NYT::NTableClient::ESortOrder::Ascending).SetRequired(true),
            NYT::NTableClient::TColumnSchema("CallbackId", NYT::NTableClient::ESimpleLogicalValueType::String, NYT::NTableClient::ESortOrder::Ascending).SetRequired(true),
            NYT::NTableClient::TColumnSchema("UserData", NYT::NTableClient::ESimpleLogicalValueType::String).SetRequired(false),
            NYT::NTableClient::TColumnSchema("dummy", NYT::NTableClient::ESimpleLogicalValueType::Int8).SetRequired(true),
        }),
        true,  // strict
        true  // uniqueKeys
    );
    return schema;
}

static NYT::NApi::TLookupRowsOptions MakeLookupRowsOptions(NYT::NTableClient::TTableSchemaPtr schema) {
    NYT::NApi::TLookupRowsOptions options;
    options.ColumnFilter = NYT::NTableClient::TColumnFilter(schema->GetColumnCount());

    return options;
}

static const NYT::NTableClient::TTableSchemaPtr StaticTimerSchema = MakeTimerSchema();
// NOTE: timer lookup name table is used both for key & column filter encoding, so it should be wide
static const NYT::NTableClient::TNameTablePtr StaticTimerLookupNameTable = NYT::NTableClient::TNameTable::FromSchema(*StaticTimerSchema->ToWrite());
static const NYT::NTableClient::TNameTablePtr StaticTimerDeleteNameTable = NYT::NTableClient::TNameTable::FromSchema(*StaticTimerSchema->ToDelete());
static const NYT::NTableClient::TNameTablePtr StaticTimerInsertNameTable = NYT::NTableClient::TNameTable::FromSchema(*StaticTimerSchema->ToWrite());
static const NYT::NApi::TLookupRowsOptions StaticTimerLookupRowsOptions = MakeLookupRowsOptions(StaticTimerSchema);

static const NYT::NTableClient::TTableSchemaPtr StaticTimerIndexSchema = MakeTimerIndexSchema();
static const NYT::NTableClient::TNameTablePtr StaticTimerIndexLookupNameTable = NYT::NTableClient::TNameTable::FromSchema(*StaticTimerIndexSchema);
static const NYT::NTableClient::TNameTablePtr StaticTimerIndexInsertNameTable = NYT::NTableClient::TNameTable::FromSchema(*StaticTimerIndexSchema->ToWrite());

static TTimer TimerFromRow(const NYT::NTableClient::TUnversionedRow& row, const NYT::NTableClient::TTableSchemaPtr schema)
{
    const auto keyIndex = schema->GetColumnIndexOrThrow("Key");
    const auto timerIdIndex = schema->GetColumnIndexOrThrow("TimerId");
    const auto callbackIdIndex = schema->GetColumnIndexOrThrow("CallbackId");
    const auto timestampIndex = schema->GetColumnIndexOrThrow("Timestamp");
    const auto userDataIndex = schema->GetColumnIndexOrThrow("UserData");

    TTimer::TRawKey key{row[keyIndex].Data.String, row[keyIndex].Length};
    TTimer::TTimerId timerId{row[timerIdIndex].Data.String, row[timerIdIndex].Length};
    TTimer::TCallbackId callbackId{row[callbackIdIndex].Data.String, row[callbackIdIndex].Length};
    TTimer::TTimestamp timestamp = row[timestampIndex].Data.Uint64;
    TTimer::TUserData userData = TString{row[userDataIndex].Data.String, row[userDataIndex].Length};
    return TTimer(key, timerId, callbackId, timestamp, userData);
}

NYT::TFuture<void> WaitForMount(const NYT::NApi::IClientBasePtr ytClient, const NYT::NYPath::TYPath path)
{
    return ytClient->GetNode(path + "/@tablet_state").Apply(BIND(
        [ytClient, path](const NYT::NYson::TYsonString& value) mutable -> NYT::TFuture<void> {
            if (NYT::NYTree::ConvertTo<NYT::NYTree::IStringNodePtr>(value)->GetValue() == "mounted") {
                return NYT::MakeFuture<void>({});
            } else {
                const TDuration delay = TDuration::Seconds(1);
                return NYT::NConcurrency::TDelayedExecutor::MakeDelayed(delay).Apply(BIND(
                    [ytClient, path=std::move(path)] () -> NYT::TFuture<void> {
                        return WaitForMount(ytClient, std::move(path));
                    }
                ));
            }
        }
    ));
}

NYT::TFuture<void> CreateTableAndMount(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath path, NYT::NTableClient::TTableSchemaPtr schema)
{
    NYT::NApi::TCreateNodeOptions options;
    options.Attributes = NYT::NYTree::CreateEphemeralAttributes();
    options.Attributes->Set("dynamic", true);
    options.Attributes->Set("schema", schema);

    return ytClient->CreateNode(path, NYT::NCypressClient::EObjectType::Table, options).Apply(BIND(
        [ytClient, path=std::move(path)] (const NYT::TErrorOr<NYT::NCypressClient::TNodeId>& result) mutable -> NYT::TFuture<void> {
            if (result.IsOK()) {
                return ytClient->MountTable(path).Apply(BIND(
                    [ytClient, path=std::move(path)] () -> NYT::TFuture<void> {
                        return WaitForMount(ytClient, path);
                    }
                ));
            }

            if (result.GetCode() == NYT::NYTree::EErrorCode::AlreadyExists) {
                return NYT::MakeFuture<void>({});  // skip mount cause table already exists
            }
            return NYT::MakeFuture<void>(NYT::TError(result));
        }
    ));
}

void CreateTimerTable(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& timerTable)
{
    auto future = CreateTableAndMount(ytClient, timerTable, StaticTimerSchema);
    NYT::NConcurrency::WaitFor(future).ThrowOnError();
}

void CreateTimerIndexTable(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& timerIndexTable)
{
    auto future = CreateTableAndMount(ytClient, timerIndexTable, StaticTimerIndexSchema);
    NYT::NConcurrency::WaitFor(future).ThrowOnError();
}

void CreateTimerMigrateTable(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& timerMigrateTable)
{
    auto future = CreateTableAndMount(ytClient, timerMigrateTable, StaticTimerIndexSchema);
    NYT::NConcurrency::WaitFor(future).ThrowOnError();
}

TVector<TTimer> YtSelectIndex(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& timerIndexTable, const TTimer::TShardId shardId, const std::optional<TTimer>& timer, const size_t limit)
{
    TVector<TTimer> result;
    TString columns;
    for (const auto& column : StaticTimerSchema->Columns()) {
        if (!columns.empty()) {
            columns += ", ";
        }
        columns += column.Name();
    }

    TString selector = "WHERE ShardId = " + ToString(shardId);
    if (timer) {
        selector += " AND (Timestamp, Key, TimerId, CallbackId) > (" + ToString(timer->GetValue().GetTimestamp()) + ",'" + timer->GetKey().GetKey() + "','" + timer->GetKey().GetTimerId() + "','" + timer->GetKey().GetCallbackId() + "')";
    }

    const TString query = columns + " from [" + timerIndexTable + "]" + selector + " LIMIT " + ToString(limit);
    auto select_result = NYT::NConcurrency::WaitFor(ytClient->SelectRows(query)).ValueOrThrow();
    const auto& rows = select_result.Rowset->GetRows();
    for (const auto& row : rows) {
        result.emplace_back(TimerFromRow(row, StaticTimerSchema));
    }
    return result;
}

TVector<TTimer> YtSelectMigrate(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& migrateTable, const TTimer::TShardId shardId, const size_t limit)
{
    return YtSelectIndex(ytClient, migrateTable, shardId, {}, limit);
}

TVector<TTimer> YtLookupTimers(const NYT::NApi::IClientBasePtr tx, const NYT::NYPath::TYPath& timerTable, const TVector<TTimer::TKey>& keys)
{
    NYT::NTableClient::TUnversionedRowsBuilder lookupBuilder;
    for (const auto& key : keys) {
        lookupBuilder.AddRow(key.GetKey(), key.GetTimerId(), key.GetCallbackId());
    }
    auto rowset = NYT::NConcurrency::WaitFor(tx->LookupRows(timerTable, StaticTimerLookupNameTable, lookupBuilder.Build(), StaticTimerLookupRowsOptions))
        .ValueOrThrow()
        .Rowset;
    TVector<TTimer> result;
    for (auto row : rowset->GetRows()) {
        result.push_back(TimerFromRow(row, StaticTimerSchema));
    }
    return result;
}

void YtInsertMigrate(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& migrateTable, const TTimer& timer, const TTimer::TShardId shardId)
{
    YtInsertIndex(tx, migrateTable, timer, shardId);
}

void YtInsertTimer(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& timerTable, const TTimer& timer)
{
    NYT::NTableClient::TUnversionedRowsBuilder insertBuilder;
    insertBuilder.AddRow(timer.GetKey().GetKey(), timer.GetKey().GetTimerId(), timer.GetKey().GetCallbackId(), timer.GetValue().GetTimestamp(), timer.GetValue().GetUserData());
    tx->WriteRows(timerTable, StaticTimerInsertNameTable, insertBuilder.Build());
}

void YtDeleteTimer(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& timerTable, const TTimer::TKey& key)
{
    NYT::NTableClient::TUnversionedRowsBuilder deleteBuilder;
    deleteBuilder.AddRow(key.GetKey(), key.GetTimerId(), key.GetCallbackId());
    tx->DeleteRows(timerTable, StaticTimerDeleteNameTable, deleteBuilder.Build());
}

void YtInsertIndex(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& timerIndexTable, const TTimer& timer, const TTimer::TShardId shardId)
{
    NYT::NTableClient::TUnversionedRowsBuilder insertBuilder;
    insertBuilder.AddRow(shardId, timer.GetValue().GetTimestamp(), timer.GetKey().GetKey(), timer.GetKey().GetTimerId(), timer.GetKey().GetCallbackId(), nullptr, 0);
    tx->WriteRows(timerIndexTable, StaticTimerIndexInsertNameTable, insertBuilder.Build());
}

void YtDeleteIndex(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& timerIndexTable, const TTimer& timer, const TTimer::TShardId shardId)
{
    NYT::NTableClient::TUnversionedRowsBuilder deleteBuilder;
    deleteBuilder.AddRow(shardId, timer.GetValue().GetTimestamp(), timer.GetKey().GetKey(), timer.GetKey().GetTimerId(), timer.GetKey().GetCallbackId());
    tx->DeleteRows(timerIndexTable, StaticTimerIndexLookupNameTable, deleteBuilder.Build());
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate

