#include "yt.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/helpers.h>

namespace NRoren::NPrivate
{
static NYT::NTableClient::TTableSchemaPtr MakeTimerSchema()
{
    NYT::NTableClient::TTableSchemaPtr schema = NYT::New<NYT::NTableClient::TTableSchema>(
        std::vector<NYT::NTableClient::TColumnSchema>({
            NYT::NTableClient::TColumnSchema("Key", NYT::NTableClient::ESimpleLogicalValueType::String, NYT::NTableClient::ESortOrder::Ascending).SetRequired(true),
            NYT::NTableClient::TColumnSchema("TimerId", NYT::NTableClient::ESimpleLogicalValueType::String, NYT::NTableClient::ESortOrder::Ascending).SetRequired(true),
            NYT::NTableClient::TColumnSchema("CallbackName", NYT::NTableClient::ESimpleLogicalValueType::String, NYT::NTableClient::ESortOrder::Ascending).SetRequired(true),
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
            NYT::NTableClient::TColumnSchema("CallbackName", NYT::NTableClient::ESimpleLogicalValueType::String, NYT::NTableClient::ESortOrder::Ascending).SetRequired(true),
            NYT::NTableClient::TColumnSchema("UserData", NYT::NTableClient::ESimpleLogicalValueType::String).SetRequired(false),
            NYT::NTableClient::TColumnSchema("dummy", NYT::NTableClient::ESimpleLogicalValueType::Int8).SetRequired(true),
        }),
        true,  // strict
        true  // uniqueKeys
    );
    return schema;
}

const NYT::NTableClient::TTableSchemaPtr g_TimerSchema = MakeTimerSchema();
const NYT::NTableClient::TNameTablePtr g_TimerLookupNameTable = NYT::NTableClient::TNameTable::FromSchema(*g_TimerSchema->ToLookup());
const NYT::NTableClient::TNameTablePtr g_TimerInsertNameTable = NYT::NTableClient::TNameTable::FromSchema(*g_TimerSchema->ToWrite());

const NYT::NTableClient::TTableSchemaPtr g_TimerIndexSchema = MakeTimerIndexSchema();
const NYT::NTableClient::TNameTablePtr g_TimerIndexLookupNameTable = NYT::NTableClient::TNameTable::FromSchema(*g_TimerIndexSchema->ToLookup());
const NYT::NTableClient::TNameTablePtr g_TimerIndexInsertNameTable = NYT::NTableClient::TNameTable::FromSchema(*g_TimerIndexSchema->ToWrite());

static TTimer TimerFromRow(const NYT::NTableClient::TUnversionedRow& row, const NYT::NTableClient::TTableSchemaPtr schema)
{
    const auto keyIndex = schema->GetColumnIndexOrThrow("Key");
    const auto timerIdIndex = schema->GetColumnIndexOrThrow("TimerId");
    const auto callbackNameIndex = schema->GetColumnIndexOrThrow("CallbackName");
    const auto timestampIndex = schema->GetColumnIndexOrThrow("Timestamp");
    const auto userDataIndex = schema->GetColumnIndexOrThrow("UserData");

    TTimer::TRawKey key{row[keyIndex].Data.String, row[keyIndex].Length};
    TTimer::TTimerId timerId{row[timerIdIndex].Data.String, row[timerIdIndex].Length};
    TTimer::TCallbackName callbackName{row[callbackNameIndex].Data.String, row[callbackNameIndex].Length};
    TTimer::TTimestamp timestamp = row[timestampIndex].Data.Uint64;
    TTimer::TUserData userData = TString{row[userDataIndex].Data.String, row[userDataIndex].Length};
    return TTimer(key, timerId, callbackName, timestamp, userData);
}

#if 0
static TYtTimerIndex TimerIndexFromRow(const NYT::NTableClient::TUnversionedRow& row, const NYT::NTableClient::TTableSchemaPtr schema)
{
    const auto shardIdIndex = schema->GetColumnIndexOrThrow("ShardId");
    return TYtTimerIndex{
        .ShardId = row[shardIdIndex].Data.Uint64,
        .Timer = TimerFromRow(row, schema)
    };
}
#endif

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
                return NYT::MakeFuture<void>({});  // skip mount cause table allready exists
            }
            return NYT::MakeFuture<void>(NYT::TError(result));
        }
    ));
}

void CreateTimerTable(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& timerTable)
{
    auto future = CreateTableAndMount(ytClient, timerTable, g_TimerSchema);
    NYT::NConcurrency::WaitFor(future).ThrowOnError();
}

void CreateTimerIndexTable(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& timerIndexTable)
{
    auto future = CreateTableAndMount(ytClient, timerIndexTable, g_TimerIndexSchema);
    NYT::NConcurrency::WaitFor(future).ThrowOnError();
}

void CreateTimerMigrateTable(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& timerMigrateTable)
{
    auto future = CreateTableAndMount(ytClient, timerMigrateTable, g_TimerIndexSchema);
    NYT::NConcurrency::WaitFor(future).ThrowOnError();
}

TVector<TTimer> YtSelectIndex(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& timerIndexTable, const TTimer::TShardId shardId, const size_t offset, const size_t limit)
{
    TVector<TTimer> result;
    TString columns;
    for (const auto& column : g_TimerSchema->Columns()) {
        if (!columns.empty()) {
            columns += ", ";
        }
        columns += column.Name();
    }
    const TString query = columns + " from [" + timerIndexTable+ "] WHERE ShardId = " + ToString(shardId) + " OFFSET " + ToString(offset) + " LIMIT " + ToString(limit);
    auto select_result = NYT::NConcurrency::WaitFor(ytClient->SelectRows(query)).ValueOrThrow();
    const auto& rows = select_result.Rowset->GetRows();
    for (const auto& row : rows) {
        result.emplace_back(TimerFromRow(row, g_TimerSchema));
    }
    return result;
}

TVector<TTimer> YtSelectMigrate(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& migrateTable, const TTimer::TShardId shardId, const size_t limit)
{
    return YtSelectIndex(ytClient, migrateTable, shardId, 0, limit);
}

TVector<TTimer> YtLookupTimers(const NYT::NApi::IClientBasePtr tx, const NYT::NYPath::TYPath& timerTable, const TVector<TTimer::TKey>& keys)
{
    NYT::NTableClient::TUnversionedRowsBuilder lookup_builder;
    for (const auto& key : keys) {
        lookup_builder.AddRow(key.GetKey(), key.GetTimerId(), key.GetCallbackName());
    }
    NYT::NApi::IUnversionedRowsetPtr rowSet = NYT::NConcurrency::WaitFor(tx->LookupRows(timerTable, g_TimerLookupNameTable, lookup_builder.Build())).ValueOrThrow();
    TVector<TTimer> result;
    for (const auto& row : rowSet->GetRows()) {
        result.emplace_back(TimerFromRow(row, g_TimerSchema));
    }
    return result;
}

void YtInsertMigrate(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& migrateTable, const TTimer& timer, const TTimer::TShardId shardId)
{
    YtInsertIndex(tx, migrateTable, timer, shardId);
}

void YtInsertTimer(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& timerTable, const TTimer& timer)
{
    NYT::NTableClient::TUnversionedRowsBuilder insert_builder;
    insert_builder.AddRow(timer.GetKey().GetKey(), timer.GetKey().GetTimerId(), timer.GetKey().GetCallbackName(), timer.GetValue().GetTimestamp(), timer.GetValue().GetUserData());
    tx->WriteRows(timerTable, g_TimerInsertNameTable, insert_builder.Build());
}

void YtDeleteTimer(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& timerTable, const TTimer::TKey& key)
{
    NYT::NTableClient::TUnversionedRowsBuilder delete_builder;
    delete_builder.AddRow(key.GetKey(), key.GetTimerId(), key.GetCallbackName());
    tx->DeleteRows(timerTable, g_TimerLookupNameTable, delete_builder.Build());
}

void YtInsertIndex(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& timerIndexTable, const TTimer& timer, const TTimer::TShardId shardId)
{
    NYT::NTableClient::TUnversionedRowsBuilder insert_builder;
    insert_builder.AddRow(shardId, timer.GetValue().GetTimestamp(), timer.GetKey().GetKey(), timer.GetKey().GetTimerId(), timer.GetKey().GetCallbackName(), nullptr, 0);
    tx->WriteRows(timerIndexTable, g_TimerIndexInsertNameTable, insert_builder.Build());
}

void YtDeleteIndex(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& timerIndexTable, const TTimer& timer, const TTimer::TShardId shardId)
{
    NYT::NTableClient::TUnversionedRowsBuilder delete_builder;
    delete_builder.AddRow(shardId, timer.GetValue().GetTimestamp(), timer.GetKey().GetKey(), timer.GetKey().GetTimerId(), timer.GetKey().GetCallbackName());
    tx->DeleteRows(timerIndexTable, g_TimerIndexLookupNameTable, delete_builder.Build());
}

}  // namespace NRoren::NPrivate

