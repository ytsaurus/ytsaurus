#include "simple_external_state_manager.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/select_literals.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>
#include <yt/yt/flow/library/cpp/common/yt_path_option.h>

#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/string/join.h>

namespace NYT::NFlow {

using namespace NTableClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TUnversionedValue TSimpleExternalState::GetColumn(TStringBuf columnName) const
{
    YT_ASSERT(Schema);
    const auto columnId = Schema->GetColumnIndexOrThrow(columnName);
    return Payload.Underlying()[columnId];
}

TUnversionedValue TSimpleExternalState::GetColumn(int columnId) const
{
    return Payload.Underlying()[columnId];
}

void TSimpleExternalState::Clear()
{
    TPayloadBuilder builder(Schema);
    Payload = builder.Finish();
}

bool TSimpleExternalState::IsEmpty() const
{
    return NYT::NFlow::IsEmpty(Payload);
}

NYson::TYsonString TSimpleExternalState::ToYsonView() const
{
    if (!Schema || Payload.Underlying().GetCount() == 0) {
        return NYson::TYsonString(TStringBuf("#"));
    }
    return NYTree::BuildYsonStringFluently().DoMap([this] (auto fluent) {
        const auto count = std::min<int>(
            Schema->GetColumnCount(),
            Payload.Underlying().GetCount());
        for (int i = 0; i < count; ++i) {
            const auto& column = Schema->Columns()[i];
            const auto& value = Payload.Underlying()[i];
            fluent.Item(column.Name()).Value(UnversionedValueToYson(value));
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

namespace NSimpleExternalState {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void ValidateTableSchema(const TTableSchema& keySchema, const TTableSchema& tableSchema)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        keySchema.GetColumnCount() < tableSchema.GetColumnCount(),
        "Too few columns in table schema");
    for (int i = 0; i < keySchema.GetColumnCount(); ++i) {
        const auto& keyColumn = keySchema.Columns()[i];
        const auto& column = tableSchema.Columns()[i];
        if (column.Name() != keyColumn.Name()) {
            THROW_ERROR_EXCEPTION(
                "Table schema differs from group-by schema "
                "(ColumnName: %v, GroupByColumnName: %v)",
                column.Name(),
                keyColumn.Name());
        }
        const auto& columnType = *MakeOptionalIfNot(column.LogicalType());
        const auto& keyColumnType = *MakeOptionalIfNot(keyColumn.LogicalType());
        if (columnType != keyColumnType) {
            THROW_ERROR_EXCEPTION(
                "Table schema differs from group-by schema "
                "(ColumnName: %v, ColumnType: %v, GroupByColumnType: %v)",
                column.Name(),
                columnType,
                keyColumnType);
        }
        if (column.Expression() != keyColumn.Expression()) {
            THROW_ERROR_EXCEPTION(
                "Table schema differs from group-by schema "
                "(ColumnName: %v, ColumnExpression: %v, GroupByColumnExpression: %v)",
                column.Name(),
                column.Expression(),
                keyColumn.Expression());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TKeyLookupBatch
{
    TNameTablePtr NameTable;
    TSharedRange<TUnversionedRow> Rows;
};

TKeyLookupBatch BuildKeyLookupBatch(const TTableSchemaPtr& keySchema, TRange<TKey> keys)
{
    auto lookupSchema = keySchema->ToSorted(keySchema->GetColumnNames())->ToLookup();
    auto nameTable = TNameTable::FromSchema(*lookupSchema);

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TUnversionedRow> rows;
    rows.reserve(keys.size());

    for (const auto& key : keys) {
        int nextId = 0;
        TUnversionedRowBuilder builder;
        for (int i = 0; i < keySchema->GetColumnCount(); ++i) {
            const auto& column = keySchema->Columns()[i];
            if (!column.Expression()) {
                auto value = key.Underlying()[i];
                value.Id = nextId;
                nextId += 1;
                builder.AddValue(value);
            }
        }
        rows.push_back(rowBuffer->CaptureRow(builder.GetRow()));
    }

    return {
        .NameTable = std::move(nameTable),
        .Rows = MakeSharedRange(std::move(rows), std::move(rowBuffer)),
    };
}

////////////////////////////////////////////////////////////////////////////////

TLoadedStates ParseKeyLookupResult(
    const TTableSchemaPtr& keySchema,
    const TTableSchemaPtr& expectedStateSchema,
    const TUnversionedLookupRowsResult& result)
{
    auto resultSchema = result.Rowset->GetSchema();
    ValidateTableSchema(*keySchema, *resultSchema);

    auto keyCount = keySchema->GetColumnCount();
    // Canonicalize the state schema so a YT-side column reorder doesn't look
    // like a real schema change.
    auto resultStateSchema = New<TTableSchema>(std::vector(
        resultSchema->Columns().begin() + keyCount,
        resultSchema->Columns().end()));
    auto canonicalStateSchema = resultStateSchema->ToCanonical();

    if (expectedStateSchema && *expectedStateSchema != *canonicalStateSchema) {
        THROW_ERROR_EXCEPTION("State schemas disagree")
            << TErrorAttribute("expected_state_schema", expectedStateSchema)
            << TErrorAttribute("actual_state_schema", canonicalStateSchema);
    }
    auto stateSchema = expectedStateSchema ? expectedStateSchema : canonicalStateSchema;

    std::vector<int> resultPosToCanonical(resultStateSchema->GetColumnCount());
    for (int i = 0; i < resultStateSchema->GetColumnCount(); ++i) {
        resultPosToCanonical[i] = stateSchema->GetColumnIndexOrThrow(
            resultStateSchema->Columns()[i].Name());
    }

    auto rows = result.Rowset->GetRows();
    std::vector<TPayload> payloads;
    payloads.reserve(rows.Size());
    for (const auto& row : rows) {
        TPayloadBuilder builder(stateSchema);
        for (const auto& value : row) {
            if (value.Id < keyCount) {
                continue;
            }
            builder.SetValue(value, resultPosToCanonical[value.Id - keyCount]);
        }
        payloads.push_back(builder.Finish());
    }

    return {
        .StateSchema = std::move(stateSchema),
        .Payloads = std::move(payloads),
    };
}

////////////////////////////////////////////////////////////////////////////////

void EnsureSchema(
    NTableClient::TTableSchemaPtr& slot,
    const NTableClient::TTableSchemaPtr& candidate,
    const TKey& key)
{
    if (!slot) {
        slot = candidate;
    } else if (*slot != *candidate) {
        THROW_ERROR_EXCEPTION("State schemas disagree")
            << TErrorAttribute("expected_state_schema", slot)
            << TErrorAttribute("cached_state_schema", candidate)
            << TErrorAttribute("key", key);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TOperator::TOperator(
    TTableSchemaPtr keySchema,
    IClientBasePtr client,
    NYPath::TYPath path)
    : KeySchema_(std::move(keySchema))
    , Client_(std::move(client))
    , Path_(std::move(path))
{ }

TFuture<TLoadedStates> TOperator::Lookup(
    TRange<TKey> keys,
    TTableSchemaPtr expectedStateSchema) const
{
    auto batch = BuildKeyLookupBatch(KeySchema_, keys);

    TLookupRowsOptions options;
    options.KeepMissingRows = true;
    options.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
    return Client_->LookupRows(
        Path_,
        std::move(batch.NameTable),
        batch.Rows,
        options)
        .AsUnique()
        .Apply(BIND([keySchema = KeySchema_, expectedStateSchema = std::move(expectedStateSchema)] (TUnversionedLookupRowsResult&& result) {
            return ParseKeyLookupResult(keySchema, expectedStateSchema, result);
        }));
}

void TOperator::Write(
    const IRetryableTransactionPtr& tx,
    const TTableSchemaPtr& stateSchema,
    const THashMap<TKey, TPayload>& oldPayloads,
    const THashMap<TKey, TPayload>& newPayloads) const
{
    YT_VERIFY(stateSchema);
    YT_VERIFY(oldPayloads.size() == newPayloads.size());

    auto lookupKeySchema = KeySchema_->ToSorted(KeySchema_->GetColumnNames())->ToLookup();
    auto fullSchema = New<TTableSchema>(
        ConcatVectors(lookupKeySchema->Columns(), stateSchema->Columns()));
    auto rowBuffer = New<TRowBuffer>();
    std::vector<TRowModification> rows;

    for (const auto& [key, oldPayload] : oldPayloads) {
        const auto& newPayload = GetOrCrash(newPayloads, key);
        if (TBitwiseUnversionedRowEqual()(oldPayload.Underlying(), newPayload.Underlying())) {
            continue;
        }

        TUnversionedRowBuilder builder;
        int nextId = 0;
        for (int i = 0; i < KeySchema_->GetColumnCount(); ++i) {
            const auto& column = KeySchema_->Columns()[i];
            if (!column.Expression()) {
                auto value = key.Underlying()[i];
                value.Id = nextId;
                nextId += 1;
                builder.AddValue(value);
            }
        }

        if (IsEmpty(newPayload)) {
            auto row = rowBuffer->CaptureRow(builder.GetRow());
            rows.push_back(NRowModifications::TDeleteRow(row));
        } else {
            YT_VERIFY(oldPayload.Underlying().GetCount() == newPayload.Underlying().GetCount());
            for (int i = 0; i < newPayload.Underlying().GetCount(); ++i) {
                const auto& oldValue = oldPayload.Underlying()[i];
                const auto& newValue = newPayload.Underlying()[i];
                if (!TBitwiseUnversionedValueEqual()(oldValue, newValue)) {
                    auto value = newValue;
                    value.Id = nextId;
                    builder.AddValue(value);
                }
                nextId += 1;
            }
            auto row = rowBuffer->CaptureRow(builder.GetRow());
            rows.push_back(NRowModifications::TWriteRow(row));
        }
    }

    auto nameTable = TNameTable::FromSchema(*fullSchema);
    tx->ModifyRows(
        Path_,
        nameTable,
        MakeSharedRange(std::move(rows), std::move(rowBuffer)));
}

TFuture<TOperator::TListedKeys> TOperator::ListKeys(
    std::optional<TKey> lowerKey,
    std::optional<TKey> upperKey,
    std::optional<TKey> offsetExclusive,
    i64 limit) const
{
    std::vector<std::string> columnNames;
    columnNames.reserve(KeySchema_->GetColumnCount());
    for (const auto& column : KeySchema_->Columns()) {
        columnNames.push_back(std::string(column.Name()));
    }
    auto columnTuple = JoinSeq(",", columnNames);

    auto formatBound = [&] (TStringBuf op, const TKey& key) {
        int count = key.Underlying().GetCount();
        return Format("%v %v %v",
            BuildColumnTuple(*KeySchema_, count),
            op,
            BuildLiteralTuple(key, *KeySchema_));
    };

    std::vector<std::string> conditions;
    // Min/Max sentinels carry no real bound — skip them so the SELECT builder
    // doesn't try to materialize them as literals.
    if (lowerKey && *lowerKey != MinKey()) {
        conditions.push_back(formatBound(">=", *lowerKey));
    }
    if (upperKey && *upperKey != MaxKey()) {
        conditions.push_back(formatBound("<", *upperKey));
    }
    if (offsetExclusive) {
        conditions.push_back(formatBound(">", *offsetExclusive));
    }

    std::string where;
    if (!conditions.empty()) {
        where = Format(" WHERE %v", JoinSeq(" AND ", conditions));
    }
    auto query = Format("%v FROM [%v]%v ORDER BY %v LIMIT %v",
        columnTuple,
        Path_,
        where,
        columnTuple,
        limit);

    TSelectRowsOptions options;
    options.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
    return Client_->SelectRows(query, options)
        .AsUnique()
        .Apply(BIND([keySchema = KeySchema_, limit] (TSelectRowsResult&& result) {
            TListedKeys listed;
            const auto& rowset = result.Rowset;
            const auto& resultSchema = rowset->GetSchema();
            std::vector<int> resultColumnIds;
            resultColumnIds.reserve(keySchema->GetColumnCount());
            for (const auto& column : keySchema->Columns()) {
                resultColumnIds.push_back(resultSchema->GetColumnIndexOrThrow(column.Name()));
            }
            auto rows = rowset->GetRows();
            listed.Keys.reserve(rows.Size());
            for (const auto& row : rows) {
                TUnversionedOwningRowBuilder builder(keySchema->GetColumnCount());
                for (int i = 0; i < keySchema->GetColumnCount(); ++i) {
                    auto value = row[resultColumnIds[i]];
                    value.Id = i;
                    builder.AddValue(value);
                }
                listed.Keys.push_back(TKey(TKey::TUnderlying(builder.FinishRow())));
            }
            if (std::ssize(listed.Keys) == limit) {
                listed.OffsetExclusive = listed.Keys.back();
            }
            return listed;
        }));
}

////////////////////////////////////////////////////////////////////////////////

void TCachedValue::Compress()
{ }

void TCachedValue::Decompress()
{ }

i64 TCachedValue::GetWeight()
{
    return Payload.Underlying().GetSpaceUsed();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSimpleExternalState

////////////////////////////////////////////////////////////////////////////////

void TSimpleExternalStateManagerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .AddOption(EYTPathOwnership::ExclusiveWrite);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicSimpleExternalStateManagerSpec::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

TSimpleExternalStateManager::TSimpleExternalStateManager(
    TExternalStateManagerContextPtr context,
    TDynamicExternalStateManagerContextPtr dynamicContext)
    : TExternalStateManagerBase<TSimpleExternalState>(context, std::move(dynamicContext))
    , StateCache_(context->StateCache)
    , Operator_(context->KeySchema, GetClient(), GetParameters()->Path.GetPath())
    , Logger(context->Logger)
{
    YT_VERIFY(StateCache_);
}

TFuture<void> TSimpleExternalStateManager::PreloadKeyStates(const THashSet<TKey>& keys)
{
    YT_TLOG_DEBUG("Preloading keys")
        .With("Count", keys.size());

    auto guard = Guard(Lock_);
    YT_VERIFY(!EpochState_);
    EpochState_ = TEpochState{};

    std::vector<TKey> keysToLoad;
    keysToLoad.reserve(keys.size());
    for (const auto& key : keys) {
        if (auto cached = ExtractCachedState(key)) {
            NSimpleExternalState::EnsureSchema(EpochState_->StateSchema, cached->Schema, key);
            EmplaceOrCrash(EpochState_->OldStates, key, cached->Payload);
            auto state = New<TStateHolder>();
            state->Get().Payload = cached->Payload;
            state->Get().Schema = EpochState_->StateSchema;
            EmplaceOrCrash(EpochState_->States, key, std::move(state));
        } else {
            keysToLoad.push_back(key);
        }
    }

    if (keysToLoad.empty()) {
        YT_TLOG_DEBUG("All keys served from cache")
            .With("CachedCount", keys.size());
        return OKFuture;
    }

    YT_TLOG_DEBUG("Loading keys from YT")
        .With("CachedCount", keys.size() - keysToLoad.size())
        .With("LoadCount", keysToLoad.size());

    return Operator_.Lookup(keysToLoad, EpochState_->StateSchema)
        .AsUnique()
        .Apply(BIND([this, strongThis = MakeStrong(this), keys = std::move(keysToLoad)] (NSimpleExternalState::TLoadedStates&& loaded) {
            auto guard = Guard(Lock_);
            YT_TLOG_DEBUG("Preloaded keys")
                .With("Count", keys.size());
            YT_VERIFY(EpochState_);
            YT_VERIFY(std::ssize(keys) == std::ssize(loaded.Payloads));
            NSimpleExternalState::EnsureSchema(EpochState_->StateSchema, loaded.StateSchema, keys.front());
            for (int i = 0; i < std::ssize(keys); ++i) {
                EmplaceOrCrash(EpochState_->OldStates, keys[i], loaded.Payloads[i]);
                auto newState = New<TStateHolder>();
                newState->Get().Payload = std::move(loaded.Payloads[i]);
                newState->Get().Schema = EpochState_->StateSchema;
                EmplaceOrCrash(EpochState_->States, keys[i], std::move(newState));
            }
        })
                .AsyncVia(GetCurrentInvoker()));
}

void TSimpleExternalStateManager::Sync(IRetryableTransactionPtr transaction)
{
    auto guard = Guard(Lock_);
    if (!EpochState_ || EpochState_->OldStates.empty()) {
        YT_TLOG_DEBUG("Nothing to sync");
        EpochState_ = std::nullopt;
        return;
    }
    YT_TLOG_DEBUG("Syncing")
        .With("Count", EpochState_->OldStates.size());
    YT_VERIFY(EpochState_->StateSchema);

    THashMap<TKey, TPayload> newPayloads;
    newPayloads.reserve(EpochState_->States.size());
    for (const auto& [key, state] : EpochState_->States) {
        EmplaceOrCrash(newPayloads, key, state->Get().Payload);
    }

    Operator_.Write(transaction, EpochState_->StateSchema, EpochState_->OldStates, newPayloads);

    for (const auto& [key, payload] : newPayloads) {
        UpdateCache(key, payload, EpochState_->StateSchema);
    }

    EpochState_ = std::nullopt;
}

IStateHolderPtr TSimpleExternalStateManager::GetState(const TKey& key)
{
    auto guard = Guard(Lock_);
    YT_TLOG_DEBUG("GetState")
        .With("Key", key);
    YT_VERIFY(EpochState_);
    return GetOrCrash(EpochState_->States, key);
}

TFuture<IExternalStateManager::TListResult> TSimpleExternalStateManager::List(
    TFilter filter,
    i64 limit,
    std::optional<TKey> offsetExclusive)
{
    return Operator_.ListKeys(
        std::move(filter.LowerKey),
        std::move(filter.UpperKey),
        std::move(offsetExclusive),
        limit)
        .AsUnique()
        .Apply(BIND([] (NSimpleExternalState::TOperator::TListedKeys&& listed) {
            return TListResult{
                .Keys = std::move(listed.Keys),
                .OffsetExclusive = std::move(listed.OffsetExclusive),
            };
        }));
}

NSimpleExternalState::TCachedValuePtr TSimpleExternalStateManager::ExtractCachedState(const TKey& key)
{
    auto value = StateCache_->Extract(key);
    if (!value) {
        return nullptr;
    }
    auto cached = DynamicPointerCast<NSimpleExternalState::TCachedValue>(value);
    YT_VERIFY(cached);
    return cached;
}

void TSimpleExternalStateManager::UpdateCache(const TKey& key, const TPayload& payload, const TStateSchemaPtr& schema)
{
    auto cached = New<NSimpleExternalState::TCachedValue>();
    cached->Payload = payload;
    cached->Schema = schema;
    StateCache_->Insert(key, std::move(cached));
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleExternalStateJoinerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .AddOption(EYTPathOwnership::ReadOnly);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicSimpleExternalStateJoinerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("cache", &TThis::Cache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TSimpleExternalStateJoiner::TSimpleExternalStateJoiner(
    TExternalStateJoinerContextPtr context,
    TDynamicExternalStateJoinerContextPtr dynamicContext)
    : TExternalStateJoinerBase<TSimpleExternalState>(context, dynamicContext)
    , StateCache_(New<TExpiringJobNamedStateCache>(context->StateCache, GetDynamicParameters()->Cache))
    , Operator_(
        context->KeySchema,
        GetClient(GetParameters()->Path.GetCluster()),
        GetParameters()->Path.GetPath())
    , Logger(context->Logger)
{
    YT_VERIFY(StateCache_);

    SubscribeReconfigured(BIND([this] (const TDynamicExternalStateJoinerContextPtr& /*dynamicContext*/) {
        StateCache_->Reconfigure(GetDynamicParameters()->Cache);
    }));
}

TFuture<void> TSimpleExternalStateJoiner::PreloadKeyStates(const THashSet<TKey>& keys)
{
    YT_TLOG_DEBUG("Preloading keys")
        .With("Count", keys.size());

    auto guard = Guard(Lock_);

    std::vector<TKey> keysToLoad;
    keysToLoad.reserve(keys.size());
    for (const auto& key : keys) {
        if (States_.contains(key)) {
            continue;
        }
        if (auto extraction = ExtractCachedState(key)) {
            NSimpleExternalState::EnsureSchema(StateSchema_, extraction->Schema, key);
            auto state = New<TStateHolder>();
            state->Get().Payload = std::move(extraction->Payload);
            state->Get().Schema = StateSchema_;
            EmplaceOrCrash(States_, key, std::move(state));
            EmplaceOrCrash(Cookies_, key, extraction->Cookie);
        } else {
            keysToLoad.push_back(key);
        }
    }

    if (keysToLoad.empty()) {
        YT_TLOG_DEBUG("All keys served from cache")
            .With("CachedCount", keys.size());
        return OKFuture;
    }

    YT_TLOG_DEBUG("Loading keys from YT")
        .With("CachedCount", keys.size() - keysToLoad.size())
        .With("LoadCount", keysToLoad.size());

    return Operator_.Lookup(keysToLoad, StateSchema_)
        .AsUnique()
        .Apply(BIND([this, strongThis = MakeStrong(this), keys = std::move(keysToLoad)] (NSimpleExternalState::TLoadedStates&& loaded) {
            auto guard = Guard(Lock_);
            YT_TLOG_DEBUG("Preloaded keys")
                .With("Count", keys.size());
            YT_VERIFY(std::ssize(keys) == std::ssize(loaded.Payloads));
            NSimpleExternalState::EnsureSchema(StateSchema_, loaded.StateSchema, keys.front());
            for (int i = 0; i < std::ssize(keys); ++i) {
                auto state = New<TStateHolder>();
                state->Get().Payload = std::move(loaded.Payloads[i]);
                state->Get().Schema = StateSchema_;
                EmplaceOrCrash(States_, keys[i], std::move(state));
            }
        })
                .AsyncVia(GetCurrentInvoker()));
}

void TSimpleExternalStateJoiner::Reset()
{
    auto guard = Guard(Lock_);
    YT_TLOG_DEBUG("Resetting")
        .With("Count", States_.size());

    for (const auto& [key, state] : States_) {
        std::optional<TCacheCookie> cookie;
        if (auto it = Cookies_.find(key); it != Cookies_.end()) {
            cookie = it->second;
        }
        UpdateCache(key, state->Get().Payload, state->Get().Schema, cookie);
    }
    States_.clear();
    Cookies_.clear();
}

IStateHolderPtr TSimpleExternalStateJoiner::GetState(const TKey& key)
{
    auto guard = Guard(Lock_);
    YT_TLOG_DEBUG("GetState")
        .With("Key", key);
    return GetOrCrash(States_, key);
}

TFuture<IExternalStateJoiner::TListResult> TSimpleExternalStateJoiner::List(
    TFilter filter,
    i64 limit,
    std::optional<TKey> offsetExclusive)
{
    return Operator_.ListKeys(
        std::move(filter.LowerKey),
        std::move(filter.UpperKey),
        std::move(offsetExclusive),
        limit)
        .AsUnique()
        .Apply(BIND([] (NSimpleExternalState::TOperator::TListedKeys&& listed) {
            return TListResult{
                .Keys = std::move(listed.Keys),
                .OffsetExclusive = std::move(listed.OffsetExclusive),
            };
        }));
}

std::optional<TSimpleExternalStateJoiner::TCachedExtraction> TSimpleExternalStateJoiner::ExtractCachedState(const TKey& key)
{
    auto extracted = StateCache_->Extract(key);
    if (!extracted) {
        return std::nullopt;
    }
    auto& [value, cookie] = *extracted;
    auto cached = DynamicPointerCast<NSimpleExternalState::TCachedValue>(value);
    YT_VERIFY(cached);
    return TCachedExtraction{
        .Payload = cached->Payload,
        .Schema = cached->Schema,
        .Cookie = cookie,
    };
}

void TSimpleExternalStateJoiner::UpdateCache(
    const TKey& key,
    const TPayload& payload,
    const TStateSchemaPtr& schema,
    const std::optional<TCacheCookie>& cookie)
{
    auto cached = New<NSimpleExternalState::TCachedValue>();
    cached->Payload = payload;
    cached->Schema = schema;
    StateCache_->Insert(key, std::move(cached), cookie);
}

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_EXTERNAL_STATE_MANAGER(TSimpleExternalStateManager);
YT_FLOW_DEFINE_EXTERNAL_STATE_JOINER(TSimpleExternalStateJoiner);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
