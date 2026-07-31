#include "static_table_key_visitor_joiner.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/payload.h>

#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/retryable_client_spec.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/table_reader.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

#include <ranges>

namespace NYT::NFlow {

using namespace NTableClient;
using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TStaticTableKeyVisitorJoinerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("unavailable_source_policy", &TThis::UnavailableSourcePolicy)
        .Default(EUnavailableSourcePolicy::Retry);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicStaticTableKeyVisitorJoinerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("read_attempts", &TThis::ReadAttempts)
        .GreaterThanOrEqual(1)
        .Default(3);
    registrar.Parameter("unavailable_source_backoff", &TThis::UnavailableSourceBackoff)
        .Default(TDuration::Minutes(5));
}

////////////////////////////////////////////////////////////////////////////////

TStaticTableKeyVisitorJoiner::TStaticTableKeyVisitorJoiner(
    TExternalStateJoinerContextPtr context,
    TDynamicExternalStateJoinerContextPtr dynamicContext)
    : TExternalStateJoinerBase<TSimpleExternalState>(context, dynamicContext)
    , UnavailableSourcePolicy_(GetParameters()->UnavailableSourcePolicy)
    , KeySchema_(context->KeySchema)
    , Client_(GetClient(GetParameters()->Path.GetCluster()))
    , Path_(GetParameters()->Path)
    , Logger(context->Logger)
    , ListedSizeGauge_(context->Profiler.Gauge("/static_table_key_visitor_joiner/listed_size"))
    , ReaderOpensCounter_(context->Profiler.Counter("/static_table_key_visitor_joiner/reader_opens"))
    , SourceUnavailableGauge_(context->Profiler.Gauge("/static_table_key_visitor_joiner/source_unavailable"))
    , FailedReadsCounter_(context->Profiler.Counter("/static_table_key_visitor_joiner/failed_reads"))
{
    THROW_ERROR_EXCEPTION_IF(
        HasKeySchemaOverride(),
        "Key-visitor joiner does not support key_schema_override");
    SourceUnavailableGauge_.Update(0);
}

bool TStaticTableKeyVisitorJoiner::IsVisitorDriven() const
{
    return VisitorDriven;
}

namespace {

////////////////////////////////////////////////////////////////////////////////

//! Validates that |tableSchema|'s key prefix matches |keySchema| by names and types only.
//! Computed-column expressions are deliberately not compared: a static source stores the
//! partition column materialized, with no expression.
void ValidateTableSchema(const TTableSchema& keySchema, const TTableSchema& tableSchema)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        keySchema.GetColumnCount() <= tableSchema.GetColumnCount(),
        "Too few columns in table schema");
    for (int i = 0; i < keySchema.GetColumnCount(); ++i) {
        const auto& keyColumn = keySchema.Columns()[i];
        const auto& column = tableSchema.Columns()[i];
        if (column.Name() != keyColumn.Name()) {
            THROW_ERROR_EXCEPTION(
                "Table schema differs from group-by schema: "
                "column name %Qv does not match group-by column name %Qv",
                column.Name(),
                keyColumn.Name());
        }
        const auto& columnType = *MakeOptionalIfNot(column.LogicalType());
        const auto& keyColumnType = *MakeOptionalIfNot(keyColumn.LogicalType());
        if (columnType != keyColumnType) {
            THROW_ERROR_EXCEPTION(
                "Table schema differs from group-by schema: "
                "column %Qv has type %v while group-by column has type %v",
                column.Name(),
                columnType,
                keyColumnType);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TStaticTableKeyVisitorJoiner::TSchemaBundle TStaticTableKeyVisitorJoiner::FetchSchemaBundle()
{
    // A table reader over a dynamic table performs far worse than a select; reject it up front
    // instead of silently degrading.
    auto dynamicYson = WaitFor(Client_->GetNode(Path_.GetPath() + "/@dynamic"))
        .ValueOrThrow();
    THROW_ERROR_EXCEPTION_IF(
        NYTree::ConvertTo<bool>(dynamicYson),
        "Static-table key-visitor joiner requires a static table, but %v is dynamic",
        Path_.GetPath());

    auto schemaYson = WaitFor(Client_->GetNode(Path_.GetPath() + "/@schema"))
        .ValueOrThrow();
    auto tableSchema = NYTree::ConvertTo<TTableSchemaPtr>(schemaYson);
    ValidateTableSchema(*KeySchema_, *tableSchema);

    int keyCount = KeySchema_->GetColumnCount();
    auto resultStateSchema = New<TTableSchema>(std::vector(
        tableSchema->Columns().begin() + keyCount,
        tableSchema->Columns().end()));
    auto stateSchema = resultStateSchema->ToCanonical();

    auto columnNamesView = tableSchema->Columns() | std::views::transform([] (const TColumnSchema& column) {
        return column.Name();
    });
    std::vector<std::string> columnNames(columnNamesView.begin(), columnNamesView.end());

    THashMap<std::string, TColumnTarget> targetByColumnName;
    for (int i = 0; i < keyCount; ++i) {
        targetByColumnName[tableSchema->Columns()[i].Name()] =
            TColumnTarget{.IsKey = true, .Position = i};
    }
    for (int i = 0; i < resultStateSchema->GetColumnCount(); ++i) {
        const auto& name = resultStateSchema->Columns()[i].Name();
        targetByColumnName[name] =
            TColumnTarget{.IsKey = false, .Position = stateSchema->GetColumnIndexOrThrow(name)};
    }

    return TSchemaBundle{
        .StateSchema = std::move(stateSchema),
        .KeyCount = keyCount,
        .ColumnNames = std::move(columnNames),
        .TargetByColumnName = std::move(targetByColumnName),
    };
}

NApi::ITableReaderPtr TStaticTableKeyVisitorJoiner::OpenForwardReader(const TKey& lower, const TSchemaBundle& bundle)
{
    auto path = Path_;
    NChunkClient::TReadRange readerRange;
    if (lower != MinKey()) {
        readerRange.LowerLimit().KeyBound() =
            TOwningKeyBound::FromRow() >= TUnversionedOwningRow(lower.Underlying());
    }
    path.SetRanges({readerRange});
    path.SetColumns(bundle.ColumnNames);

    ReaderOpensCounter_.Increment();
    return WaitFor(Client_->CreateTableReader(path, /*options*/ {}))
        .ValueOrThrow();
}

TStaticTableKeyVisitorJoiner::TBufferedRow TStaticTableKeyVisitorJoiner::DecodeRow(
    TUnversionedRow row,
    const TSchemaBundle& bundle,
    const TNameTablePtr& nameTable)
{
    std::vector<TUnversionedValue> keyValues(bundle.KeyCount);
    for (int i = 0; i < bundle.KeyCount; ++i) {
        keyValues[i] = MakeUnversionedNullValue(i);
    }
    TPayloadBuilder payloadBuilder(bundle.StateSchema);
    for (const auto& value : row) {
        auto it = bundle.TargetByColumnName.find(std::string(nameTable->GetName(value.Id)));
        if (it == bundle.TargetByColumnName.end()) {
            continue;
        }
        const auto& target = it->second;
        if (target.IsKey) {
            auto keyValue = value;
            keyValue.Id = target.Position;
            keyValues[target.Position] = keyValue;
        } else {
            payloadBuilder.SetValue(value, target.Position);
        }
    }

    TUnversionedOwningRowBuilder keyBuilder(bundle.KeyCount);
    for (const auto& keyValue : keyValues) {
        keyBuilder.AddValue(keyValue);
    }

    return TBufferedRow{
        .Key = TKey(TKey::TUnderlying(keyBuilder.FinishRow())),
        .Payload = payloadBuilder.Finish(),
    };
}

TFuture<TStaticTableKeyVisitorJoiner::TListedStates> TStaticTableKeyVisitorJoiner::DoListStates(
    TFilter filter,
    i64 limit)
{
    return BIND([
        =,
        this,
        strongThis = MakeStrong(this),
        lowerKey = std::move(filter.LowerKey),
        upperKey = std::move(filter.UpperKey)
    ] () -> TListedStates {
        auto lower = lowerKey.value_or(MinKey());
        auto upper = upperKey.value_or(MaxKey());

        // Detach the reader state into locals so a read that throws leaves the members cleared and
        // the next List reopens cleanly. Reopen only on a backward jump: the reader spans the sweep.
        auto previousServableFrom = ServableFrom_;
        bool reopen = !Reader_ || !ServableFrom_ || lower < *ServableFrom_ || (lower == *ServableFrom_ && !ServableInclusive_);

        auto bundle = std::move(SchemaBundle_);
        auto reader = std::move(Reader_);
        auto overshoot = std::move(Overshoot_);
        bool readerExhausted = ReaderExhausted_;
        SchemaBundle_.reset();
        Reader_.Reset();
        Overshoot_.clear();
        if (reopen) {
            reader.Reset();
            overshoot.clear();
            readerExhausted = false;
        }

        if (!bundle) {
            bundle = FetchSchemaBundle();
        }

        if (reopen) {
            reader = OpenForwardReader(lower, *bundle);
        }

        TListedStates listed;
        listed.StateSchema = bundle->StateSchema;

        auto ensureBuffered = [&] () -> bool {
            if (!overshoot.empty()) {
                return true;
            }
            if (readerExhausted || !reader) {
                return false;
            }
            while (true) {
                auto batch = ReadRowBatch(reader, TRowBatchReadOptions{.MaxRowsPerRead = limit});
                if (!batch) {
                    readerExhausted = true;
                    return false;
                }
                auto rows = batch->MaterializeRows();
                if (rows.empty()) {
                    WaitFor(reader->GetReadyEvent())
                        .ThrowOnError();
                    continue;
                }
                for (const auto& row : rows) {
                    overshoot.push_back(DecodeRow(row, *bundle, reader->GetNameTable()));
                }
                return true;
            }
        };

        std::optional<TKey> newServableFrom;
        bool newServableInclusive = true;
        while (std::ssize(listed.Keys) < limit) {
            if (!ensureBuffered()) {
                newServableFrom = upper;
                break;
            }
            auto& row = overshoot.front();
            if (row.Key < lower) {
                overshoot.pop_front();
                continue;
            }
            if (row.Key >= upper) {
                newServableFrom = upper;
                break;
            }
            listed.Keys.push_back(row.Key);
            listed.Payloads.push_back(std::move(row.Payload));
            overshoot.pop_front();
        }
        if (std::ssize(listed.Keys) == limit) {
            newServableFrom = listed.Keys.back();
            newServableInclusive = false;
        }

        YT_ASSERT(reopen || !previousServableFrom || !newServableFrom || !(*newServableFrom < *previousServableFrom));
        SchemaBundle_ = std::move(bundle);
        Reader_ = std::move(reader);
        Overshoot_ = std::move(overshoot);
        ReaderExhausted_ = readerExhausted;
        ServableFrom_ = std::move(newServableFrom);
        ServableInclusive_ = newServableInclusive;
        return listed;
    })
        .AsyncVia(GetContext()->SerializedInvoker)
        .Run();
}

TFuture<IExternalStateJoiner::TListResult> TStaticTableKeyVisitorJoiner::List(
    TFilter filter,
    i64 limit,
    std::optional<TKey> offsetExclusive)
{
    // The visitor always lists each throttled range from scratch, never continuing from an offset;
    // the covered-range bookkeeping below relies on this.
    YT_VERIFY(!offsetExclusive);
    auto lower = filter.LowerKey.value_or(MinKey());
    auto upper = filter.UpperKey.value_or(MaxKey());

    // While the source is marked unavailable, resolve immediately instead of paying the read
    // timeout on every List; the first List past the mark is the probe.
    TError unavailableError;
    {
        auto guard = Guard(Lock_);
        if (TInstant::Now() < NextAttemptTime_) {
            unavailableError = TError("Source is marked unavailable until the next probe")
                << TErrorAttribute("next_attempt_time", NextAttemptTime_)
                << LastSourceError_;
        }
    }
    if (!unavailableError.IsOK()) {
        if (UnavailableSourcePolicy_ == EUnavailableSourcePolicy::Retry) {
            return MakeFuture<TListResult>(std::move(unavailableError));
        }
        return MakeFuture(HandleFailedList(std::move(unavailableError), lower, upper));
    }

    // Cap the client's retry schedule by the attempt budget so an unavailable source fails this
    // List quickly instead of eating the generic schedule; a healthy slow read is not cut short.
    {
        auto clientSpec = New<TDynamicRetryableClientSpec>();
        clientSpec->Backoff.InvocationCount = GetDynamicParameters()->ReadAttempts;
        DynamicPointerCast<IRetryableClient>(Client_)->Reconfigure(clientSpec);
    }
    return DoListStates(std::move(filter), limit)
        .AsUnique()
        .Apply(BIND([this, strongThis = MakeStrong(this), lower, upper, limit] (TListedStates&& listed) {
            auto guard = Guard(Lock_);
            YT_VERIFY(std::ssize(listed.Keys) == std::ssize(listed.Payloads));
            NextAttemptTime_ = TInstant::Zero();
            LastSourceError_ = {};
            SourceUnavailableGauge_.Update(0);
            for (int i = 0; i < std::ssize(listed.Keys); ++i) {
                Listed_[listed.Keys[i]] = TListedRow{
                    .Payload = std::move(listed.Payloads[i]),
                    .Schema = listed.StateSchema,
                };
            }
            // On a cap-hit read only the prefix up to the last returned key was covered; a key
            // beyond it stays "unreadable" rather than being mistaken for "absent".
            auto covered = (std::ssize(listed.Keys) == limit && !listed.Keys.empty())
                ? listed.Keys.back()
                : upper;
            AddListedRange(TKeyRange{.Lower = lower, .Upper = covered});
            YT_TLOG_DEBUG("Listed states")
                .With("Count", listed.Keys.size())
                .With("ListedSize", Listed_.size());
            return TListResult{
                .Keys = std::move(listed.Keys),
            };
        }))
        .Apply(BIND([this, strongThis = MakeStrong(this), lower, upper] (const TErrorOr<TListResult>& result) {
            if (result.IsOK()) {
                return result.Value();
            }
            auto backoff = GetDynamicParameters()->UnavailableSourceBackoff;
            {
                auto guard = Guard(Lock_);
                NextAttemptTime_ = TInstant::Now() + backoff;
                LastSourceError_ = TError(result);
                FailedReadsCounter_.Increment();
                SourceUnavailableGauge_.Update(1);
            }
            YT_TLOG_WARNING("Failed to list states; source marked unavailable")
                .With("Backoff", backoff)
                .With(result);
            return HandleFailedList(TError(result), lower, upper);
        }));
}

IExternalStateJoiner::TListResult TStaticTableKeyVisitorJoiner::HandleFailedList(
    TError error,
    const TKey& lower,
    const TKey& upper)
{
    if (UnavailableSourcePolicy_ == EUnavailableSourcePolicy::Retry) {
        THROW_ERROR std::move(error);
    }
    // The most recent list attempt over this range failed: retract its coverage so the range's
    // keys resolve as unreadable rather than absent.
    auto guard = Guard(Lock_);
    ListedRanges_ = SubtractRanges(
        std::move(ListedRanges_),
        {TKeyRange{.Lower = lower, .Upper = upper}});
    return {};
}

void TStaticTableKeyVisitorJoiner::AddListedRange(const TKeyRange& range)
{
    ListedRanges_.push_back(range);
    ListedRanges_ = UniteRanges(std::move(ListedRanges_));
}

TFuture<void> TStaticTableKeyVisitorJoiner::PreloadKeyStates(const THashSet<TKey>& keys)
{
    auto guard = Guard(Lock_);
    YT_TLOG_DEBUG("Preloading visit keys")
        .With("Count", keys.size());

    for (const auto& key : keys) {
        if (auto it = Listed_.find(key); it != Listed_.end()) {
            auto state = New<TStateHolder>();
            state->Get().Payload = std::move(it->second.Payload);
            state->Get().Schema = std::move(it->second.Schema);
            // Consumed rows are dropped right away: a later visit of the same key is always
            // preceded by its own List, which re-caches the row.
            Listed_.erase(it);
            States_[key] = std::move(state);
        } else if (!IsInListedRange(key)) {
            States_[key] = nullptr;
        } else {
            States_[key] = New<TStateHolder>();
        }
    }

    return OKFuture;
}

IStateHolderPtr TStaticTableKeyVisitorJoiner::GetState(const TKey& key)
{
    auto guard = Guard(Lock_);
    auto it = States_.find(key);
    THROW_ERROR_EXCEPTION_IF(it == States_.end(),
        "Key-visitor joiner has no preloaded state for key %v: only visit keys of the bound "
        "key_visitor stream may be accessed",
        key);
    return it->second;
}

void TStaticTableKeyVisitorJoiner::Reset()
{
    auto guard = Guard(Lock_);
    States_.clear();

    ListedSizeGauge_.Update(std::ssize(Listed_));
    YT_TLOG_DEBUG("Reset key-visitor joiner")
        .With("ListedSize", Listed_.size());

    // The forward reader, the cached schema and #ListedRanges_ deliberately survive: Reset runs
    // once per epoch, whereas a sweep spans many epochs. Dropping the reader or the schema here
    // would reopen and re-read them every epoch; dropping the coverage would misclassify a key
    // whose visit is consumed an epoch after its range was listed as unreadable instead of
    // absent. Coverage is retracted only by a failed re-list over the range.
}

bool TStaticTableKeyVisitorJoiner::IsInListedRange(const TKey& key) const
{
    for (const auto& range : ListedRanges_) {
        if (range.Contains(key)) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
