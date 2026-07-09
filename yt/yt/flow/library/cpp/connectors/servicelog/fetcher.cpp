#include "fetcher.h"

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/table_client.h>
#include <yt/yt/client/api/table_reader.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <util/string/escape.h>
#include <util/string/join.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TTableFetcherSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("table_path", &TThis::TablePath)
        .Default();
    registrar.Parameter("value_columns", &TThis::ValueColumns)
        .Default();
    registrar.Parameter("attempts", &TThis::Attempts)
        .Default(1);
    registrar.Parameter("retry_timeout", &TThis::RetryTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("fetch_type", &TThis::FetchType)
        .Default(EFetchType::TableReader);
    registrar.Postprocessor([] (TTableFetcherSpec* spec) {
        const auto& tablePath = spec->TablePath;
        // The path itself must be present and non-empty.
        if (tablePath.GetPath().empty()) {
            THROW_ERROR_EXCEPTION("table_path must be set and have a non-empty path");
        }
        const auto cluster = tablePath.GetCluster();
        const auto clusters = tablePath.GetClusters();
        // Exactly one of <cluster=...> or <clusters=[...]> must provide cluster info.
        const bool hasCluster = cluster.has_value() && !cluster->empty();
        const bool hasClusters = clusters.has_value();
        if (hasClusters && clusters->empty()) {
            THROW_ERROR_EXCEPTION("table_path must not have an empty <clusters=[]> attribute")
                << TErrorAttribute("table_path", tablePath);
        }
        if (!hasCluster && !hasClusters) {
            THROW_ERROR_EXCEPTION(
                    "table_path must specify a cluster via <cluster=...> or <clusters=[...]> attribute")
                << TErrorAttribute("table_path", tablePath);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

//! Builds a parenthesized list of column names from the schema, e.g. "(col1,col2)".
std::string BuildKeysNamePrefix(const NTableClient::TTableSchemaPtr& schema, int length)
{
    std::vector<std::string> result;
    for (int i = 0; i < length; ++i) {
        result.push_back(schema->Columns()[i].Name());
    }
    return Format("(%v)", JoinSeq(",", result));
}

//! Builds a comma-separated list of all column names from the schema, e.g. "col1,col2,col3".
std::string BuildColumnsList(const NTableClient::TTableSchemaPtr& schema)
{
    std::vector<std::string> result;
    for (const auto& column : schema->Columns()) {
        result.push_back(column.Name());
    }
    return JoinSeq(",", result);
}

////////////////////////////////////////////////////////////////////////////////

//! Builds a parenthesized list of placeholder references, e.g. "({lower_0},{lower_1})".
std::string BuildKeysPlaceholderPrefix(const std::string& prefix, int length)
{
    std::vector<std::string> result;
    for (int i = 0; i < length; i++) {
        result.push_back(Format("{%v_%v}", prefix, i));
    }
    return Format("(%v)", JoinSeq(",", result));
}

//! Builds a comparison clause with placeholders like "(col1,col2) >= ({lower_0},{lower_1})".
std::string BuildParameterizedClause(
    const NTableClient::TTableSchemaPtr& schema,
    const int keyCount,
    const std::string& sign,
    const std::string& paramPrefix)
{
    return Format("%v %v %v", BuildKeysNamePrefix(schema, keyCount), sign, BuildKeysPlaceholderPrefix(paramPrefix, keyCount));
}

void AddKeyPlaceholderValues(
    NYTree::TFluentMap fluent,
    const std::string& prefix,
    const TKey& key)
{
    for (int i = 0; i < key.Underlying().GetCount(); i++) {
        fluent.Item(Format("%v_%v", prefix, i)).Value(key.Underlying()[i]);
    }
}

TParameterizedSelectRowsQuery BuildParameterizedSelectRowsQuery(
    const std::string& tablePath,
    const NTableClient::TTableSchemaPtr& schema,
    const TServiceLogRangePtr& range,
    i64 rowLimit)
{
    std::vector<std::string> clauses;

    if (range->Lower) {
        auto sign = range->Lower->Exclusive ? ">" : ">=";
        clauses.push_back(BuildParameterizedClause(schema, range->Lower->Key.Underlying().GetCount(), sign, "lower"));
    }

    if (range->Upper) {
        auto sign = range->Upper->Exclusive ? "<" : "<=";
        clauses.push_back(BuildParameterizedClause(schema, range->Upper->Key.Underlying().GetCount(), sign, "upper"));
    }

    std::string whereClause;
    if (!clauses.empty()) {
        whereClause = Format("WHERE %v", JoinSeq(" AND ", clauses));
    }

    auto query = Format("SELECT %v FROM [%v] %v ORDER BY %v LIMIT {row_limit}",
        BuildColumnsList(schema),
        tablePath,
        whereClause,
        BuildKeysNamePrefix(schema, schema->GetKeyColumnCount()));

    auto placeholderValues = NYTree::BuildYsonStringFluently()
        .BeginMap()
        .DoIf(range->Lower.has_value(),
            [&] (NYTree::TFluentMap fluent) {
                AddKeyPlaceholderValues(fluent, /*prefix*/ "lower", range->Lower->Key);
            })
        .DoIf(range->Upper.has_value(),
            [&] (NYTree::TFluentMap fluent) {
                AddKeyPlaceholderValues(fluent, /*prefix*/ "upper", range->Upper->Key);
            })
        .Item("row_limit")
        .Value(rowLimit)
        .EndMap();

    return TParameterizedSelectRowsQuery{
        .Query = std::move(query),
        .PlaceholderValues = std::move(placeholderValues),
    };
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTableFetcher);

class TTableFetcher
    : public IServiceLogRowsProvider
{
public:
    using TPayloadRow = NTableClient::TUnversionedOwningRow;

    TTableFetcher(NLogging::TLogger logger, NProfiling::TProfiler profiler, IStatusProfilerPtr statusProfiler, NClient::NCache::IClientsCachePtr clientsCache, TTableFetcherSpecPtr spec)
        : Logger(std::move(logger))
        , Profiler(std::move(profiler))
        , Clients_([spec, clientsCache] {
            THashMap<std::string, NApi::IClientPtr> clients;
            auto clusters = spec->TablePath.GetClusters();
            if (!clusters) {
                YT_VERIFY(spec->TablePath.GetCluster());
                clusters = std::vector<std::string>{*spec->TablePath.GetCluster()};
            }

            for (const auto& cluster : *clusters) {
                auto client = clientsCache->GetClient(cluster);
                clients[cluster] = std::move(client);
            }

            return clients;
        }())
        , Spec_(std::move(spec))
        , CurrentClient_(Clients_.begin())
        , ReadErrorState_(statusProfiler->ErrorState("/read"))
        , SupplementaryErrorState_(statusProfiler->ErrorState("/supplementary"))
        , RowsCounter_(Profiler.Counter("/rows_fetched"))
        , RetryableErrorsCounter_(Profiler.Counter("/retryable_errors"))
    { }

    NTableClient::TTableSchemaPtr GetSchema() override
    {
        NConcurrency::WaitFor(EnsureConfigured()).ThrowOnError();
        return Schema_;
    }

    const TTableFetcherSpecPtr& GetSpec() const
    {
        return Spec_;
    }

    TFuture<TFetchResult> FetchTableReader(const TServiceLogRangePtr& range, i64 rowLimit)
    {
        auto procedure = [this, strongThis = MakeStrong(this), range, rowLimit] () -> TFetchResult {
            NConcurrency::WaitFor(EnsureConfigured()).ThrowOnError();
            auto path = Spec_->TablePath;
            auto schema = GetSchema();
            NChunkClient::TReadRange readerRange;
            if (range->Lower) {
                if (range->Lower->Exclusive) {
                    readerRange.LowerLimit().KeyBound() = (NYT::NTableClient::TOwningKeyBound::FromRow() > NTableClient::TUnversionedOwningRow(range->Lower->Key.Underlying()));
                } else {
                    readerRange.LowerLimit().KeyBound() = (NYT::NTableClient::TOwningKeyBound::FromRow() >= NTableClient::TUnversionedOwningRow(range->Lower->Key.Underlying()));
                }
            }

            if (range->Upper) {
                if (range->Upper->Exclusive) {
                    readerRange.UpperLimit().KeyBound() = (NYT::NTableClient::TOwningKeyBound::FromRow() < NTableClient::TUnversionedOwningRow(range->Upper->Key.Underlying()));
                } else {
                    readerRange.UpperLimit().KeyBound() = (NYT::NTableClient::TOwningKeyBound::FromRow() <= NTableClient::TUnversionedOwningRow(range->Upper->Key.Underlying()));
                }
            }

            path.SetRanges({readerRange});
            auto columnsFilteredRange = schema->Columns() | std::views::transform([] (const auto& columnSchema) {
                return columnSchema.Name();
            });
            path.SetColumns(std::vector<std::string>(columnsFilteredRange.begin(), columnsFilteredRange.end()));
            typename NApi::TTableReaderOptions options{
                .EnableRangeIndex = true};
            int attemptsRemaining = Spec_->Attempts;
            NTableClient::IUnversionedRowBatchPtr batch;
            TError lastError;
            std::vector<TPayloadRow> result;

            while (attemptsRemaining > 0 && std::ssize(result) < rowLimit) {
                try {
                    if (!CachedReader_ || !CachedNextReadRange_ || *CachedNextReadRange_ != readerRange) {
                        auto client = GetClient();
                        CachedReader_ = NConcurrency::WaitFor(client->CreateTableReader(path, options)).ValueOrThrow();
                    }

                    batch = NTableClient::ReadRowBatch(*CachedReader_, NYT::NTableClient::TRowBatchReadOptions{.MaxRowsPerRead = rowLimit - std::ssize(result)});
                    if (!batch) {
                        CachedNextReadRange_ = std::nullopt;
                        break;
                    }
                    auto rows = batch->MaterializeRows();
                    YT_LOG_DEBUG("Read subbatch (Size: %v)", std::ssize(rows));
                    YT_VERIFY(!rows.empty());
                    RowsCounter_.Increment(std::ssize(rows));
                    for (auto& row : rows) {
                        result.push_back(NTableClient::TUnversionedOwningRow(row));
                    }
                    ReadErrorState_->ClearError();
                    auto& lastRow = result.back();
                    auto lastKey = GetKeyPrefix(lastRow, schema->GetKeyColumnCount());
                    readerRange.LowerLimit().KeyBound() = (NYT::NTableClient::TOwningKeyBound::FromRow() > lastKey);
                    path.SetRanges({readerRange});
                    CachedNextReadRange_ = readerRange;
                } catch (const std::exception& e) {
                    CachedReader_ = std::nullopt;

                    auto error = TError(e);
                    YT_LOG_ERROR(error, "Error fetching batch");
                    ReadErrorState_->SetError(error);
                    RetryableErrorsCounter_.Increment(1);

                    const auto& retryTimeout = Spec_->RetryTimeout;
                    NConcurrency::TDelayedExecutor::WaitForDuration(retryTimeout);

                    AdvanceClient();
                    attemptsRemaining--;
                    lastError = std::move(error);
                }
            }

            bool finished = std::ssize(result) < rowLimit && attemptsRemaining != 0;

            if (attemptsRemaining == 0) {
                THROW_ERROR_EXCEPTION(lastError);
            }
            YT_LOG_DEBUG("Retrieved rows (Count: %v)", std::ssize(result));
            return {
                .Rows = result,
                .Finished = finished};
        };
        return BIND(procedure)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    TFuture<TFetchResult> FetchSelectRows(const TServiceLogRangePtr& range, i64 rowLimit)
    {
        auto procedure = [this, strongThis = MakeStrong(this), range = CloneYsonStruct(range), rowLimit] () -> TFetchResult {
            NConcurrency::WaitFor(EnsureConfigured()).ThrowOnError();
            auto path = Spec_->TablePath;
            auto schema = GetSchema();

            int attemptsRemaining = Spec_->Attempts;
            TError lastError;
            std::vector<TPayloadRow> result;

            while (attemptsRemaining > 0 && std::ssize(result) < rowLimit) {
                try {
                    auto parameterizedQuery = BuildParameterizedSelectRowsQuery(
                        path.GetPath(),
                        schema,
                        range,
                        rowLimit - std::ssize(result));
                    YT_LOG_DEBUG("Prepare for fetch using SelectRows query (Query: %v, PlaceholderValues: %v)",
                        parameterizedQuery.Query,
                        ConvertToYsonString(parameterizedQuery.PlaceholderValues, NYson::EYsonFormat::Text));

                    auto batch = NConcurrency::WaitFor(GetClient()->SelectRows(
                        parameterizedQuery.Query,
                        {
                            .PlaceholderValues = std::move(parameterizedQuery.PlaceholderValues),
                        }))
                        .ValueOrThrow();
                    const auto rows = batch.Rowset->GetRows();
                    YT_LOG_DEBUG("Got subbatch (Size: %v)", std::ssize(rows));
                    RowsCounter_.Increment(std::ssize(rows));
                    for (auto& row : rows) {
                        result.push_back(NTableClient::TUnversionedOwningRow(row));
                    }
                    ReadErrorState_->ClearError();

                    if (rows.empty()) {
                        break;
                    }

                    auto& lastRow = result.back();
                    auto lastKey = TKey(TKey::TUnderlying(GetKeyPrefix(lastRow, schema->GetKeyColumnCount())));
                    range->Lower = TServiceLogEndpoint();
                    range->Lower->Key = lastKey;
                    range->Lower->Exclusive = true;
                } catch (const std::exception& e) {
                    auto error = TError(e);
                    YT_LOG_ERROR(error, "Error fetching batch");
                    ReadErrorState_->SetError(error);
                    RetryableErrorsCounter_.Increment(1);
                    const auto& retryTimeout = Spec_->RetryTimeout;
                    NConcurrency::TDelayedExecutor::WaitForDuration(retryTimeout);
                    AdvanceClient();
                    attemptsRemaining--;
                    lastError = std::move(error);
                }
            }

            bool finished = std::ssize(result) < rowLimit && attemptsRemaining != 0;

            if (attemptsRemaining == 0) {
                THROW_ERROR_EXCEPTION(lastError);
            }
            YT_LOG_DEBUG("Retrieved rows (Count: %v)", std::ssize(result));
            return {
                .Rows = result,
                .Finished = finished};
        };
        return BIND(procedure)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    TFuture<TFetchResult> Fetch(const TServiceLogRangePtr& range, i64 rowLimit) override
    {
        if (Spec_->FetchType == EFetchType::SelectRows) {
            return FetchSelectRows(range, rowLimit);
        } else if (Spec_->FetchType == EFetchType::TableReader) {
            return FetchTableReader(range, rowLimit);
        }
        THROW_ERROR_EXCEPTION("Unknown fetch type");
    }

    i64 GetApproximateRowCount() override
    {
        NConcurrency::WaitFor(EnsureConfigured()).ThrowOnError();
        return ApproximateRowCount_;
    }

protected:
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;

private:
    TFuture<void> Configured_;
    THashMap<std::string, NApi::IClientPtr> Clients_;
    TTableFetcherSpecPtr Spec_;
    NTableClient::TTableSchemaPtr Schema_;
    THashMap<std::string, NApi::IClientPtr>::iterator CurrentClient_;
    IStatusErrorStatePtr ReadErrorState_;
    IStatusErrorStatePtr SupplementaryErrorState_;
    NProfiling::TCounter RowsCounter_;
    NProfiling::TCounter RetryableErrorsCounter_;
    std::optional<NChunkClient::TReadRange> CachedNextReadRange_;
    std::optional<NApi::ITableReaderPtr> CachedReader_;
    i64 ApproximateRowCount_{};

    NApi::IClientPtr GetClient() const
    {
        return CurrentClient_->second;
    }

    void AdvanceClient()
    {
        CurrentClient_++;
        if (CurrentClient_ == Clients_.end()) {
            CurrentClient_ = Clients_.begin();
        }
    }

    TFuture<void> EnsureConfigured()
    {
        if (Configured_) {
            return Configured_;
        }

        auto configured = [this, strongThis = MakeStrong(this)] {
            NYT::NApi::TGetNodeOptions options;
            options.Attributes = {"schema", "row_count", "chunk_row_count"};
            options.ReadFrom = NYT::NApi::EMasterChannelKind::Cache;
            i64 attemptsRemaining = Spec_->Attempts;
            NTableClient::TTableSchemaPtr schema;
            TError lastError;
            while (attemptsRemaining > 0) {
                try {
                    auto client = GetClient();
                    auto node = ConvertTo<NYTree::INodePtr>(NConcurrency::WaitFor(client->GetNode(Spec_->TablePath.GetPath(), options)).ValueOrThrow());
                    schema = (node->Attributes().template Get<NTableClient::TTableSchemaPtr>("schema"));
                    if (auto rowCount = node->Attributes().template Find<ui64>("row_count")) {
                        ApproximateRowCount_ = *rowCount;
                    } else if (auto chunkedRowCount = node->Attributes().template Find<ui64>("chunk_row_count")) {
                        ApproximateRowCount_ = *chunkedRowCount;
                    } else {
                        THROW_ERROR_EXCEPTION("Row count attributes not found");
                    }
                    SupplementaryErrorState_->ClearError();
                    break;
                } catch (const std::exception& e) {
                    auto error = TError(e);
                    YT_LOG_ERROR(error, "Error retrieving schema");
                    SupplementaryErrorState_->SetError(error);
                    RetryableErrorsCounter_.Increment(1);
                    const auto& retryTimeout = Spec_->RetryTimeout;
                    NConcurrency::TDelayedExecutor::WaitForDuration(retryTimeout);
                    AdvanceClient();
                    attemptsRemaining--;
                    lastError = std::move(error);
                }
            }

            if (attemptsRemaining == 0) {
                THROW_ERROR_EXCEPTION(lastError);
            }

            int keyPrefix = schema->GetKeyColumnCount();

            NTableClient::TSortColumns sortColumns;
            for (int i = 0; i < keyPrefix; ++i) {
                const auto& column = schema->Columns()[i];
                sortColumns.push_back(NTableClient::TColumnSortSchema{
                    .Name = column.Name(),
                    .SortOrder = *column.SortOrder(),
                });
            }

            schema = schema->ToSorted(sortColumns);
            THashSet<std::string> allColumns;
            for (int i = 0; i < keyPrefix; i++) {
                allColumns.insert(schema->Columns()[i].Name());
            }
            if (Spec_->ValueColumns) {
                for (const auto& column : *Spec_->ValueColumns) {
                    allColumns.insert(column);
                }
            } else {
                for (const auto& column : schema->Columns()) {
                    allColumns.insert(column.Name());
                }
            }

            Schema_ = schema->Filter(allColumns);
        };

        return Configured_ = BIND(configured)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }
};

DEFINE_REFCOUNTED_TYPE(TTableFetcher);

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IServiceLogRowsProviderPtr CreateTableFetcher(
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    IStatusProfilerPtr statusProfiler,
    NClient::NCache::IClientsCachePtr clientsCache,
    const TTableFetcherSpecPtr& spec)
{
    return New<TTableFetcher>(logger, profiler, statusProfiler, clientsCache, spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
