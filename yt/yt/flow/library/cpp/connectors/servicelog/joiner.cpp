#include "joiner.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TTableJoinerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("fetchers", &TThis::Fetchers)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTableJoiner);

class TTableJoiner
    : public IServiceLogRowsProvider
{
public:
    using TPayloadRow = NTableClient::TUnversionedOwningRow;

    TTableJoiner(NLogging::TLogger logger, NProfiling::TProfiler profiler, std::vector<std::pair<std::string, IServiceLogRowsProviderPtr>>&& fetchers)
        : Logger(logger)
        , Profiler(profiler)
    {
        for (auto&& [prefix, fetcher] : fetchers) {
            Fetchers_.push_back(TFetcherData{
                .Fetcher = std::move(fetcher),
                .Prefix = std::move(prefix)});
        }
    }

    NTableClient::TTableSchemaPtr GetSchema() override
    {
        NConcurrency::WaitFor(EnsureConfigured()).ThrowOnError();
        return Schema_;
    }

    TFuture<TFetchResult> Fetch(const TServiceLogRangePtr& range, i64 rowLimit) override
    {
        auto proc = [this, strongThis = MakeStrong(this), range, rowLimit] {
            NConcurrency::WaitFor(EnsureConfigured()).ThrowOnError();
            auto futures = std::vector<TFuture<TFetchResult>>();
            for (const auto& fetcher : Fetchers_ | std::views::transform(&TFetcherData::Fetcher)) {
                futures.push_back(fetcher->Fetch(range, rowLimit));
            }
            auto results = NConcurrency::WaitFor(AllSucceeded(std::move(futures))).ValueOrThrow();

            auto falseValue = NTableClient::MakeUnversionedBooleanValue(false);
            auto trueValue = NTableClient::MakeUnversionedBooleanValue(true);
            auto nullValue = NTableClient::MakeUnversionedNullValue();

            std::map<TKey, std::vector<NTableClient::TUnversionedValue>> rowParts;
            std::optional<TKey> greatestAllowedKey;

            bool finished = true;

            for (int i = 0; i < std::ssize(Fetchers_); i++) {
                finished &= results[i].Finished;
            }

            std::vector<NTableClient::TUnversionedValue> emptyRow;

            for (int i = 0; i < std::ssize(Fetchers_); i++) {
                const auto& result = results[i].Rows;
                auto schema = Fetchers_[i].Fetcher->GetSchema();
                if (!results[i].Finished) {
                    if (result.empty()) {
                        // Last key is -inf, thus clearing everything.
                        rowParts.clear();
                        finished = false;
                        continue;
                    } else {
                        // If there are keys in the secondary fetcher that are not in the primary fetcher, we might have skipped them if processing them now.
                        auto lastKey = TKey(TKey::TUnderlying(GetKeyPrefix(result.back(), KeyPrefixSize_)));
                        if (!greatestAllowedKey || *greatestAllowedKey > lastKey) {
                            greatestAllowedKey = lastKey;
                        }
                        while (!rowParts.empty() && std::prev(rowParts.end())->first > *greatestAllowedKey) {
                            rowParts.erase(std::prev(rowParts.end()));
                            finished = false;
                        }
                    }
                }

                for (auto& row : result) {
                    auto key = TKey(TKey::TUnderlying(GetKeyPrefix(row, KeyPrefixSize_)));
                    if (greatestAllowedKey && key > *greatestAllowedKey) {
                        break;
                    }

                    auto it = rowParts.find(key);
                    if (it == rowParts.end()) {
                        it = rowParts.insert(it, {key, emptyRow});
                    }
                    auto& rowBuilt = it->second;
                    rowBuilt.push_back(trueValue);
                    for (int index = KeyPrefixSize_; index < schema->GetColumnCount(); index++) {
                        rowBuilt.push_back(row[index]);
                    }
                }

                auto fillMissingRow = [&] (auto& row) {
                    row.push_back(falseValue);
                    for (int index = KeyPrefixSize_; index < schema->GetColumnCount(); index++) {
                        row.push_back(nullValue);
                    }
                };

                for (auto& [key, row] : rowParts) {
                    if (row.size() == emptyRow.size()) {
                        fillMissingRow(row);
                    }
                }
                fillMissingRow(emptyRow);
            }

            std::vector<TPayloadRow> result;
            for (const auto& [key, row] : rowParts) {
                NTableClient::TUnversionedOwningRowBuilder builder;

                for (const auto& value : key.Underlying()) {
                    builder.AddValue(value);
                }
                for (const auto& value : row) {
                    builder.AddValue(value);
                }
                result.push_back(builder.FinishRow());
            }

            if (result.empty()) {
                YT_VERIFY(finished);
            }

            return TFetchResult{.Rows = result,
                .Finished = finished};
        };

        return BIND(proc)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    i64 GetApproximateRowCount() override
    {
        i64 rows = 0;
        for (const auto& [fetcher, prefix] : Fetchers_) {
            rows = std::max(rows, fetcher->GetApproximateRowCount());
        }
        return rows;
    }

protected:
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;

private:
    struct TFetcherData
    {
        IServiceLogRowsProviderPtr Fetcher;
        std::string Prefix;
    };

    TFuture<void> Configured_;
    std::vector<TFetcherData> Fetchers_;
    NTableClient::TTableSchemaPtr Schema_;
    THashMap<std::optional<i64>, std::vector<i64>> ValueColumnsIndices_;
    i64 KeyPrefixSize_ = 0;

    TFuture<void> EnsureConfigured()
    {
        if (Configured_) {
            return Configured_;
        }

        auto configure = [this, strongThis = MakeStrong(this)] {
            std::vector<NTableClient::TColumnSchema> columns;

            auto addValueColumnsFromFetcher = [&] (IServiceLogRowsProviderPtr fetcher, const std::string& prefix) {
                auto schema = fetcher->GetSchema();
                columns.push_back(NTableClient::TColumnSchema(prefix + "ispresent", NTableClient::EValueType::Boolean));
                for (int i = 0; i < schema->GetColumnCount(); ++i) {
                    const auto& column = schema->Columns()[i];
                    if (!column.SortOrder()) {
                        columns.push_back(column);
                        if (prefix != "") {
                            columns.back().SetName(prefix + column.Name());
                            columns.back().SetRequired(false);
                        }
                    }
                }
            };

            auto addSortColumnsFromFetcher = [&] (IServiceLogRowsProviderPtr fetcher) {
                auto schema = fetcher->GetSchema();
                for (int i = 0; i < schema->GetKeyColumnCount(); ++i) {
                    KeyPrefixSize_++;
                    const auto& column = schema->Columns()[i];
                    columns.push_back(column);
                }
            };

            addSortColumnsFromFetcher(Fetchers_.front().Fetcher);
            for (const auto& [fetcher, prefix] : Fetchers_) {
                addValueColumnsFromFetcher(fetcher, prefix);
            }

            for (auto& column : columns) {
                column.SetExpression(std::nullopt);
                column.SetMaterialized(std::nullopt);
            }

            Schema_ = New<NTableClient::TTableSchema>(columns);
        };

        return Configured_ = BIND(configure)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }
};

DEFINE_REFCOUNTED_TYPE(TTableJoiner);

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IServiceLogRowsProviderPtr CreateTableJoiner(
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    std::vector<std::pair<std::string, IServiceLogRowsProviderPtr>> fetchers)
{
    return New<TTableJoiner>(logger, profiler, std::move(fetchers));
}

IServiceLogRowsProviderPtr CreateTableJoiner(
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    IStatusProfilerPtr statusProfiler,
    NClient::NCache::IClientsCachePtr clientsCache,
    TTableJoinerSpecPtr spec)
{
    std::vector<std::pair<std::string, IServiceLogRowsProviderPtr>> fetchers;
    for (const auto& fetcherSpec : spec->Fetchers) {
        fetchers.emplace_back(
            fetcherSpec->Prefix,
            CreateTableFetcher(
                logger,
                profiler.WithPrefix("/fetcher").WithTag("fetcher_prefix", fetcherSpec->Prefix),
                statusProfiler->WithPrefix(Format("/fetchers/%v", fetcherSpec->Prefix)),
                clientsCache,
                fetcherSpec));
    }

    return CreateTableJoiner(logger, profiler, fetchers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
