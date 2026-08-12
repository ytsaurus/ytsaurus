#include "client.h"

#include "helpers.h"
#include "private.h"
#include "sequoia_reign.h"
#include "table_descriptor.h"
#include "transaction.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/client_cache.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/client/table_client/record_descriptor.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/yt/misc/range_formatters.h>

namespace NYT::NSequoiaClient {

using namespace NApi;
using namespace NApi::NNative;
using namespace NLogging;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NRpc;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaClient
    : public ISequoiaClient
{
public:
    TSequoiaClient(
        NNative::IClientPtr authenticatedLocalClient,
        TFuture<NNative::IClientPtr> groundClientFuture)
        : AuthenticatedLocalClient_(std::move(authenticatedLocalClient))
        , GroundClientFuture_(std::move(groundClientFuture))
    { }

    NRpc::TAuthenticationIdentity GetAuthenticationIdentity() const override
    {
        return AuthenticatedLocalClient_->GetOptions().GetAuthenticationIdentity();
    }

    const TLogger& GetLogger() const override
    {
        return SequoiaClientLogger();
    }

    const TLogger& Logger() const
    {
        return GetLogger();
    }

    #define XX(name, args) \
        if (auto optionalClient = GroundClientFuture_.TryGet()) { \
            try { \
                optionalClient->ThrowOnError(); \
                return Do ## name args; \
            } catch (const std::exception& ex) { \
                return MakeFuture<decltype(Do ## name args)::TValueType>(TError(ex)); \
            } \
        } \
        return GroundClientFuture_ \
            .AsVoid() \
            .ToUncancelable() \
            .Apply(BIND([=, this, this_ = MakeStrong(this)] { \
                return Do ## name args; \
            }).AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));

    virtual TFuture<NApi::TUnversionedLookupRowsResult> LookupRows(
        ESequoiaTable table,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const NTableClient::TColumnFilter& columnFilter,
        NTransactionClient::TTimestamp timestamp) override
    {
        YT_TLOG_DEBUG("Looking up")
            .With("Table", table)
            .With("Keys", MakeShrunkFormattableView(keys, TDefaultFormatter(), 20))
            .With("Timestamp", timestamp);
        XX(LookupRows, (table, keys, columnFilter, timestamp))
    }

    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        ESequoiaTable table,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp) override
    {
        YT_TLOG_DEBUG("Selecting")
            .With("Table", table)
            .With("Query", query)
            .With("Timestamp", timestamp);
        XX(SelectRows, (table, query, timestamp))
    }

    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        const TSequoiaTablePathDescriptor& descriptor,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp) override
    {
        YT_TLOG_DEBUG("Selecting")
            .With("TablePathDescriptor", descriptor)
            .With("Query", query)
            .With("Timestamp", timestamp);
        XX(SelectRows, (descriptor, query, timestamp))
    }

    TFuture<void> TrimTable(
        const TSequoiaTablePathDescriptor& descriptor,
        int tabletIndex,
        i64 trimmedRowCount) override
    {
        YT_TLOG_DEBUG("Trimming")
            .With("TablePathDescriptor", descriptor)
            .With("TabletIndex", tabletIndex)
            .With("TrimmedRowCount", trimmedRowCount);
        XX(TrimTable, (descriptor, tabletIndex, trimmedRowCount))
    }

    virtual TFuture<ISequoiaTransactionPtr> StartTransaction(
        ESequoiaTransactionType type,
        const NApi::TTransactionStartOptions& transactionStartOptions,
        const TSequoiaTransactionOptions& sequoiaTransactionOptions) override
    {
        YT_TLOG_DEBUG("Starting transaction")
            .With("Type", type)
            .With("Id", transactionStartOptions.Id)
            .With("ParentId", transactionStartOptions.ParentId)
            .With("Timeout", transactionStartOptions.Timeout)
            .With("CellTag", transactionStartOptions.CellTag)
            .With("PrerequisiteTransactionIds", transactionStartOptions.PrerequisiteTransactionIds);
        XX(StartTransaction, (type, transactionStartOptions, sequoiaTransactionOptions))
    }

#undef XX

private:
    const NNative::IClientPtr AuthenticatedLocalClient_;
    const TFuture<NNative::IClientPtr> GroundClientFuture_;

    NNative::IClientPtr GetGroundClientOrThrow()
    {
        return GroundClientFuture_.GetOrCrash().ValueOrThrow();
    }

    NYPath::TYPath GetSequoiaTablePath(const TSequoiaTablePathDescriptor& tablePathDescriptor)
    {
        return NSequoiaClient::GetSequoiaTablePath(AuthenticatedLocalClient_, tablePathDescriptor);
    }

    TFuture<TUnversionedLookupRowsResult> DoLookupRows(
        ESequoiaTable table,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const NTableClient::TColumnFilter& columnFilter,
        NTransactionClient::TTimestamp timestamp)
    {
        NApi::TLookupRowsOptions options;
        options.KeepMissingRows = true;
        options.ColumnFilter = columnFilter;
        options.Timestamp = timestamp;

        const auto* tableDescriptor = ITableDescriptor::Get(table);
        TSequoiaTablePathDescriptor tablePathDescriptor{
            .Table = table,
        };
        auto groundClient = GetGroundClientOrThrow();
        return groundClient->LookupRows(
            GetSequoiaTablePath(tablePathDescriptor),
            tableDescriptor->GetRecordDescriptor()->GetNameTable(),
            std::move(keys),
            options)
        .Apply(BIND([groundClient] (const TErrorOr<TUnversionedLookupRowsResult>& result) -> TErrorOr<TUnversionedLookupRowsResult> {
            static NProfiling::TCounter LookupsSucceeded = SequoiaClientProfiler().Counter("/sequoia_lookups_succeeded");
            static NProfiling::TCounter LookupsFailed = SequoiaClientProfiler().Counter("/sequoia_lookups_failed");

            (result.IsOK() ? LookupsSucceeded : LookupsFailed).Increment();

            if (result.GetCode() == NHiveClient::EErrorCode::UnknownCell &&
                groundClient->GetNativeConnection()->IsTerminated())
            {
                return TError(EErrorCode::SequoiaRetriableError, "Sequoia Ground connection was probably reconfigured")
                    << TError(result);
            }

            return result;
        }));
    }

    TFuture<TSelectRowsResult> DoSelectRows(
        ESequoiaTable table,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp)
    {
        TSequoiaTablePathDescriptor descriptor{
            .Table = table,
        };
        return DoSelectRows(descriptor, query, timestamp)
            .Apply(BIND([] (const TErrorOr<TSelectRowsResult>& result) -> TErrorOr<TSelectRowsResult> {
                static NProfiling::TCounter SelectsSucceeded = SequoiaClientProfiler().Counter("/sequoia_selects_succeeded");
                static NProfiling::TCounter SelectsFailed = SequoiaClientProfiler().Counter("/sequoia_selects_failed");

                (result.IsOK() ? SelectsSucceeded : SelectsFailed).Increment();

                return result;
            }));
    }

    TFuture<TSelectRowsResult> DoSelectRows(
        const TSequoiaTablePathDescriptor& descriptor,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp)
    {
        TQueryBuilder builder;
        builder.SetSource(GetSequoiaTablePath(descriptor));
        builder.AddSelectExpression("*");
        for (const auto& whereConjunct : query.WhereConjuncts) {
            builder.AddWhereConjunct(whereConjunct);
        }
        for (const auto& orderByExpression : query.OrderBy) {
            builder.AddOrderByExpression(orderByExpression);
        }
        auto limit = query.Limit;
        if (limit) {
            builder.SetLimit(*limit);
        } else if (!query.OrderBy.empty()) {
            // TODO(h0pless): This is an arbitrary value. Remove it once ORDER BY will work with an unspecified limit.
            // For details see YT-16489.
            builder.SetLimit(100'000'000);
        }

        NApi::TSelectRowsOptions options;
        options.FailOnIncompleteResult = true;
        options.AllowFullScan = false;
        options.Timestamp = timestamp;

        auto groundClient = GetGroundClientOrThrow();
        return groundClient
            ->SelectRows(builder.Build(), options)
            .Apply(BIND([groundClient] (const TErrorOr<TSelectRowsResult>& result) -> TErrorOr<TSelectRowsResult> {
                if (result.GetCode() == NHiveClient::EErrorCode::UnknownCell &&
                    groundClient->GetNativeConnection()->IsTerminated())
                {
                    return TError(EErrorCode::SequoiaRetriableError, "Sequoia connection was probably reconfigured")
                        << TError(result);
                }

                return result;
            }));
    }

    TFuture<void> DoTrimTable(
        const TSequoiaTablePathDescriptor& descriptor,
        int tabletIndex,
        i64 trimmedRowCount)
    {
        return GetGroundClientOrThrow()->TrimTable(
            GetSequoiaTablePath(descriptor),
            tabletIndex,
            trimmedRowCount);
    }

    TFuture<ISequoiaTransactionPtr> DoStartTransaction(
        ESequoiaTransactionType type,
        const NApi::TTransactionStartOptions& transactionStartOptions,
        const TSequoiaTransactionOptions& sequoiaTransactionOptions)
    {
        return NDetail::StartSequoiaTransaction(
            this,
            type,
            AuthenticatedLocalClient_,
            GetGroundClientOrThrow(),
            transactionStartOptions,
            sequoiaTransactionOptions);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaClientPtr CreateSequoiaClient(
    NApi::NNative::IClientPtr localAuthenticatedClient,
    TFuture<NNative::IClientPtr> groundClientFuture)
{
    return New<TSequoiaClient>(
        std::move(localAuthenticatedClient),
        std::move(groundClientFuture));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
