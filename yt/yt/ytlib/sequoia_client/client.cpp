#include "client.h"

#include "helpers.h"
#include "table_descriptor.h"
#include "transaction.h"
#include "private.h"

#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/client/table_client/record_descriptor.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NSequoiaClient {

using namespace NApi;
using namespace NApi::NNative;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NLogging;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaClient
    : public ISequoiaClient
{
public:
    TSequoiaClient(
        NNative::TSequoiaConnectionConfigPtr config,
        NNative::IClientPtr localClient,
        TFuture<NNative::IClientPtr> groundClientFuture)
        : Config_(std::move(config))
        , LocalClient_(std::move(localClient))
        , GroundClientFuture_(std::move(groundClientFuture))
    { }

    const TLogger& GetLogger() const override
    {
        return SequoiaClientLogger();
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
        XX(LookupRows, (table, keys, columnFilter, timestamp))
    }

    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        ESequoiaTable table,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp) override
    {
        XX(SelectRows, (table, query, timestamp))
    }
    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        const TSequoiaTablePathDescriptor& descriptor,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp) override
    {
        XX(SelectRows, (descriptor, query, timestamp))
    }

    TFuture<void> TrimTable(
        const TSequoiaTablePathDescriptor& descriptor,
        int tabletIndex,
        i64 trimmedRowCount) override
    {
        XX(TrimTable, (descriptor, tabletIndex, trimmedRowCount))
    }

    virtual TFuture<ISequoiaTransactionPtr> StartTransaction(
        ESequoiaTransactionType type,
        const NApi::TTransactionStartOptions& transactionStartOptions,
        const TSequoiaTransactionOptions& sequoiaTransactionOptions) override
    {
        XX(StartTransaction, (type, transactionStartOptions, sequoiaTransactionOptions))
    }

#undef XX

private:
    const NNative::TSequoiaConnectionConfigPtr Config_;
    const NNative::IClientPtr LocalClient_;
    const TFuture<NNative::IClientPtr> GroundClientFuture_;

    NNative::IClientPtr GetGroundClientOrThrow()
    {
        YT_VERIFY(GroundClientFuture_.IsSet());
        return GroundClientFuture_.Get().ValueOrThrow();
    }

    NYPath::TYPath GetSequoiaTablePath(const TSequoiaTablePathDescriptor& tablePathDescriptor)
    {
        return NSequoiaClient::GetSequoiaTablePath(LocalClient_, tablePathDescriptor);
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
        return GetGroundClientOrThrow()->LookupRows(
            GetSequoiaTablePath(tablePathDescriptor),
            tableDescriptor->GetRecordDescriptor()->GetNameTable(),
            std::move(keys),
            options)
            .ApplyUnique(BIND(MaybeWrapSequoiaRetriableError<TUnversionedLookupRowsResult>));
    }

    TFuture<TSelectRowsResult> DoSelectRows(
        ESequoiaTable table,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp)
    {
        TSequoiaTablePathDescriptor descriptor{
            .Table = table,
        };
        return DoSelectRows(descriptor, query, timestamp);
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
            builder.SetLimit(100'000);
        }

        NApi::TSelectRowsOptions options;
        options.FailOnIncompleteResult = true;
        options.AllowFullScan = false;
        options.Timestamp = timestamp;

        return GetGroundClientOrThrow()
            ->SelectRows(builder.Build(), options)
            .ApplyUnique(BIND(MaybeWrapSequoiaRetriableError<TSelectRowsResult>));
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
            LocalClient_,
            GetGroundClientOrThrow(),
            transactionStartOptions,
            sequoiaTransactionOptions);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaClientPtr CreateSequoiaClient(
    NNative::TSequoiaConnectionConfigPtr config,
    NNative::IClientPtr localClient,
    TFuture<NNative::IClientPtr> groundClientFuture)
{
    return New<TSequoiaClient>(
        std::move(config),
        std::move(localClient),
        std::move(groundClientFuture));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
