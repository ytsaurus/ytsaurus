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
        NNative::IClientPtr nativeClient)
        : Config_(std::move(config))
        , NativeClient_(std::move(nativeClient))
    { }

    void Initialize()
    {
        if (Config_->GroundClusterName) {
            NativeClient_
                ->GetNativeConnection()
                ->GetClusterDirectory()
                ->SubscribeOnClusterUpdated(
                    BIND_NO_PROPAGATE([this, weakThis = MakeWeak(this)] (const std::string& clusterName, const NYTree::INodePtr& /*configNode*/) {
                        auto this_ = weakThis.Lock();
                        if (!this_) {
                            return;
                        }

                        if (clusterName == *Config_->GroundClusterName) {
                            auto groundConnection = NativeClient_
                                ->GetNativeConnection()
                                ->GetClusterDirectory()
                                ->GetConnection(*Config_->GroundClusterName);
                            auto groundClient = groundConnection->CreateNativeClient({.User = NSecurityClient::RootUserName});
                            SetGroundClient(std::move(groundClient));
                        }
                    }));
        } else {
            // When Sequoia is local it's safe to create the client right now.
            SetGroundClient(NativeClient_);
        }
    }

    const TLogger& GetLogger() const override
    {
        return SequoiaClientLogger();
    }

    #define XX(name, args) \
        if (auto groundClient = GetGroundClient()) { \
            return Do ## name args; \
        } \
        return GroundClientReadyPromise_ \
            .ToFuture() \
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
        const NApi::TTransactionStartOptions& options,
        const TSequoiaTransactionSequencingOptions& sequencingOptions) override
    {
        XX(StartTransaction, (type, options, sequencingOptions))
    }

#undef XX

private:
    const NNative::TSequoiaConnectionConfigPtr Config_;
    const NNative::IClientPtr NativeClient_;

    const TPromise<void> GroundClientReadyPromise_ = NewPromise<void>();
    TAtomicIntrusivePtr<NNative::IClient> GroundClient_;

    NNative::IClientPtr GetGroundClient() const
    {
        return GroundClient_.Acquire();
    }

    void SetGroundClient(const NNative::IClientPtr& groundClient)
    {
        GroundClient_.Store(groundClient);

        bool initial = GroundClientReadyPromise_.TrySet();

        const auto& Logger = GetLogger();
        YT_LOG_INFO("Sequoia client is %v (GroundConnectionTag: %v)",
            initial ? "created" : "recreated",
            groundClient->GetNativeConnection()->GetLoggingTag());
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
            .Table = table
        };
        return GetGroundClient()->LookupRows(
            GetSequoiaTablePath(NativeClient_, tablePathDescriptor),
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
        builder.SetSource(GetSequoiaTablePath(NativeClient_, descriptor));
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

        return GetGroundClient()
            ->SelectRows(builder.Build(), options)
            .ApplyUnique(BIND(MaybeWrapSequoiaRetriableError<TSelectRowsResult>));
    }

    TFuture<void> DoTrimTable(
        const TSequoiaTablePathDescriptor& descriptor,
        int tabletIndex,
        i64 trimmedRowCount)
    {
        return GetGroundClient()->TrimTable(
            GetSequoiaTablePath(NativeClient_, descriptor),
            tabletIndex,
            trimmedRowCount);
    }

    TFuture<ISequoiaTransactionPtr> DoStartTransaction(
        ESequoiaTransactionType type,
        const NApi::TTransactionStartOptions& options,
        const TSequoiaTransactionSequencingOptions& sequencingOptions)
    {
        return NDetail::StartSequoiaTransaction(
            this,
            type,
            NativeClient_,
            GetGroundClient(),
            options,
            sequencingOptions);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaClientPtr CreateSequoiaClient(
    NNative::TSequoiaConnectionConfigPtr config,
    NNative::IClientPtr nativeClient)
{
    auto sequoiaClient = New<TSequoiaClient>(
        std::move(config),
        std::move(nativeClient));
    sequoiaClient->Initialize();
    return sequoiaClient;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
