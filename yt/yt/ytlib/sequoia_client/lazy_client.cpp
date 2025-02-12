#include "lazy_client.h"
#include "table_descriptor.h"
#include "client.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

class TLazySequoiaClient
    : public ILazySequoiaClient
{
public:
    TLazySequoiaClient(
        NApi::NNative::IClientPtr nativeClient,
        NLogging::TLogger logger)
        : NativeClient_(std::move(nativeClient))
        , Logger(std::move(logger))
    { }

    #define FORWARD_METHOD(name, args) \
        if (auto underlyingClient = UnderlyingClient_.Acquire()) { \
            return underlyingClient->name args; \
        } \
        return ReadyPromise_ \
            .ToFuture() \
            .Apply(BIND([=, this, this_ = MakeStrong(this)] { \
                return UnderlyingClient_.Acquire()->name args; \
            }).AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));

    virtual TFuture<NApi::TUnversionedLookupRowsResult> LookupRows(
        ESequoiaTable table,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const NTableClient::TColumnFilter& columnFilter,
        NTransactionClient::TTimestamp timestamp) override
    {
        FORWARD_METHOD(LookupRows, (table, keys, columnFilter, timestamp))
    }

    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        ESequoiaTable table,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp) override
    {
        FORWARD_METHOD(SelectRows, (table, query, timestamp))
    }
    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        const TSequoiaTablePathDescriptor& descriptor,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp) override
    {
        FORWARD_METHOD(SelectRows, (descriptor, query, timestamp))
    }

    TFuture<void> TrimTable(
        const TSequoiaTablePathDescriptor& descriptor,
        int tabletIndex,
        i64 trimmedRowCount) override
    {
        FORWARD_METHOD(TrimTable, (descriptor, tabletIndex, trimmedRowCount))
    }

    virtual TFuture<ISequoiaTransactionPtr> StartTransaction(
        const NApi::TTransactionStartOptions& options,
        const TSequoiaTransactionSequencingOptions& sequencingOptions) override
    {
        FORWARD_METHOD(StartTransaction, (options, sequencingOptions))
    }

    #undef FORWARD_METHOD

    const NLogging::TLogger& GetLogger() const override
    {
        return Logger;
    }

    void SetGroundClient(const NApi::NNative::IClientPtr& groundClient) override
    {
        auto underlyingClient = CreateSequoiaClient(
            NativeClient_,
            groundClient,
            Logger);
        UnderlyingClient_.Store(std::move(underlyingClient));

        bool initial = ReadyPromise_.TrySet();
        YT_LOG_INFO("Sequoia client is %v (GroundConnectionTag: %v)",
            initial ? "created" : "recreated",
            groundClient->GetNativeConnection()->GetLoggingTag());
    }

private:
    const NApi::NNative::IClientPtr NativeClient_;
    const NLogging::TLogger Logger;

    const TPromise<void> ReadyPromise_ = NewPromise<void>();

    TAtomicIntrusivePtr<ISequoiaClient> UnderlyingClient_;
};

////////////////////////////////////////////////////////////////////////////////

ILazySequoiaClientPtr CreateLazySequoiaClient(
    NApi::NNative::IClientPtr nativeClient,
    NLogging::TLogger logger)
{
    return New<TLazySequoiaClient>(
        std::move(nativeClient),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
