#include "client.h"

#include "table_descriptor.h"
#include "transaction.h"

#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/client/table_client/record_descriptor.h>

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
        NNative::IClientPtr nativeClient,
        NNative::IClientPtr groundClient,
        TLogger logger)
        : NativeRootClient_(std::move(nativeClient))
        , GroundRootClient_(std::move(groundClient))
        , Logger(std::move(logger))
    { }

    TFuture<TUnversionedLookupRowsResult> LookupRows(
        ESequoiaTable table,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const NTableClient::TColumnFilter& columnFilter,
        NTransactionClient::TTimestamp timestamp) override
    {
        NApi::TLookupRowsOptions options = {};
        options.KeepMissingRows = true;
        options.ColumnFilter = columnFilter;
        options.Timestamp = timestamp;

        const auto* tableDescriptor = ITableDescriptor::Get(table);
        return GroundRootClient_->LookupRows(
            GetSequoiaTablePath(NativeRootClient_, tableDescriptor),
            tableDescriptor->GetRecordDescriptor()->GetNameTable(),
            std::move(keys),
            options);
    }

        TFuture<NApi::TSelectRowsResult> SelectRows(
        ESequoiaTable table,
        const TSelectRowsRequest& request,
        NTransactionClient::TTimestamp timestamp) override
    {
        auto* tableDescriptor = ITableDescriptor::Get(table);
        TQueryBuilder builder;
        builder.SetSource(GetSequoiaTablePath(NativeRootClient_, tableDescriptor));
        builder.AddSelectExpression("*");
        for (const auto& whereConjunct : request.Where) {
            builder.AddWhereConjunct(whereConjunct);
        }
        for (const auto& orderByExpression : request.OrderBy) {
            builder.AddOrderByExpression(orderByExpression);
        }
        auto limit = request.Limit;
        if (limit) {
            builder.SetLimit(*limit);
        } else if (!request.OrderBy.empty()) {
            // TODO(h0pless): This is an arbitrary value. Remove it once ORDER BY will work with an unspecified limit.
            // For details see YT-16489.
            builder.SetLimit(100'000);
        }

        NApi::TSelectRowsOptions options;
        options.FailOnIncompleteResult = true;
        options.AllowFullScan = false;
        options.Timestamp = timestamp;

        return GroundRootClient_->SelectRows(builder.Build(), options);
    }

    TFuture<ISequoiaTransactionPtr> StartTransaction(
        const NApi::TTransactionStartOptions& options) override
    {
        return NDetail::StartSequoiaTransaction(this, NativeRootClient_, GroundRootClient_, options);
    }

    const TLogger& GetLogger() const override
    {
        return Logger;
    }

private:
    const NNative::IClientPtr NativeRootClient_;
    const NNative::IClientPtr GroundRootClient_;
    const TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

// Add another client here.
ISequoiaClientPtr CreateSequoiaClient(
    NNative::IClientPtr nativeClient,
    NNative::IClientPtr groundClient,
    NLogging::TLogger logger)
{
    return New<TSequoiaClient>(
        std::move(nativeClient),
        std::move(groundClient),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
