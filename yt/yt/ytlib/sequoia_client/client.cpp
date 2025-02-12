#include "client.h"

#include "helpers.h"
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
        NApi::TLookupRowsOptions options;
        options.KeepMissingRows = true;
        options.ColumnFilter = columnFilter;
        options.Timestamp = timestamp;

        const auto* tableDescriptor = ITableDescriptor::Get(table);
        TSequoiaTablePathDescriptor tablePathDescriptor{
            .Table = table
        };
        return GroundRootClient_->LookupRows(
            GetSequoiaTablePath(NativeRootClient_, tablePathDescriptor),
            tableDescriptor->GetRecordDescriptor()->GetNameTable(),
            std::move(keys),
            options)
            .ApplyUnique(BIND(MaybeWrapSequoiaRetriableError<TUnversionedLookupRowsResult>));
    }

    TFuture<TSelectRowsResult> SelectRows(
        ESequoiaTable table,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp) override
    {
        TSequoiaTablePathDescriptor descriptor{
            .Table = table,
        };
        return SelectRows(descriptor, query, timestamp);
    }

    TFuture<TSelectRowsResult> SelectRows(
        const TSequoiaTablePathDescriptor& descriptor,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp) override
    {
        TQueryBuilder builder;
        builder.SetSource(GetSequoiaTablePath(NativeRootClient_, descriptor));
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

        return GroundRootClient_
            ->SelectRows(builder.Build(), options)
            .ApplyUnique(BIND(MaybeWrapSequoiaRetriableError<TSelectRowsResult>));
    }

    TFuture<void> TrimTable(
        const TSequoiaTablePathDescriptor& descriptor,
        int tabletIndex,
        i64 trimmedRowCount) override
    {
        return GroundRootClient_->TrimTable(
            GetSequoiaTablePath(NativeRootClient_, descriptor),
            tabletIndex,
            trimmedRowCount);
    }

    TFuture<ISequoiaTransactionPtr> StartTransaction(
        const NApi::TTransactionStartOptions& options,
        const TSequoiaTransactionSequencingOptions& sequencingOptions) override
    {
        return NDetail::StartSequoiaTransaction(
            this,
            NativeRootClient_,
            GroundRootClient_,
            options,
            sequencingOptions);
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
