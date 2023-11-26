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

    TFuture<IUnversionedRowsetPtr> LookupRows(
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
        const std::vector<TString>& whereConjuncts,
        std::optional<i64> limit,
        NTransactionClient::TTimestamp timestamp) override
    {
        auto* tableDescriptor = ITableDescriptor::Get(table);
        TQueryBuilder builder;
        builder.SetSource(GetSequoiaTablePath(NativeRootClient_, tableDescriptor));
        builder.AddSelectExpression("*");
        for (const auto& whereConjunct : whereConjuncts) {
            builder.AddWhereConjunct(whereConjunct);
        }
        if (limit) {
            builder.SetLimit(*limit);
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
        return NDetail::StartSequoiaTransaction(this, options);
    }

    const TLogger& GetLogger() const override
    {
        return Logger;
    }

    const NNative::IClientPtr& GetNativeRootClient() const override
    {
        return NativeRootClient_;
    }

    const NNative::IClientPtr& GetGroundRootClient() const override
    {
        return GroundRootClient_;
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
    return New<TSequoiaClient>(std::move(nativeClient), std::move(groundClient), logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
