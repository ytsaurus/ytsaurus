#include "response_keeper.h"

#include "config.h"

#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/response_keeper.record.h>

#include <yt/yt/core/rpc/service.h>
#include <yt/yt/core/rpc/response_keeper.h>

namespace NYT::NCypressProxy {

using namespace NConcurrency;
using namespace NRpc;
using namespace NSequoiaClient;
using namespace NLogging;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaResponseKeeper
    : public ISequoiaResponseKeeper
{
public:
    TSequoiaResponseKeeper(
        TSequoiaResponseKeeperDynamicConfigPtr config,
        TLogger logger)
        : Config_(std::move(config))
        , Logger(std::move(logger))
    { }

    std::optional<TSharedRefArray> FindResponse(const IServiceContextPtr& context, const ISequoiaTransactionPtr& transaction) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_VERIFY(transaction);

        if (!GetDynamicConfig()->Enable) {
            return std::nullopt;
        }

        auto mutationId = context->GetMutationId();
        if (!mutationId) {
            return std::nullopt;
        }

        YT_LOG_DEBUG(
            "Started looking for response in response keeper (MutationId: %v, Retry: %v)",
            mutationId,
            context->IsRetry());

        if (auto keptResponseMessage = DoLookupResponse(transaction, mutationId, context->IsRetry())) {
            YT_LOG_DEBUG(
                "Replying with finished response (MutationId: %v, Retry: %v)",
                mutationId,
                context->IsRetry());

            context->SuppressMissingRequestInfoCheck();
            context->SetResponseInfo("KeptResponse: %v", true);
            return keptResponseMessage;
        }

        YT_LOG_DEBUG(
            "Response was not found in response keeper (MutationId: %v, Retry: %v)",
            mutationId,
            context->IsRetry());

        return std::nullopt;
    }

    void KeepResponse(
        const ISequoiaTransactionPtr& transaction,
        TMutationId mutationId,
        const TErrorOr<TSharedRefArray>& responseMessageOrError) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_VERIFY(mutationId);
        YT_VERIFY(transaction);

        if (!GetDynamicConfig()->Enable) {
            YT_LOG_DEBUG("Sequoia response keeper is disabled, skipping response (MutationId: %v)", mutationId);
            return;
        }

        YT_LOG_DEBUG(
            "Saving response message into response keeper (MutationId: %v)",
            mutationId);

        auto remember = responseMessageOrError.IsOK()
            ? ValidateHeaderAndParseRememberOption(responseMessageOrError.Value())
            : false;

        DoKeepResponse(
            transaction,
            mutationId,
            responseMessageOrError.IsOK()
                ? responseMessageOrError.Value()
                : CreateErrorResponseMessage(responseMessageOrError),
            remember);

        YT_LOG_DEBUG(
            "Saved response message into response keeper (MutationId: %v)",
            mutationId);
    }

    const TSequoiaResponseKeeperDynamicConfigPtr& GetDynamicConfig() const override
    {
        return Config_;
    }

    void Reconfigure(const TSequoiaResponseKeeperDynamicConfigPtr& newConfig) override
    {
        YT_LOG_DEBUG_IF(Config_->Enable != newConfig->Enable,
            "Sequoia response keeper \"enable\" state changed (OldEnable: %v, NewEnable: %v)",
            Config_->Enable,
            newConfig->Enable);
        Config_ = newConfig;
    }

private:
    TSequoiaResponseKeeperDynamicConfigPtr Config_;
    const TLogger Logger;

    void DoKeepResponse(const ISequoiaTransactionPtr& transaction, TMutationId mutationId, const TSharedRefArray& response, bool remember) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_VERIFY(mutationId);
        YT_VERIFY(transaction);

        if (!response) {
            YT_LOG_ALERT("Null response passed to response keeper (MutationId: %v, Remember: %v)",
                mutationId,
                remember);
            return;
        }

        auto responseSharedRefParts = response.ToVector();
        std::vector<std::string> responseStringParts;
        responseStringParts.reserve(responseSharedRefParts.size());
        for (const auto& sharedRefPart : responseSharedRefParts) {
            responseStringParts.push_back(std::string(sharedRefPart.ToStringBuf()));
        }

        transaction->WriteRow(NRecords::TSequoiaResponseKeeper{
            .Key = {.MutationId = mutationId},
            .Response = std::move(responseStringParts),
        });
    }

    TSharedRefArray DoLookupResponse(const ISequoiaTransactionPtr& transaction, TMutationId mutationId, bool isRetry) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_VERIFY(mutationId);
        YT_VERIFY(transaction);

        // TODO(cherepashka): add option for dynamic table, where response keeper will write.
        auto rowsOrError = WaitFor(transaction->LookupRows<NRecords::TSequoiaResponseKeeperKey>({{.MutationId = mutationId}}));

        if (!rowsOrError.IsOK()) {
            return CreateErrorResponseMessage(rowsOrError);
        }

        auto& rows = rowsOrError.Value();
        YT_VERIFY(rows.size() == 1);

        if (!rows.front()) {
            return {};
        }
        ValidateRetry(mutationId, isRetry);

        auto& responseStringParts = rows.front()->Response;
        std::vector<TSharedRef> responseSharedRefParts;
        responseSharedRefParts.reserve(responseStringParts.size());
        std::transform(
            responseStringParts.begin(),
            responseStringParts.end(),
            std::back_inserter(responseSharedRefParts),
            [] (std::string& str) {
                return TSharedRef::FromString(std::move(str));
            });

        return TSharedRefArray(std::move(responseSharedRefParts), TSharedRefArray::TMoveParts{});
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaResponseKeeperPtr CreateSequoiaResponseKeeper(
    TSequoiaResponseKeeperDynamicConfigPtr config,
    TLogger logger)
{
    return New<TSequoiaResponseKeeper>(std::move(config), std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
