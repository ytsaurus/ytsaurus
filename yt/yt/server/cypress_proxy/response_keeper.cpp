#include "response_keeper.h"

#include "config.h"

#include <yt/yt/server/lib/sequoia/response_keeper.h>

#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/response_keeper.record.h>

#include <yt/yt/core/rpc/service.h>
#include <yt/yt/core/rpc/response_keeper.h>

namespace NYT::NCypressProxy {

using namespace NConcurrency;
using namespace NLogging;
using namespace NRpc;
using namespace NSequoiaClient;
using namespace NSequoiaServer;
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
        auto retry = context->IsRetry();
        if (!mutationId) {
            return std::nullopt;
        }

        auto keptResponse = WaitFor(FindKeptResponseInSequoiaAndLog(transaction, mutationId, retry, Logger))
            .ValueOrThrow();
        if (keptResponse.has_value()) {
            YT_LOG_DEBUG(
                "Replying with finished response (MutationId: %v, Retry: %v)",
                mutationId,
                context->IsRetry());

            context->SuppressMissingRequestInfoCheck();
            context->SetResponseInfo("KeptResponse: %v", true);
            return keptResponse;
        }

        return std::nullopt;
    }

    void KeepResponse(
        const ISequoiaTransactionPtr& transaction,
        TMutationId mutationId,
        TSharedRefArray responseMessage) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_VERIFY(mutationId);
        YT_VERIFY(transaction);

        if (!GetDynamicConfig()->Enable) {
            YT_LOG_DEBUG("Sequoia response keeper is disabled, skipping response (MutationId: %v)", mutationId);
            return;
        }

        auto remember = ValidateHeaderAndParseRememberOption(responseMessage);
        if (!remember) {
            return;
        }

        KeepResponseInSequoiaAndLog(
            transaction,
            mutationId,
            responseMessage,
            Logger);
    }

    const TSequoiaResponseKeeperDynamicConfigPtr& GetDynamicConfig() const override
    {
        return Config_;
    }

    void Reconfigure(const TSequoiaResponseKeeperDynamicConfigPtr& newConfig) override
    {
        YT_LOG_DEBUG_IF(Config_->Enable != newConfig->Enable,
            "Sequoia response keeper %v", newConfig->Enable ? "enabled" : "disabled");
        Config_ = newConfig;
    }

private:
    TSequoiaResponseKeeperDynamicConfigPtr Config_;
    const TLogger Logger;
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
