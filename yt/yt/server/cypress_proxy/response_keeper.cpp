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

        if (!IsEnabled()) {
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

        if (!IsEnabled()) {
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

    void Reconfigure(const TSequoiaResponseKeeperDynamicConfigPtr& newConfig) override
    {
        auto newEnable = newConfig->Enable;
        auto oldEnable = Config_.Exchange(newConfig)->Enable;

        YT_LOG_DEBUG_IF(oldEnable != newEnable,
            "Sequoia response keeper %v", newEnable ? "enabled" : "disabled");
    }

private:
    TAtomicIntrusivePtr<TSequoiaResponseKeeperDynamicConfig> Config_;
    const TLogger Logger;

    bool IsEnabled() const
    {
        return Config_.Acquire()->Enable;
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
