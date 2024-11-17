#include "persistent_state_transient_cache.h"

#include "bootstrap.h"
#include "hydra_facade.h"
#include "world_initializer.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/public.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NCellMaster {

using namespace NConcurrency;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

class TPersistentStateTransientCache
    : public IPersistentStateTransientCache
{
public:
    explicit TPersistentStateTransientCache(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void UpdateWorldInitializationStatus(bool initialized) override
    {
        WorldInitialized_.store(initialized, std::memory_order::release);
    }

    void ValidateWorldInitialized() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (WorldInitialized_.load(std::memory_order::acquire)) {
            return;
        }

        auto throwNotInitialized = [=] (const TError& nested) {
            TError error(NRpc::EErrorCode::Unavailable, "Cluster is not initialized");
            if (!nested.IsOK()) {
                error <<= nested;
            }
            THROW_ERROR error;
        };

        auto invoker = Bootstrap_
            ->GetHydraFacade()
            ->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic);

        if (!invoker) {
            // The absence of invoker means we're between epochs now.
            throwNotInitialized({});
        }

        auto initializedOrError = WaitFor(BIND([weakWorldInitializer = MakeWeak(Bootstrap_->GetWorldInitializer())] {
            auto worldInitialized = weakWorldInitializer.Lock();
            // Updates this cache.
            return worldInitialized ? worldInitialized->IsInitialized() : false;
        })
            .AsyncVia(invoker)
            .Run());

        if (!initializedOrError.ValueOrDefault(false)) {
            throwNotInitialized(initializedOrError);
        }
    }

    void ResetNodeDefaultAddresses() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NodeDefaultAddresses_.Store(THashMap<TNodeId, std::string>{});
    }

    void UpdateNodeDefaultAddress(TNodeId nodeId, std::optional<std::string> defaultAddress) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NodeDefaultAddresses_.Transform([&] (auto& defaultAddresses) {
            if (defaultAddress.has_value()) {
                defaultAddresses.emplace(nodeId, std::move(*defaultAddress));
            } else {
                defaultAddresses.erase(nodeId);
            }
        });
    }

    virtual std::string GetNodeDefaultAddress(TNodeId nodeId) override
    {
        return NodeDefaultAddresses_.Read([&] (const auto& nodeAddresses) {
            auto it = nodeAddresses.find(nodeId);
            if (it == nodeAddresses.end()) {
                THROW_ERROR_EXCEPTION(
                    NNodeTrackerClient::EErrorCode::NoSuchNode,
                    "Invalid or expired node id %v",
                    nodeId);
            }
            return it->second;
        });
    }

private:
    TBootstrap* const Bootstrap_;
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    std::atomic<bool> WorldInitialized_;
    NThreading::TAtomicObject<THashMap<TNodeId, std::string>> NodeDefaultAddresses_;
};

////////////////////////////////////////////////////////////////////////////////

IPersistentStateTransientCachePtr CreatePersistentStateTransientCache(TBootstrap* bootstrap)
{
    return New<TPersistentStateTransientCache>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
