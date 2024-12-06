#include "world_initializer_cache.h"

#include "bootstrap.h"
#include "hydra_facade.h"
#include "world_initializer.h"

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IWorldInitializerCache)

////////////////////////////////////////////////////////////////////////////////

class TWorldInitializerCache
    : public IWorldInitializerCache
{
public:
    explicit TWorldInitializerCache(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void UpdateWorldInitializationStatus(bool initialized) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        WorldInitialized_.store(initialized, std::memory_order::release);
    }

    TFuture<void> ValidateWorldInitialized() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!WorldInitialized_.load(std::memory_order::relaxed)) {

            auto invoker = Bootstrap_
                ->GetHydraFacade()
                ->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic);

            return BIND([
                weakWorldInitializer = MakeWeak(Bootstrap_->GetWorldInitializer()),
                weakThis = MakeWeak(this)
            ] {
                if (auto worldInitializer = weakWorldInitializer.Lock()) {
                    // Updates |this->WorldInitialized_|.
                    worldInitializer->UpdateWorldInitializerCache();
                }

                if (auto this_ = weakThis.Lock()) {
                    return this_->WorldInitialized_.load(std::memory_order::acquire);
                }

                return false;
            })
                .AsyncVia(invoker)
                .Run()
                .Apply(BIND([] (const TErrorOr<bool>& initialized) {
                    if (!initialized.IsOK()) {
                        auto error = TError(NRpc::EErrorCode::Unavailable, "Cluster is not initialized");
                        if (!initialized.IsOK()) {
                            error <<= initialized;
                        }
                        return error;
                    }

                    return TError();
                }));
        }

        if (!WorldInitialized_.load(std::memory_order::acquire)) {
            static const auto ErrorFuture = MakeFuture(TError(NRpc::EErrorCode::Unavailable, "Cluster is not initialized"));
            return ErrorFuture;
        }

        return VoidFuture;
    }

private:
    TBootstrap* const Bootstrap_;
    std::atomic<bool> WorldInitialized_;
};

////////////////////////////////////////////////////////////////////////////////

IWorldInitializerCachePtr CreateWorldInitializerCache(TBootstrap* bootstrap)
{
    return New<TWorldInitializerCache>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
