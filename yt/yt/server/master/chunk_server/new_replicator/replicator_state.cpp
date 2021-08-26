#include "replicator_state.h"

#include "private.h"

#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NChunkServer::NReplicator {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ReplicatorLogger;

////////////////////////////////////////////////////////////////////////////////

class TReplicatorState
    : public IReplicatorState
{
public:
    explicit TReplicatorState(std::unique_ptr<IReplicatorStateProxy> proxy)
        : Proxy_(std::move(proxy))
        , DualMutationInvoker_(Proxy_->GetChunkInvoker(EChunkThreadQueue::DualMutation))
    {
        VerifyAutomatonThread();
    }

    virtual void Load()
    {
        VerifyAutomatonThread();

        YT_LOG_INFO("Started loading dual state");

        const auto& dynamicConfig = Proxy_->GetDynamicConfig();
        // NB: Config copying is not essential here, however occasional config change from another thread
        // will end up with a disaster, so it's better to clone it.
        DynamicConfig_ = CloneYsonSerializable(dynamicConfig);

        YT_LOG_INFO("Finished loading dual state");
    }

    virtual void SyncWithUpstream() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        WaitFor(BIND([] { })
            .AsyncVia(DualMutationInvoker_)
            .Run())
            .ThrowOnError();
    }

    virtual void UpdateDynamicConfig(const TDynamicClusterConfigPtr& newConfig) override
    {
        VerifyAutomatonThread();

        // NB: Config copying is not essential here, however occasional config change from another thread
        // will end up with a disaster, so it's better to clone it.
        auto clonedConfig = CloneYsonSerializable(newConfig);
        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoUpdateDynamicConfig,
                MakeWeak(this),
                Passed(std::move(clonedConfig))));
    }

    virtual const TDynamicClusterConfigPtr& GetDynamicConfig() const
    {
        VerifyChunkThread();

        return DynamicConfig_;
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(ChunkThread);

    std::unique_ptr<IReplicatorStateProxy> Proxy_;

    const IInvokerPtr DualMutationInvoker_;

    TDynamicClusterConfigPtr DynamicConfig_;

    void VerifyAutomatonThread() const
    {
        if (Proxy_->CheckThreadAffinity()) {
            VERIFY_THREAD_AFFINITY(AutomatonThread);
        }
    }

    void VerifyChunkThread() const
    {
        if (Proxy_->CheckThreadAffinity()) {
            VERIFY_THREAD_AFFINITY(ChunkThread);
        }
    }

    void DoUpdateDynamicConfig(TDynamicClusterConfigPtr newConfig)
    {
        VerifyChunkThread();

        DynamicConfig_ = std::move(newConfig);

        YT_LOG_DEBUG("Dynamic config updated");
    }
};

////////////////////////////////////////////////////////////////////////////////

IReplicatorStatePtr CreateReplicatorState(std::unique_ptr<IReplicatorStateProxy> proxy)
{
    return New<TReplicatorState>(std::move(proxy));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
