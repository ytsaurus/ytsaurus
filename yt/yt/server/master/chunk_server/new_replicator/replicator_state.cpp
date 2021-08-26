#include "replicator_state.h"

#include "private.h"
#include "medium.h"

#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/config.h>
#include <yt/yt/server/master/chunk_server/medium.h>

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

        CheckThreadAffinity_ = false;

        const auto& dynamicConfig = Proxy_->GetDynamicConfig();
        // NB: Config copying is not essential here, however occasional config change from another thread
        // will end up with a disaster, so it's better to clone it.
        DynamicConfig_ = CloneYsonSerializable(dynamicConfig);

        for (auto* primaryMedium : Proxy_->GetMedia()) {
            auto mediumId = primaryMedium->GetId();
            auto dualMedium = TMedium::FromPrimary(primaryMedium);
            YT_VERIFY(Media_.emplace(mediumId, std::move(dualMedium)).second);
            RegisterMediumInMaps(mediumId);
        }

        CheckThreadAffinity_ = true;

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
        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoUpdateDynamicConfig,
                MakeWeak(this),
                CloneYsonSerializable(newConfig)));
    }

    virtual void CreateMedium(NChunkServer::TMedium* medium) override
    {
        VerifyAutomatonThread();

        auto dualMedium = TMedium::FromPrimary(medium);
        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoCreateMedium,
                MakeWeak(this),
                Passed(std::move(dualMedium))));
    }

    virtual void RenameMedium(TMediumId mediumId, const TString& newName) override
    {
        VerifyAutomatonThread();

        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoRenameMedium,
                MakeWeak(this),
                mediumId,
                newName));
    }

    virtual void UpdateMediumConfig(TMediumId mediumId, const TMediumConfigPtr& newConfig) override
    {
        VerifyAutomatonThread();

        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoUpdateMediumConfig,
                MakeWeak(this),
                mediumId,
                CloneYsonSerializable(newConfig)));
    }

    virtual const TDynamicClusterConfigPtr& GetDynamicConfig() const override
    {
        VerifyChunkThread();

        return DynamicConfig_;
    }

    virtual const THashMap<TMediumId, std::unique_ptr<TMedium>>& Media() const override
    {
        VerifyChunkThread();

        return Media_;
    }

    virtual TMedium* FindMedium(TMediumId mediumId) const override
    {
        VerifyChunkThread();

        if (auto mediumIt = Media_.find(mediumId); mediumIt != Media_.end()) {
            return mediumIt->second.get();
        } else {
            return nullptr;
        }
    }

    virtual TMedium* GetMedium(TMediumId mediumId) const override
    {
        VerifyChunkThread();

        return GetOrCrash(Media_, mediumId).get();
    }

    virtual TMedium* FindMediumByIndex(TMediumIndex index) const override
    {
        VerifyChunkThread();

        if (auto mediumIt = MediumIndexToMedium_.find(index); mediumIt != MediumIndexToMedium_.end()) {
            return mediumIt->second;
        } else {
            return nullptr;
        }
    }

    virtual TMedium* FindMediumByName(const TString& name) const override
    {
        VerifyChunkThread();

        if (auto mediumIt = MediumNameToMedium_.find(name); mediumIt != MediumNameToMedium_.end()) {
            return mediumIt->second;
        } else {
            return nullptr;
        }
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(ChunkThread);

    std::unique_ptr<IReplicatorStateProxy> Proxy_;

    const IInvokerPtr DualMutationInvoker_;

    TDynamicClusterConfigPtr DynamicConfig_;

    THashMap<TMediumId, std::unique_ptr<TMedium>> Media_;
    THashMap<TMediumIndex, TMedium*> MediumIndexToMedium_;
    THashMap<TString, TMedium*> MediumNameToMedium_;

    bool CheckThreadAffinity_ = true;

    void VerifyAutomatonThread() const
    {
        if (CheckThreadAffinity_ && Proxy_->CheckThreadAffinity()) {
            VERIFY_THREAD_AFFINITY(AutomatonThread);
        }
    }

    void VerifyChunkThread() const
    {
        if (CheckThreadAffinity_ && Proxy_->CheckThreadAffinity()) {
            VERIFY_THREAD_AFFINITY(ChunkThread);
        }
    }

    void DoUpdateDynamicConfig(TDynamicClusterConfigPtr newConfig)
    {
        VerifyChunkThread();

        DynamicConfig_ = std::move(newConfig);

        YT_LOG_DEBUG("Dynamic config updated");
    }

    void DoCreateMedium(std::unique_ptr<TMedium> medium)
    {
        VerifyChunkThread();

        auto mediumId = medium->GetId();
        auto* mediumPtr = medium.get();
        // TODO(gritukan): Use helpers from Max.
        YT_VERIFY(Media_.emplace(mediumId, std::move(medium)).second);
        RegisterMediumInMaps(mediumId);

        YT_LOG_DEBUG("Medium created (MediumId: %v, MediumIndex: %v, MediumName: %v)",
            mediumId,
            mediumPtr->GetIndex(),
            mediumPtr->Name());
    }

    void DoRenameMedium(TMediumId mediumId, const TString& newName)
    {
        VerifyChunkThread();

        auto* medium = GetMedium(mediumId);
        UnregisterMediumFromMaps(mediumId);
        auto oldName = medium->Name();
        medium->Name() = newName;
        RegisterMediumInMaps(mediumId);

        YT_LOG_DEBUG("Medium renamed (MediumId: %v, MediumName: %v -> %v)",
            mediumId,
            oldName,
            newName);
    }

    void DoUpdateMediumConfig(TMediumId mediumId, TMediumConfigPtr newConfig)
    {
        VerifyChunkThread();

        auto* medium = GetMedium(mediumId);
        medium->Config() = std::move(newConfig);

        YT_LOG_DEBUG("Medium config updated (MediumId: %v, MediumName: %v)",
            mediumId,
            medium->Name());
    }

    void RegisterMediumInMaps(TMediumId mediumId)
    {
        VerifyChunkThread();

        auto* medium = GetMedium(mediumId);

        // TODO(gritukan): Use helpers from Max.
        YT_VERIFY(MediumIndexToMedium_.emplace(medium->GetIndex(), medium).second);
        YT_VERIFY(MediumNameToMedium_.emplace(medium->Name(), medium).second);
    }

    void UnregisterMediumFromMaps(TMediumId mediumId)
    {
        VerifyChunkThread();

        auto* medium = GetMedium(mediumId);

        // TODO(gritukan): Use helpers from Max.
        YT_VERIFY(MediumIndexToMedium_.erase(medium->GetIndex()) > 0);
        YT_VERIFY(MediumNameToMedium_.erase(medium->Name()) > 0);
    }
};

////////////////////////////////////////////////////////////////////////////////

IReplicatorStatePtr CreateReplicatorState(std::unique_ptr<IReplicatorStateProxy> proxy)
{
    return New<TReplicatorState>(std::move(proxy));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
