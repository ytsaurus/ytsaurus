#include "replicator_state.h"

#include "private.h"
#include "data_center.h"
#include "medium.h"
#include "rack.h"

#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/config.h>
#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/server/master/node_tracker_server/data_center.h>
#include <yt/yt/server/master/node_tracker_server/rack.h>

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

    void Load() override
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

        for (auto* primaryDataCenter : Proxy_->GetDataCenters()) {
            auto dataCenterId = primaryDataCenter->GetId();
            auto dualDataCenter = TDataCenter::FromPrimary(primaryDataCenter);
            YT_VERIFY(DataCenters_.emplace(dataCenterId, std::move(dualDataCenter)).second);
            RegisterDataCenterInMaps(dataCenterId);
        }

        for (auto* primaryRack : Proxy_->GetRacks()) {
            auto rackId = primaryRack->GetId();
            auto dualRack = TRack::FromPrimary(primaryRack);
            auto* dataCenter = primaryRack->GetDataCenter()
                ? FindDataCenter(primaryRack->GetDataCenter()->GetId())
                : nullptr;
            dualRack->SetDataCenter(dataCenter);
            YT_VERIFY(Racks_.emplace(rackId, std::move(dualRack)).second);
            RegisterRackInMaps(rackId);
        }

        CheckThreadAffinity_ = true;

        YT_LOG_INFO("Finished loading dual state");
    }

    void SyncWithUpstream() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        WaitFor(BIND([] { })
            .AsyncVia(DualMutationInvoker_)
            .Run())
            .ThrowOnError();
    }

    void UpdateDynamicConfig(const TDynamicClusterConfigPtr& newConfig) override
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

    void CreateMedium(NChunkServer::TMedium* medium) override
    {
        VerifyAutomatonThread();

        auto dualMedium = TMedium::FromPrimary(medium);
        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoCreateMedium,
                MakeWeak(this),
                Passed(std::move(dualMedium))));
    }

    void RenameMedium(TMediumId mediumId, const TString& newName) override
    {
        VerifyAutomatonThread();

        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoRenameMedium,
                MakeWeak(this),
                mediumId,
                newName));
    }

    void UpdateMediumConfig(TMediumId mediumId, const TMediumConfigPtr& newConfig) override
    {
        VerifyAutomatonThread();

        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoUpdateMediumConfig,
                MakeWeak(this),
                mediumId,
                CloneYsonSerializable(newConfig)));
    }

    void CreateDataCenter(NNodeTrackerServer::TDataCenter* dataCenter) override
    {
        VerifyAutomatonThread();

        auto dualDataCenter = TDataCenter::FromPrimary(dataCenter);
        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoCreateDataCenter,
                MakeWeak(this),
                Passed(std::move(dualDataCenter))));
    }

    void DestroyDataCenter(TDataCenterId dataCenterId) override
    {
        VerifyAutomatonThread();

        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoDestroyDataCenter,
                MakeWeak(this),
                dataCenterId));
    }

    void RenameDataCenter(TDataCenterId dataCenterId, const TString& name) override
    {
        VerifyAutomatonThread();

        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoRenameDataCenter,
                MakeWeak(this),
                dataCenterId,
                name));
    }

    void CreateRack(NNodeTrackerServer::TRack* rack) override
    {
        VerifyAutomatonThread();

        auto dataCenterId = rack->GetDataCenter()
            ? rack->GetDataCenter()->GetId()
            : TDataCenterId();

        auto dualRack = TRack::FromPrimary(rack);

        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoCreateRack,
                MakeWeak(this),
                Passed(std::move(dualRack)),
                dataCenterId));
    }

    void DestroyRack(TRackId rackId) override
    {
        VerifyAutomatonThread();

        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoDestroyRack,
                MakeWeak(this),
                rackId));
    }

    void RenameRack(TRackId rackId, const TString& newName) override
    {
        VerifyAutomatonThread();

        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoRenameRack,
                MakeWeak(this),
                rackId,
                newName));
    }

    void SetRackDataCenter(TRackId rackId, TDataCenterId newDataCenterId) override
    {
        VerifyAutomatonThread();

        DualMutationInvoker_->Invoke(
            BIND(
                &TReplicatorState::DoSetRackDataCenter,
                MakeWeak(this),
                rackId,
                newDataCenterId));
    }

    const TDynamicClusterConfigPtr& GetDynamicConfig() const override
    {
        VerifyChunkThread();

        return DynamicConfig_;
    }

    const THashMap<TMediumId, std::unique_ptr<TMedium>>& Media() const override
    {
        VerifyChunkThread();

        return Media_;
    }

    TMedium* FindMedium(TMediumId mediumId) const override
    {
        VerifyChunkThread();

        if (auto mediumIt = Media_.find(mediumId); mediumIt != Media_.end()) {
            return mediumIt->second.get();
        } else {
            return nullptr;
        }
    }

    TMedium* GetMedium(TMediumId mediumId) const override
    {
        VerifyChunkThread();

        return GetOrCrash(Media_, mediumId).get();
    }

    TMedium* FindMediumByIndex(TMediumIndex index) const override
    {
        VerifyChunkThread();

        if (auto mediumIt = MediumIndexToMedium_.find(index); mediumIt != MediumIndexToMedium_.end()) {
            return mediumIt->second;
        } else {
            return nullptr;
        }
    }

    TMedium* FindMediumByName(const TString& name) const override
    {
        VerifyChunkThread();

        if (auto mediumIt = MediumNameToMedium_.find(name); mediumIt != MediumNameToMedium_.end()) {
            return mediumIt->second;
        } else {
            return nullptr;
        }
    }

    const THashMap<TDataCenterId, std::unique_ptr<TDataCenter>>& DataCenters() const override
    {
        VerifyChunkThread();

        return DataCenters_;
    }

    TDataCenter* FindDataCenter(TDataCenterId dataCenterId) const override
    {
        VerifyChunkThread();

        if (auto dataCenterIt = DataCenters_.find(dataCenterId); dataCenterIt != DataCenters_.end()) {
            return dataCenterIt->second.get();
        } else {
            return nullptr;
        }
    }

    TDataCenter* GetDataCenter(TDataCenterId dataCenterId) const override
    {
        VerifyChunkThread();

        return GetOrCrash(DataCenters_, dataCenterId).get();
    }

    TDataCenter* FindDataCenterByName(const TString& name) const override
    {
        VerifyChunkThread();

        if (auto dataCenterIt = DataCenterNameToDataCenter_.find(name); dataCenterIt != DataCenterNameToDataCenter_.end()) {
            return dataCenterIt->second;
        } else {
            return nullptr;
        }
    }

    const THashMap<TRackId, std::unique_ptr<TRack>>& Racks() const override
    {
        VerifyChunkThread();

        return Racks_;
    }

    TRack* FindRack(TRackId rackId) const override
    {
        VerifyChunkThread();

        if (auto rackIt = Racks_.find(rackId); rackIt != Racks_.end()) {
            return rackIt->second.get();
        } else {
            return nullptr;
        }
    }

    TRack* GetRack(TRackId rackId) const override
    {
        VerifyChunkThread();

        return GetOrCrash(Racks_, rackId).get();
    }

    TRack* FindRackByIndex(TRackIndex rackIndex) const override
    {
        VerifyChunkThread();

        if (auto rackIt = RackIndexToRack_.find(rackIndex); rackIt != RackIndexToRack_.end()) {
            return rackIt->second;
        } else {
            return nullptr;
        }
    }

    TRack* FindRackByName(const TString& name) const override
    {
        VerifyChunkThread();

        if (auto rackIt = RackNameToRack_.find(name); rackIt != RackNameToRack_.end()) {
            return rackIt->second;
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

    THashMap<TDataCenterId, std::unique_ptr<TDataCenter>> DataCenters_;
    THashMap<TString, TDataCenter*> DataCenterNameToDataCenter_;

    THashMap<TRackId, std::unique_ptr<TRack>> Racks_;
    THashMap<TRackIndex, TRack*> RackIndexToRack_;
    THashMap<TString, TRack*> RackNameToRack_;

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

    void DoCreateDataCenter(std::unique_ptr<TDataCenter> dataCenter)
    {
        VerifyChunkThread();

        auto dataCenterId = dataCenter->GetId();
        auto dataCenterPtr = dataCenter.get();
        // TODO(gritukan): Use helpers from Max.
        YT_VERIFY(DataCenters_.emplace(dataCenterId, std::move(dataCenter)).second);
        RegisterDataCenterInMaps(dataCenterId);

        YT_LOG_DEBUG("Data center created (DataCenterId: %v, DataCenterName: %v)",
            dataCenterId,
            dataCenterPtr->Name());
    }

    void DoDestroyDataCenter(TDataCenterId dataCenterId)
    {
        VerifyChunkThread();

        auto* dataCenter = GetDataCenter(dataCenterId);
        auto dataCenterName = dataCenter->Name();
        UnregisterDataCenterFromMaps(dataCenterId);

        // Unbind racks with removed data center.
        for (const auto& [rackId, rack] : Racks_) {
            if (rack->GetDataCenter() == dataCenter) {
                rack->SetDataCenter(nullptr);
            }
        }

        // TODO(gritukan): Use helpers from Max.
        YT_VERIFY(DataCenters_.erase(dataCenterId) > 0);

        YT_LOG_DEBUG("Data center destroyed (DataCenterId: %v, DataCenterName: %v)",
            dataCenterId,
            dataCenterName);
    }

    void DoRenameDataCenter(TDataCenterId dataCenterId, const TString& newName)
    {
        VerifyChunkThread();

        auto* dataCenter = GetDataCenter(dataCenterId);
        auto oldName = dataCenter->Name();

        UnregisterDataCenterFromMaps(dataCenterId);
        dataCenter->Name() = newName;
        RegisterDataCenterInMaps(dataCenterId);

        YT_LOG_DEBUG("Data center renamed (DataCenterId: %v, DataCenterName: %v -> %v)",
            dataCenterId,
            oldName,
            newName);
    }

    void RegisterDataCenterInMaps(TDataCenterId dataCenterId)
    {
        VerifyChunkThread();

        auto* dataCenter = GetDataCenter(dataCenterId);

        // TODO(gritukan): Use helpers from Max.
        YT_VERIFY(DataCenterNameToDataCenter_.emplace(dataCenter->Name(), dataCenter).second);
    }

    void UnregisterDataCenterFromMaps(TDataCenterId dataCenterId)
    {
        VerifyChunkThread();

        auto* dataCenter = GetDataCenter(dataCenterId);

        // TODO(gritukan): Use helpers from Max.
        YT_VERIFY(DataCenterNameToDataCenter_.erase(dataCenter->Name()) > 0);
    }

    void DoCreateRack(std::unique_ptr<TRack> rack, TDataCenterId dataCenterId)
    {
        VerifyChunkThread();

        auto* dataCenter = dataCenterId ? GetDataCenter(dataCenterId) : nullptr;
        rack->SetDataCenter(dataCenter);

        auto rackId = rack->GetId();
        auto rackPtr = rack.get();
        YT_VERIFY(Racks_.emplace(rackId, std::move(rack)).second);
        RegisterRackInMaps(rackId);

        YT_LOG_DEBUG("Rack created (RackId: %v, RackIndex: %v, RackName: %v, DataCenterId: %v)",
            rackId,
            rackPtr->GetIndex(),
            rackPtr->Name(),
            dataCenterId);
    }

    void DoDestroyRack(TRackId rackId)
    {
        VerifyChunkThread();

        auto* rack = GetRack(rackId);
        auto rackName = rack->Name();
        auto rackIndex = rack->GetIndex();
        UnregisterRackFromMaps(rackId);
        YT_VERIFY(Racks_.erase(rackId) > 0);

        YT_LOG_DEBUG("Rack destroyed (RackId: %v, RackIndex: %v, RackName: %v)",
            rackId,
            rackIndex,
            rackName);
    }

    void DoRenameRack(TRackId rackId, const TString& newName)
    {
        VerifyChunkThread();

        auto* rack = GetRack(rackId);
        auto oldName = rack->Name();
        UnregisterRackFromMaps(rackId);
        rack->Name() = newName;
        RegisterRackInMaps(rackId);

        YT_LOG_DEBUG("Rack renamed (RackId: %v, RackIndex: %v, RackName: %v -> %v)",
            rackId,
            rack->GetIndex(),
            oldName,
            newName);
    }

    void DoSetRackDataCenter(TRackId rackId, TDataCenterId newDataCenterId)
    {
        VerifyChunkThread();

        auto* rack = GetRack(rackId);
        auto oldDataCenterId = rack->GetDataCenter()
            ? rack->GetDataCenter()->GetId()
            : TDataCenterId();
        auto* newDataCenter = newDataCenterId ? GetDataCenter(newDataCenterId) : nullptr;
        rack->SetDataCenter(newDataCenter);

        YT_LOG_DEBUG("Rack data center updated (RackId: %v, RackIndex: %v, DataCenterId: %v -> %v)",
            rackId,
            rack->GetIndex(),
            oldDataCenterId,
            newDataCenterId);
    }

    void RegisterRackInMaps(TRackId rackId)
    {
        VerifyChunkThread();

        auto* rack = GetRack(rackId);

        // TODO(gritukan): Use helpers from Max.
        YT_VERIFY(RackIndexToRack_.emplace(rack->GetIndex(), rack).second);
        YT_VERIFY(RackNameToRack_.emplace(rack->Name(), rack).second);
    }

    void UnregisterRackFromMaps(TRackId rackId)
    {
        VerifyChunkThread();

        auto* rack = GetRack(rackId);

        // TODO(gritukan): Use helpers from Max.
        YT_VERIFY(RackIndexToRack_.erase(rack->GetIndex()) > 0);
        YT_VERIFY(RackNameToRack_.erase(rack->Name()) > 0);
    }
};

////////////////////////////////////////////////////////////////////////////////

IReplicatorStatePtr CreateReplicatorState(std::unique_ptr<IReplicatorStateProxy> proxy)
{
    return New<TReplicatorState>(std::move(proxy));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
