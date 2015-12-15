#include "multicell_manager.h"
#include "config.h"
#include "bootstrap.h"
#include "private.h"
#include "automaton.h"
#include "serialize.h"
#include "hydra_facade.h"
#include "world_initializer.h"
#include "helpers.h"

#include <yt/core/misc/collection_helpers.h>

#include <yt/core/ytree/ypath_client.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/server/hive/hive_manager.h>
#include <yt/server/hive/mailbox.h>

#include <yt/server/hydra/mutation.h>

#include <yt/server/security_server/security_manager.h>
#include <yt/server/security_server/user.h>

#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/server/cell_master/multicell_manager.pb.h>

namespace NYT {
namespace NCellMaster {

using namespace NElection;
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NHive;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellMasterLogger;
static const auto RegisterRetryPeriod = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPrimaryRegisterState,
    (None)
    (Registering)
    (Registered)
);

class TMulticellManager::TImpl
    : public TMasterAutomatonPart
{
public:
    TImpl(
        TMulticellManagerConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap)
        , Config_(config)
    {
        YCHECK(Config_);

        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraRegisterSecondaryMasterAtPrimary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraOnSecondaryMasterRegisteredAtPrimary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraRegisterSecondaryMasterAtSecondary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraStartSecondaryMasterRegistration, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraSetCellStatistics, Unretained(this)));

        RegisterLoader(
            "MulticellManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Values,
            "MulticellManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));
    }


    void PostToMaster(
        IClientRequestPtr request,
        TCellTag cellTag,
        bool reliable)
    {
        PostToMaster(request->Serialize(), cellTag, reliable);
    }

    void PostToMaster(
        const TObjectId& objectId,
        IServiceContextPtr context,
        TCellTag cellTag,
        bool reliable)
    {
        PostToMaster(ReplicateTarget(objectId, context), cellTag, reliable);
    }

    void PostToMaster(
        const ::google::protobuf::MessageLite& requestMessage,
        TCellTag cellTag,
        bool reliable)
    {
        DoPostMessage(requestMessage, cellTag, reliable);
    }

    void PostToMaster(
        TSharedRefArray requestMessage,
        TCellTag cellTag,
        bool reliable)
    {
        NObjectServer::NProto::TReqExecute wrappedRequest;
        WrapRequest(&wrappedRequest, requestMessage);
        DoPostMessage(wrappedRequest, cellTag, reliable);
    }


    void PostToSecondaryMasters(
        IClientRequestPtr request,
        bool reliable)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());
        if (Bootstrap_->IsMulticell()) {
            PostToSecondaryMasters(request->Serialize(), reliable);
        }
    }

    void PostToSecondaryMasters(
        const TObjectId& objectId,
        IServiceContextPtr context,
        bool reliable)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());
        if (Bootstrap_->IsMulticell()) {
            PostToSecondaryMasters(ReplicateTarget(objectId, context), reliable);
        }
    }

    void PostToSecondaryMasters(
        const ::google::protobuf::MessageLite& requestMessage,
        bool reliable)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());
        if (Bootstrap_->IsMulticell()) {
            DoPostMessage(requestMessage, AllSecondaryMastersCellTag, reliable);
        }
    }

    void PostToSecondaryMasters(
        TSharedRefArray requestMessage,
        bool reliable)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());
        if (Bootstrap_->IsMulticell()) {
            NObjectServer::NProto::TReqExecute hydraReq;
            WrapRequest(&hydraReq, requestMessage);
            DoPostMessage(hydraReq, AllSecondaryMastersCellTag, reliable);
        }
    }


    bool IsRegisteredSecondaryMaster(TCellTag cellTag)
    {
        return FindMasterEntry(cellTag) != nullptr;
    }

    std::vector<NObjectClient::TCellTag> GetRegisteredMasterCellTags()
    {
        return GetKeys(RegisteredMasterMap_);
    }

    int GetRegisteredMasterCellIndex(TCellTag cellTag)
    {
        return GetMasterEntry(cellTag)->Index;
    }


    TCellTag PickSecondaryMasterCell()
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());

        if (RegisteredMasterMap_.empty()) {
            return InvalidCellTag;
        }

        // Compute the average number of chunks.
        int chunkCountSum = 0;
        for (const auto& pair : RegisteredMasterMap_) {
            const auto& entry = pair.second;
            chunkCountSum += entry.Statistics.chunk_count();
        }

        int avgChunkCount = chunkCountSum / RegisteredMasterMap_.size();

        // Construct PickCellList_ by putting each secondary cell
        // * once if the number of chunks there is at least the average
        // * twice otherwise
        PickCellList_.reserve(RegisteredMasterMap_.size() * 2);
        PickCellList_.clear();
        for (const auto& pair : RegisteredMasterMap_) {
            auto cellTag = pair.first;
            const auto& entry = pair.second;
            PickCellList_.push_back(cellTag);
            if (entry.Statistics.chunk_count() < avgChunkCount) {
                PickCellList_.push_back(cellTag);
            }
        }

        // Sample PickCellList_ uniformly.
        auto* mutationContext = GetCurrentMutationContext();
        return PickCellList_[mutationContext->RandomGenerator().Generate<size_t>() % PickCellList_.size()];
    }

    NProto::TCellStatistics ComputeClusterStatistics()
    {
        auto result = GetLocalCellStatistics();
        for (const auto& pair : RegisteredMasterMap_) {
            const auto& entry = pair.second;
            result += entry.Statistics;
        }
        return result;
    }


    IChannelPtr GetMasterChannelOrThrow(TCellTag cellTag, EPeerKind peerKind)
    {
        auto channel = FindMasterChannel(cellTag, peerKind);
        if (!channel) {
            THROW_ERROR_EXCEPTION("Unknown cell tag %v",
                cellTag);
        }
        return channel;
    }

    IChannelPtr FindMasterChannel(TCellTag cellTag, EPeerKind peerKind)
    {
        auto key = std::make_tuple(cellTag, peerKind);
        auto it = MasterChannelCache_.find(key);
        if (it != MasterChannelCache_.end()) {
            return it->second;
        }

        auto cellDirectory = Bootstrap_->GetCellDirectory();
        auto cellId = Bootstrap_->GetCellId(cellTag);
        auto channel = cellDirectory->FindChannel(cellId, peerKind);
        if (!channel) {
            return nullptr;
        }

        auto wrappedChannel = CreateDefaultTimeoutChannel(channel, Config_->MasterRpcTimeout);
        YCHECK(MasterChannelCache_.insert(std::make_pair(key, wrappedChannel)).second);

        return wrappedChannel;
    }


    DEFINE_SIGNAL(void(TCellTag), ValidateSecondaryMasterRegistration);
    DEFINE_SIGNAL(void(TCellTag), SecondaryMasterRegistered);

private:
    const TMulticellManagerConfigPtr Config_;

    struct TMasterEntry
    {
        int Index = -1;
        NProto::TCellStatistics Statistics;

        void Persist(NCellMaster::TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, Statistics);
        }
    };

    // NB: Must ensure stable order.
    std::map<TCellTag, TMasterEntry> RegisteredMasterMap_;
    EPrimaryRegisterState RegisterState_;

    TMailbox* PrimaryMasterMailbox_ = nullptr;

    TPeriodicExecutorPtr RegisterAtPrimaryMasterExecutor_;
    TPeriodicExecutorPtr CellStatiticsGossipExecutor_;

    //! A temporary buffer used in PickSecondaryMasterCell.
    std::vector<TCellTag> PickCellList_;

    //! Caches master channels returned by FindMasterChannel and GetMasterChannelOrThrow.
    std::map<std::tuple<TCellTag, EPeerKind>, IChannelPtr> MasterChannelCache_;


    virtual void OnAfterSnapshotLoaded()
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        int index = 0;
        for (auto& pair : RegisteredMasterMap_) {
            auto cellTag = pair.first;
            auto& entry = pair.second;
            entry.Index = index++;
            ValidateCellTag(cellTag);
        }

        if (RegisterState_ == EPrimaryRegisterState::Registered) {
            YCHECK(!PrimaryMasterMailbox_);
            auto hiveManager = Bootstrap_->GetHiveManager();
            PrimaryMasterMailbox_ = hiveManager->GetMailbox(Bootstrap_->GetPrimaryCellId());
        }
    }

    virtual void Clear() override
    {
        TMasterAutomatonPart::Clear();

        RegisteredMasterMap_.clear();
        if (Bootstrap_->IsSecondaryMaster()) {
            RegisterMasterEntry(Bootstrap_->GetPrimaryCellTag());
        }

        RegisterState_ = EPrimaryRegisterState::None;
        PrimaryMasterMailbox_ = nullptr;
    }


    void LoadValues(TLoadContext& context)
    {
        using NYT::Load;

        Load(context, RegisteredMasterMap_);
        // COMPAT(babenko)
        if (context.GetVersion() >= 207) {
            Load(context, RegisterState_);
        } else {
            RegisterState_ = Load<bool>(context) ? EPrimaryRegisterState::Registered : EPrimaryRegisterState::None;
        }
    }

    void SaveValues(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, RegisteredMasterMap_);
        Save(context, RegisterState_);
    }


    virtual void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        if (Bootstrap_->IsSecondaryMaster()) {
            RegisterAtPrimaryMasterExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
                BIND(&TImpl::OnStartSecondaryMasterRegistration, MakeWeak(this)),
                RegisterRetryPeriod);
            RegisterAtPrimaryMasterExecutor_->Start();

            CellStatiticsGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
                BIND(&TImpl::OnCellStatisticsGossip, MakeWeak(this)),
                Config_->CellStatisticsGossipPeriod);
            CellStatiticsGossipExecutor_->Start();
        }
    }

    virtual void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        if (RegisterAtPrimaryMasterExecutor_) {
            RegisterAtPrimaryMasterExecutor_->Stop();
            RegisterAtPrimaryMasterExecutor_.Reset();
        }

        if (CellStatiticsGossipExecutor_) {
            CellStatiticsGossipExecutor_->Stop();
            CellStatiticsGossipExecutor_.Reset();
        }

        ClearMasterChannelCache();
    }

    virtual void OnStopFollowing() override
    {
        TMasterAutomatonPart::OnStopFollowing();

        ClearMasterChannelCache();
    }


    void ClearMasterChannelCache()
    {
        MasterChannelCache_.clear();
    }


    void HydraRegisterSecondaryMasterAtPrimary(const NProto::TReqRegisterSecondaryMasterAtPrimary& request)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());

        auto cellTag = request.cell_tag();
        try {
            ValidateSecondaryCellTag(cellTag);

            if (FindMasterEntry(cellTag))  {
                THROW_ERROR_EXCEPTION("Attempted to re-register secondary master %v", cellTag);
            }

            ValidateSecondaryMasterRegistration_.Fire(cellTag);

            LOG_INFO_UNLESS(IsRecovery(), "Secondary master registered (CellTag: %v)", cellTag);

            RegisterMasterEntry(cellTag);

            SecondaryMasterRegistered_.Fire(cellTag);

            for (const auto& pair : RegisteredMasterMap_) {
                if (pair.first == cellTag) {
                    continue;
                }
                
                {
                    // Inform others about the new secondary.
                    NProto::TReqRegisterSecondaryMasterAtSecondary request;
                    request.set_cell_tag(cellTag);
                    PostToMaster(request, pair.first, true);
                }
                {
                    // Inform the new secondary about others.
                    NProto::TReqRegisterSecondaryMasterAtSecondary request;
                    request.set_cell_tag(pair.first);
                    PostToMaster(request, cellTag, true);
                }
            }

            NProto::TRspRegisterSecondaryMasterAtPrimary response;
            PostToMaster(response, cellTag, true);
        } catch (const std::exception& ex) {
            NProto::TRspRegisterSecondaryMasterAtPrimary response;
            ToProto(response.mutable_error(), TError(ex).Sanitize());
            PostToMaster(response, cellTag, true);
        }
    }

    void HydraOnSecondaryMasterRegisteredAtPrimary(const NProto::TRspRegisterSecondaryMasterAtPrimary& response)
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        if (response.has_error()) {
            auto error = FromProto<TError>(response.error());
            LOG_ERROR_UNLESS(IsRecovery(), error, "Error registering at primary master");
            RegisterState_ = EPrimaryRegisterState::None;
            return;
        }

        RegisterState_ = EPrimaryRegisterState::Registered;

        LOG_INFO_UNLESS(IsRecovery(), "Successfully registered at primary master");
    }

    void HydraRegisterSecondaryMasterAtSecondary(const NProto::TReqRegisterSecondaryMasterAtSecondary& request)
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        auto cellTag = request.cell_tag();
        try {
            ValidateSecondaryCellTag(cellTag);

            if (FindMasterEntry(cellTag))  {
                THROW_ERROR_EXCEPTION("Attempted to re-register secondary master %v", cellTag);
            }

            LOG_INFO_UNLESS(IsRecovery(), "Secondary master registered (CellTag: %v)", cellTag);

            RegisterMasterEntry(cellTag);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error registering secondary master %v", cellTag);
        }
    }

    void HydraStartSecondaryMasterRegistration(const NProto::TReqStartSecondaryMasterRegistration& /*request*/)
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        if (RegisterState_ != EPrimaryRegisterState::None) {
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Registering at primary master");

        RegisterState_ = EPrimaryRegisterState::Registering;

        if (!PrimaryMasterMailbox_) {
            auto hiveManager = Bootstrap_->GetHiveManager();
            PrimaryMasterMailbox_ = hiveManager->GetOrCreateMailbox(Bootstrap_->GetPrimaryCellId());
        }

        NProto::TReqRegisterSecondaryMasterAtPrimary request;
        request.set_cell_tag(Bootstrap_->GetCellTag());
        PostToMaster(request, PrimaryMasterCellTag, true);
    }

    void HydraSetCellStatistics(const NProto::TReqSetCellStatistics& request)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());

        auto cellTag = request.cell_tag();
        LOG_INFO_UNLESS(IsRecovery(), "Received cell statistics gossip message (CellTag: %v)",
            cellTag);

        auto* entry = GetMasterEntry(cellTag);
        entry->Statistics = request.statistics();
    }


    void ValidateSecondaryCellTag(TCellTag cellTag)
    {
        auto config = Bootstrap_->GetConfig();
        for (auto cellConfig : config->SecondaryMasters) {
            if (CellTagFromId(cellConfig->CellId) == cellTag)
                return;
        }
        THROW_ERROR_EXCEPTION("Unknown secondary master cell tag %v", cellTag);
    }

    void ValidateCellTag(TCellTag cellTag)
    {
        auto config = Bootstrap_->GetConfig();
        if (CellTagFromId(config->PrimaryMaster->CellId) == cellTag)
            return;
        for (auto cellConfig : config->SecondaryMasters) {
            if (CellTagFromId(cellConfig->CellId) == cellTag)
                return;
        }
        THROW_ERROR_EXCEPTION("Unknown master cell tag %v", cellTag);
    }


    void RegisterMasterEntry(TCellTag cellTag)
    {
        int index = RegisteredMasterMap_.empty() ? 0 : RegisteredMasterMap_.rbegin()->second.Index + 1;
        auto pair = RegisteredMasterMap_.insert(std::make_pair(cellTag, TMasterEntry()));
        YCHECK(pair.second);
        auto& entry = pair.first->second;
        entry.Index = index;
    }

    TMasterEntry* FindMasterEntry(TCellTag cellTag)
    {
        auto it = RegisteredMasterMap_.find(cellTag);
        return it == RegisteredMasterMap_.end() ? nullptr : &it->second;
    }

    TMasterEntry* GetMasterEntry(TCellTag cellTag)
    {
        auto it = RegisteredMasterMap_.find(cellTag);
        YCHECK(it != RegisteredMasterMap_.end());
        return &it->second;
    }


    void OnStartSecondaryMasterRegistration()
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        auto worldInitializer = Bootstrap_->GetWorldInitializer();
        if (!worldInitializer->CheckInitialized()) {
            return;
        }

        if (RegisterState_ != EPrimaryRegisterState::None) {
            return;
        }

        NProto::TReqStartSecondaryMasterRegistration request;
        CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
            ->CommitAndLog(Logger);
    }

    void OnCellStatisticsGossip()
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        if (RegisterState_ != EPrimaryRegisterState::Registered) {
            return;
        }

        LOG_INFO("Sending cell statistics gossip message");

        NProto::TReqSetCellStatistics request;
        request.set_cell_tag(Bootstrap_->GetCellTag());
        *request.mutable_statistics() = GetLocalCellStatistics();
        PostToMaster(request, PrimaryMasterCellTag, false);
    }

    NProto::TCellStatistics GetLocalCellStatistics()
    {
        NProto::TCellStatistics result;
        auto chunkManager = Bootstrap_->GetChunkManager();
        result.set_chunk_count(chunkManager->Chunks().GetSize());
        result.set_lost_vital_chunk_count(chunkManager->LostVitalChunks().size());
        return result;
    }


    void WrapRequest(
        NObjectServer::NProto::TReqExecute* hydraRequest,
        const TSharedRefArray& requestMessage)
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        ToProto(hydraRequest->mutable_user_id(), user->GetId());

        for (const auto& part : requestMessage) {
            hydraRequest->add_request_parts(part.Begin(), part.Size());
        }
    }

    TSharedRefArray ReplicateTarget(
        const TObjectId& objectId,
        IServiceContextPtr context)
    {
        auto requestMessage = context->GetRequestMessage();
        NRpc::NProto::TRequestHeader requestHeader;
        ParseRequestHeader(requestMessage, &requestHeader);

        auto updatedYPath = FromObjectId(objectId) + GetRequestYPath(context);
        SetRequestYPath(&requestHeader, updatedYPath);
        return SetRequestHeader(requestMessage, requestHeader);
    }

    void DoPostMessage(
        const ::google::protobuf::MessageLite& requestMessage,
        TCellTag cellTag,
        bool reliable)
    {
        auto hiveManager = Bootstrap_->GetHiveManager();
        if (cellTag >= MinimumValidCellTag && cellTag <= MaximumValidCellTag) {
            auto cellId = Bootstrap_->GetCellId(cellTag);
            auto* mailbox = hiveManager->GetOrCreateMailbox(cellId);
            hiveManager->PostMessage(mailbox, requestMessage, reliable);
        } else if (cellTag == PrimaryMasterCellTag) {
            if (!reliable && !PrimaryMasterMailbox_)
                return;

            // Failure here indicates an attempt to send a reliable message to the primary master
            // before registering.
            YCHECK(PrimaryMasterMailbox_);

            hiveManager->PostMessage(PrimaryMasterMailbox_, requestMessage, reliable);
        } else if (cellTag == AllSecondaryMastersCellTag) {
            YCHECK(Bootstrap_->IsPrimaryMaster());
            for (const auto& pair : RegisteredMasterMap_) {
                auto currentCellId = Bootstrap_->GetCellId(pair.first);
                auto* currentMailbox = hiveManager->GetOrCreateMailbox(currentCellId);
                hiveManager->PostMessage(currentMailbox, requestMessage, reliable);
            }
        } else {
            YUNREACHABLE();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TMulticellManager::TMulticellManager(
    TMulticellManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TMulticellManager::~TMulticellManager()
{ }

void TMulticellManager::PostToMaster(
    IClientRequestPtr request,
    TCellTag cellTag,
    bool reliable)
{
    Impl_->PostToMaster(std::move(request), cellTag, reliable);
}

void TMulticellManager::PostToMaster(
    const TObjectId& objectId,
    IServiceContextPtr context,
    TCellTag cellTag,
    bool reliable)
{
    Impl_->PostToMaster(objectId, std::move(context), cellTag, reliable);
}

void TMulticellManager::PostToMaster(
    const ::google::protobuf::MessageLite& requestMessage,
    TCellTag cellTag,
    bool reliable)
{
    Impl_->PostToMaster(requestMessage, cellTag, reliable);
}

void TMulticellManager::PostToMaster(
    TSharedRefArray requestMessage,
    TCellTag cellTag,
    bool reliable)
{
    Impl_->PostToMaster(std::move(requestMessage), cellTag, reliable);
}

void TMulticellManager::PostToSecondaryMasters(
    IClientRequestPtr request,
    bool reliable)
{
    Impl_->PostToSecondaryMasters(std::move(request), reliable);
}

void TMulticellManager::PostToSecondaryMasters(
    const TObjectId& objectId,
    IServiceContextPtr context,
    bool reliable)
{
    Impl_->PostToSecondaryMasters(objectId, std::move(context), reliable);
}

void TMulticellManager::PostToSecondaryMasters(
    const ::google::protobuf::MessageLite& requestMessage,
    bool reliable)
{
    Impl_->PostToSecondaryMasters(requestMessage, reliable);
}

void TMulticellManager::PostToSecondaryMasters(
    TSharedRefArray requestMessage,
    bool reliable)
{
    Impl_->PostToSecondaryMasters(std::move(requestMessage), reliable);
}

bool TMulticellManager::IsRegisteredMasterCell(TCellTag cellTag)
{
    return Impl_->IsRegisteredSecondaryMaster(cellTag);
}

std::vector<NObjectClient::TCellTag> TMulticellManager::GetRegisteredMasterCellTags()
{
    return Impl_->GetRegisteredMasterCellTags();
}

int TMulticellManager::GetRegisteredMasterCellIndex(TCellTag cellTag)
{
    return Impl_->GetRegisteredMasterCellIndex(cellTag);
}

TCellTag TMulticellManager::PickSecondaryMasterCell()
{
    return Impl_->PickSecondaryMasterCell();
}

NProto::TCellStatistics TMulticellManager::ComputeClusterStatistics()
{
    return Impl_->ComputeClusterStatistics();
}

IChannelPtr TMulticellManager::GetMasterChannelOrThrow(TCellTag cellTag, EPeerKind peerKind)
{
    return Impl_->GetMasterChannelOrThrow(cellTag, peerKind);
}

IChannelPtr TMulticellManager::FindMasterChannel(TCellTag cellTag, EPeerKind peerKind)
{
    return Impl_->FindMasterChannel(cellTag, peerKind);
}

DELEGATE_SIGNAL(TMulticellManager, void(TCellTag), ValidateSecondaryMasterRegistration, *Impl_);
DELEGATE_SIGNAL(TMulticellManager, void(TCellTag), SecondaryMasterRegistered, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
