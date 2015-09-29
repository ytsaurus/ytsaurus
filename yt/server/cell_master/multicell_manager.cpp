#include "stdafx.h"
#include "multicell_manager.h"
#include "config.h"
#include "bootstrap.h"
#include "private.h"
#include "automaton.h"
#include "serialize.h"
#include "hydra_facade.h"

#include <core/misc/collection_helpers.h>

#include <core/ytree/ypath_client.h>

#include <core/concurrency/periodic_executor.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/hive/cell_directory.h>

#include <server/hive/hive_manager.h>
#include <server/hive/mailbox.h>

#include <server/hydra/mutation.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/user.h>

#include <server/chunk_server/chunk_manager.h>

#include <server/cell_master/multicell_manager.pb.h>

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

        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraRegisterSecondaryMaster, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraRegisterAtPrimaryMaster, Unretained(this)));
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

    TCellTag PickCellForNode()
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
    bool RegisteredAtPrimaryMaster_ = false;

    TMailbox* PrimaryMasterMailbox_ = nullptr;

    TPeriodicExecutorPtr RegisterAtPrimaryMasterExecutor_;
    TPeriodicExecutorPtr CellStatiticsGossipExecutor_;

    //! A temporary buffer used in PickCellForNode.
    std::vector<TCellTag> PickCellList_;


    virtual void OnAfterSnapshotLoaded()
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        int index = 0;
        for (auto& pair : RegisteredMasterMap_) {
            auto cellTag = pair.first;
            auto& entry = pair.second;
            entry.Index = index++;
            ValidateSecondaryCellTag(cellTag);
        }
    }

    virtual void Clear() override
    {
        TMasterAutomatonPart::Clear();

        RegisteredMasterMap_.clear();
        if (Bootstrap_->IsSecondaryMaster()) {
            RegisterMasterEntry(Bootstrap_->GetPrimaryCellTag());
        }

        RegisteredAtPrimaryMaster_ = false;
        PrimaryMasterMailbox_ = nullptr;
    }


    void LoadValues(TLoadContext& context)
    {
        using NYT::Load;

        Load(context, RegisteredMasterMap_);
        Load(context, RegisteredAtPrimaryMaster_);
    }

    void SaveValues(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, RegisteredMasterMap_);
        Save(context, RegisteredAtPrimaryMaster_);
    }


    virtual void OnLeaderActive()
    {
        TMasterAutomatonPart::OnLeaderActive();

        if (Bootstrap_->IsSecondaryMaster()) {
            RegisterAtPrimaryMasterExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
                BIND(&TImpl::OnRegisterAtPrimaryMaster, MakeWeak(this)),
                RegisterRetryPeriod);
            RegisterAtPrimaryMasterExecutor_->Start();

            CellStatiticsGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
                BIND(&TImpl::OnCellStatisticsGossip, MakeWeak(this)),
                Config_->CellStatisticsGossipPeriod);
            CellStatiticsGossipExecutor_->Start();
        }
    }

    virtual void OnStopLeading()
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
    }


    void HydraRegisterSecondaryMaster(const NProto::TReqRegisterSecondaryMaster& request)
    {
        auto cellTag = request.cell_tag();
        ValidateSecondaryCellTag(cellTag);

        if (FindMasterEntry(cellTag))  {
            LOG_ERROR_UNLESS(IsRecovery(), "Attempted to re-register secondary master (CellTag: %v)", cellTag);
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Secondary master registered (CellTag: %v)", cellTag);

        RegisterMasterEntry(cellTag);

        SecondaryMasterRegistered_.Fire(cellTag);

        if (Bootstrap_->IsPrimaryMaster()) {
            for (const auto& pair : RegisteredMasterMap_) {
                if (pair.first != cellTag) {
                    PostToMaster(request, pair.first, true);
                }
            }
        }
    }

    void HydraRegisterAtPrimaryMaster(const NProto::TReqRegisterAtPrimaryMaster& /*request*/)
    {
        if (RegisteredAtPrimaryMaster_)
            return;

        LOG_INFO_UNLESS(IsRecovery(), "Registering at primary master");

        RegisteredAtPrimaryMaster_ = true;
        InitializePrimaryMailbox();

        NProto::TReqRegisterSecondaryMaster request;
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
        for (auto cellConfig : Bootstrap_->GetConfig()->SecondaryMasters) {
            if (CellTagFromId(cellConfig->CellId) == cellTag)
                return;
        }
        LOG_FATAL("Unknown secondary master cell tag %v", cellTag);
    }


    void InitializePrimaryMailbox()
    {
        if (RegisteredAtPrimaryMaster_ && !PrimaryMasterMailbox_) {
            auto hiveManager = Bootstrap_->GetHiveManager();
            auto multicellManager = Bootstrap_->GetMulticellManager();
            PrimaryMasterMailbox_ = hiveManager->GetOrCreateMailbox(Bootstrap_->GetPrimaryCellId());
        }
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


    void OnRegisterAtPrimaryMaster()
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        if (RegisteredAtPrimaryMaster_)
            return;

        NProto::TReqRegisterAtPrimaryMaster request;
        CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
            ->Commit()
            .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TMutationResponse>& error) {
                if (!error.IsOK()) {
                    LOG_WARNING(error, "Error committing registration mutation");
                }
            }));
    }

    void OnCellStatisticsGossip()
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        if (!RegisteredAtPrimaryMaster_)
            return;

        LOG_INFO("Sending cell statistics gossip message");

        NProto::TReqSetCellStatistics request;
        request.set_cell_tag(Bootstrap_->GetCellTag());

        auto* statistics = request.mutable_statistics();

        auto chunkManager = Bootstrap_->GetChunkManager();
        statistics->set_chunk_count(chunkManager->Chunks().GetSize());

        PostToMaster(request, PrimaryMasterCellTag, false);
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
            auto cellId = Bootstrap_->GetSecondaryCellId(cellTag);
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
                auto currentCellId = Bootstrap_->GetSecondaryCellId(pair.first);
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
    return Impl_->PickCellForNode();
}

DELEGATE_SIGNAL(TMulticellManager, void(TCellTag), SecondaryMasterRegistered, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
