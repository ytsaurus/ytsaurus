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


    void PostToPrimaryMaster(
        const ::google::protobuf::MessageLite& requestMessage,
        bool reliable = true)
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());
        DoPostMessage(requestMessage, PrimaryMasterCellTag, reliable);
    }

    void PostToPrimaryMaster(
        IClientRequestPtr request,
        bool reliable = true)
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());
        PostToPrimaryMaster(request->Serialize(), reliable);
    }

    void PostToPrimaryMaster(
        TSharedRefArray requestMessage,
        bool reliable = true)
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());
        NObjectServer::NProto::TReqExecute wrappedRequest;
        WrapRequest(&wrappedRequest, requestMessage);
        DoPostMessage(wrappedRequest, PrimaryMasterCellTag, reliable);
    }


    void PostToSecondaryMaster(
        IClientRequestPtr request,
        TCellTag cellTag,
        bool reliable)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());
        PostToSecondaryMaster(request->Serialize(), cellTag, reliable);
    }

    void PostToSecondaryMaster(
        const TObjectId& objectId,
        IServiceContextPtr context,
        TCellTag cellTag,
        bool reliable)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());
        PostToSecondaryMaster(ReplicateTarget(objectId, context), cellTag, reliable);
    }

    void PostToSecondaryMaster(
        const ::google::protobuf::MessageLite& requestMessage,
        TCellTag cellTag,
        bool reliable)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());
        DoPostMessage(requestMessage, cellTag, reliable);
    }

    void PostToSecondaryMaster(
        TSharedRefArray requestMessage,
        TCellTag cellTag,
        bool reliable)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());
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
        return FindSecondaryCellEntry(cellTag) != nullptr;
    }

    std::vector<NObjectClient::TCellTag> GetRegisteredSecondaryMasterCellTags()
    {
        return GetKeys(RegisteredSecondaryCellMap_);
    }

    TCellTag GetLeastLoadedSecondaryMaster()
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());

        auto bestCellTag = InvalidCellTag;
        int minChunkCount = std::numeric_limits<int>::max();
        for (const auto& pair : RegisteredSecondaryCellMap_) {
            const auto& entry = pair.second;
            if (entry.Statistics.chunk_count() < minChunkCount) {
                minChunkCount = entry.Statistics.chunk_count();
                bestCellTag = pair.first;
            }
        }

        return bestCellTag;
    }


    DEFINE_SIGNAL(void(TCellTag), SecondaryMasterRegistered);

private:
    const TMulticellManagerConfigPtr Config_;

    struct TSecondaryCellEntry
    {
        TMailbox* Mailbox = nullptr;
        NProto::TCellStatistics Statistics;

        void Persist(NCellMaster::TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, Statistics);
        }
    };

    // NB: Must ensure stable order.
    std::map<TCellTag, TSecondaryCellEntry> RegisteredSecondaryCellMap_;
    bool RegisteredAtPrimaryMaster_ = false;

    TMailbox* PrimaryMasterMailbox_ = nullptr;

    TPeriodicExecutorPtr RegisterAtPrimaryMasterExecutor_;
    TPeriodicExecutorPtr CellStatiticsGossipExecutor_;


    virtual void OnAfterSnapshotLoaded()
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        for (auto& pair : RegisteredSecondaryCellMap_) {
            auto cellTag = pair.first;
            auto* entry = &pair.second;
            ValidateSecondaryCellTag(cellTag);
            InitializeSecondaryMailbox(cellTag, entry);
        }
    }

    virtual void Clear() override
    {
        TMasterAutomatonPart::Clear();

        RegisteredSecondaryCellMap_.clear();
        RegisteredAtPrimaryMaster_ = false;
        PrimaryMasterMailbox_ = nullptr;
    }


    void LoadValues(TLoadContext& context)
    {
        using NYT::Load;

        Load(context, RegisteredSecondaryCellMap_);
        Load(context, RegisteredAtPrimaryMaster_);
    }

    void SaveValues(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, RegisteredSecondaryCellMap_);
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

        if (FindSecondaryCellEntry(cellTag))  {
            LOG_ERROR_UNLESS(IsRecovery(), "Attempted to re-register secondary master (CellTag: %v)", cellTag);
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Secondary master registered (CellTag: %v)", cellTag);

        auto* entry = RegisterSecondaryCellEntry(cellTag);
        InitializeSecondaryMailbox(cellTag, entry);

        SecondaryMasterRegistered_.Fire(cellTag);
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
        PostToPrimaryMaster(request);
    }

    void HydraSetCellStatistics(const NProto::TReqSetCellStatistics& request)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());

        auto cellTag = request.cell_tag();
        LOG_INFO_UNLESS(IsRecovery(), "Received cell statistics gossip message (CellTag: %v)",
            cellTag);

        auto* entry = GetSecondaryCellEntry(cellTag);
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

    void InitializeSecondaryMailbox(TCellTag cellTag, TSecondaryCellEntry* entry)
    {
        auto hiveManager = Bootstrap_->GetHiveManager();
        auto multicellManager = Bootstrap_->GetMulticellManager();
        auto cellId = Bootstrap_->GetSecondaryCellId(cellTag);
        YCHECK(!entry->Mailbox);
        entry->Mailbox = hiveManager->GetOrCreateMailbox(cellId);
    }


    TSecondaryCellEntry* RegisterSecondaryCellEntry(TCellTag cellTag)
    {
        auto pair = RegisteredSecondaryCellMap_.insert(std::make_pair(cellTag, TSecondaryCellEntry()));
        YCHECK(pair.second);
        return &pair.first->second;
    }

    TSecondaryCellEntry* FindSecondaryCellEntry(TCellTag cellTag)
    {
        auto it = RegisteredSecondaryCellMap_.find(cellTag);
        return it == RegisteredSecondaryCellMap_.end() ? nullptr : &it->second;
    }

    TSecondaryCellEntry* GetSecondaryCellEntry(TCellTag cellTag)
    {
        auto it = RegisteredSecondaryCellMap_.find(cellTag);
        YCHECK(it != RegisteredSecondaryCellMap_.end());
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

        PostToPrimaryMaster(request, false);
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
            for (const auto& pair : RegisteredSecondaryCellMap_) {
                const auto& entry = pair.second;
                hiveManager->PostMessage(entry.Mailbox, requestMessage, reliable);
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

void TMulticellManager::PostToPrimaryMaster(
    const ::google::protobuf::MessageLite& requestMessage,
    bool reliable)
{
    Impl_->PostToPrimaryMaster(requestMessage, reliable);
}

void TMulticellManager::PostToPrimaryMaster(
    IClientRequestPtr request,
    bool reliable)
{
    Impl_->PostToPrimaryMaster(std::move(request), reliable);
}

void TMulticellManager::PostToPrimaryMaster(
    TSharedRefArray requestMessage,
    bool reliable)
{
    Impl_->PostToPrimaryMaster(std::move(requestMessage), reliable);
}

void TMulticellManager::PostToSecondaryMaster(
    IClientRequestPtr request,
    TCellTag cellTag,
    bool reliable)
{
    Impl_->PostToSecondaryMaster(std::move(request), cellTag, reliable);
}

void TMulticellManager::PostToSecondaryMaster(
    const TObjectId& objectId,
    IServiceContextPtr context,
    TCellTag cellTag,
    bool reliable)
{
    Impl_->PostToSecondaryMaster(objectId, std::move(context), cellTag, reliable);
}

void TMulticellManager::PostToSecondaryMaster(
    const ::google::protobuf::MessageLite& requestMessage,
    TCellTag cellTag,
    bool reliable)
{
    Impl_->PostToSecondaryMaster(requestMessage, cellTag, reliable);
}

void TMulticellManager::PostToSecondaryMaster(
    TSharedRefArray requestMessage,
    TCellTag cellTag,
    bool reliable)
{
    Impl_->PostToSecondaryMaster(std::move(requestMessage), cellTag, reliable);
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

bool TMulticellManager::IsRegisteredSecondaryMaster(TCellTag cellTag)
{
    return Impl_->IsRegisteredSecondaryMaster(cellTag);
}

std::vector<NObjectClient::TCellTag> TMulticellManager::GetRegisteredSecondaryMasterCellTags()
{
    return Impl_->GetRegisteredSecondaryMasterCellTags();
}

TCellTag TMulticellManager::GetLeastLoadedSecondaryMaster()
{
    return Impl_->GetLeastLoadedSecondaryMaster();
}

DELEGATE_SIGNAL(TMulticellManager, void(TCellTag), SecondaryMasterRegistered, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
