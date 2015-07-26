#include "stdafx.h"
#include "multicell_manager.h"
#include "config.h"
#include "bootstrap.h"
#include "private.h"
#include "automaton.h"
#include "serialize.h"
#include "hydra_facade.h"

#include <core/ytree/ypath_client.h>

#include <core/concurrency/periodic_executor.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/hive/cell_directory.h>

#include <server/hive/hive_manager.h>
#include <server/hive/mailbox.h>

#include <server/hydra/mutation.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/user.h>

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
        TBootstrap* bootstrap,
        TCellMasterConfigPtr config)
        : TMasterAutomatonPart(bootstrap)
        , Config_(config)
    {
        YCHECK(Config_);

        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraRegisterSecondaryMaster, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraRegisterAtPrimaryMaster, Unretained(this)));

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


    DEFINE_SIGNAL(void(TCellTag), SecondaryMasterRegistered);

private:
    const TCellMasterConfigPtr Config_;

    std::vector<TCellTag> RegisteredSecondaryCellTags_;
    bool RegisteredAtPrimaryMaster_ = false;

    TMailbox* PrimaryMasterMailbox_ = nullptr;
    std::vector<TMailbox*> SecondaryMasterMailboxes_;

    TPeriodicExecutorPtr RegisterAtPrimaryMasterExecutor_;


    virtual void OnAfterSnapshotLoaded()
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        for (auto cellTag : RegisteredSecondaryCellTags_) {
            ValidateSecondaryCellTag(cellTag);
        }
        InitializeMailboxes();
    }

    virtual void Clear() override
    {
        TMasterAutomatonPart::Clear();

        RegisteredSecondaryCellTags_.clear();
        RegisteredAtPrimaryMaster_ = false;

        PrimaryMasterMailbox_ = nullptr;
        SecondaryMasterMailboxes_.clear();
    }


    void LoadValues(TLoadContext& context)
    {
        using NYT::Load;

        Load(context, RegisteredSecondaryCellTags_);
        Load(context, RegisteredAtPrimaryMaster_);
    }

    void SaveValues(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, RegisteredSecondaryCellTags_);
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
        }
    }

    virtual void OnStopLeading()
    {
        TMasterAutomatonPart::OnStopLeading();

        if (RegisterAtPrimaryMasterExecutor_) {
            RegisterAtPrimaryMasterExecutor_->Stop();
            RegisterAtPrimaryMasterExecutor_.Reset();
        }
    }


    void HydraRegisterSecondaryMaster(const NProto::TReqRegisterSecondaryMaster& request)
    {
        auto cellTag = request.cell_tag();
        ValidateSecondaryCellTag(cellTag);

        if (std::find(RegisteredSecondaryCellTags_.begin(), RegisteredSecondaryCellTags_.end(), cellTag) !=
            RegisteredSecondaryCellTags_.end())
        {
            LOG_ERROR_UNLESS(IsRecovery(), "Attempted to re-register secondary master (CellTag: %v)", cellTag);
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Secondary master registered (CellTag: %v)", cellTag);

        RegisteredSecondaryCellTags_.push_back(cellTag);
        InitializeMailboxes();

        SecondaryMasterRegistered_.Fire(cellTag);
    }

    void HydraRegisterAtPrimaryMaster(const NProto::TReqRegisterAtPrimaryMaster& /*request*/)
    {
        if (RegisteredAtPrimaryMaster_)
            return;

        LOG_INFO_UNLESS(IsRecovery(), "Registering at primary master");

        RegisteredAtPrimaryMaster_ = true;
        InitializeMailboxes();

        NProto::TReqRegisterSecondaryMaster request;
        request.set_cell_tag(Bootstrap_->GetCellTag());
        PostToPrimaryMaster(request);
    }


    void ValidateSecondaryCellTag(TCellTag cellTag)
    {
        for (auto cellConfig : Bootstrap_->GetConfig()->SecondaryMasters) {
            if (CellTagFromId(cellConfig->CellId) == cellTag)
                return;
        }
        LOG_FATAL("Unknown secondary master cell tag %v", cellTag);
    }

    void InitializeMailboxes()
    {
        auto hiveManager = Bootstrap_->GetHiveManager();
        auto multicellManager = Bootstrap_->GetMulticellManager();

        if (RegisteredAtPrimaryMaster_ && !PrimaryMasterMailbox_) {
            PrimaryMasterMailbox_ = hiveManager->GetOrCreateMailbox(Bootstrap_->GetPrimaryCellId());
        }

        while (SecondaryMasterMailboxes_.size() < RegisteredSecondaryCellTags_.size()) {
            auto cellTag = RegisteredSecondaryCellTags_[SecondaryMasterMailboxes_.size()];
            auto cellId = ReplaceCellTagInId(Bootstrap_->GetPrimaryCellId(), cellTag);
            auto* mailbox = hiveManager->GetOrCreateMailbox(cellId);
            SecondaryMasterMailboxes_.push_back(mailbox);
        }
    }

    void OnRegisterAtPrimaryMaster()
    {
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
            auto cellId = ReplaceCellTagInId(Bootstrap_->GetPrimaryCellId(), cellTag);
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
            for (auto* mailbox : SecondaryMasterMailboxes_) {
                hiveManager->PostMessage(mailbox, requestMessage, reliable);
            }
        } else {
            YUNREACHABLE();
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TMulticellManager::TMulticellManager(
    TBootstrap* bootstrap,
    TCellMasterConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, config))
{ }

TMulticellManager::~TMulticellManager()
{ }

void TMulticellManager::PostToPrimaryMaster(
    const ::google::protobuf::MessageLite& requestMessage,
    bool reliable)
{
    Impl_->PostToPrimaryMaster(requestMessage, reliable);
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

DELEGATE_SIGNAL(TMulticellManager, void(TCellTag), SecondaryMasterRegistered, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
