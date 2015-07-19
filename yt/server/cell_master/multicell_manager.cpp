#include "stdafx.h"
#include "multicell_manager.h"
#include "config.h"
#include "bootstrap.h"
#include "private.h"
#include "automaton.h"
#include "serialize.h"
#include "hydra_facade.h"

#include <core/misc/address.h>

#include <core/ytree/ypath_client.h>

#include <core/concurrency/periodic_executor.h>

#include <ytlib/election/config.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/hive/cell_directory.h>

#include <server/hive/hive_manager.h>
#include <server/hive/mailbox.h>

#include <server/hydra/mutation.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/user.h>

//#include <server/object_server/object_manager.pb.h>

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

class TMulticellManager::TPart
    : public TMasterAutomatonPart
{
public:
    explicit TPart(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap)
    {
        TMasterAutomatonPart::RegisterMethod(BIND(&TPart::HydraRegisterSecondaryMaster, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TPart::HydraRegisterAtPrimaryMaster, Unretained(this)));

        RegisterLoader(
            "MulticellManager.Values",
            BIND(&TPart::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Values,
            "MulticellManager.Values",
            BIND(&TPart::SaveValues, Unretained(this)));
    }

    TMailbox* GetPrimaryMasterMailbox() const
    {
        return PrimaryMasterMailbox_;
    }

    const std::vector<TMailbox*>& SecondaryMasterMailboxes() const
    {
        return SecondaryMasterMailboxes_;
    }

private:
    std::vector<TCellTag> RegisteredSecondaryCellTags_;
    bool RegisteredAtPrimaryMaster_ = false;

    TMailbox* PrimaryMasterMailbox_ = nullptr;
    std::vector<TMailbox*> SecondaryMasterMailboxes_;

    TPeriodicExecutorPtr RegisterAtPrimaryMasterExecutor_;


    virtual void Clear() override
    {
        TMasterAutomatonPart::Clear();

        DoClear();
    }

    void DoClear()
    {
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


    virtual void OnBeforeSnapshotLoaded()
    {
        TMasterAutomatonPart::OnBeforeSnapshotLoaded();

        DoClear();
    }

    virtual void OnAfterSnapshotLoaded()
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        for (auto cellTag : RegisteredSecondaryCellTags_) {
            ValidateSecondaryCellTag(cellTag);
        }
        InitializeMailboxes();
    }


    virtual void OnLeaderActive()
    {
        TMasterAutomatonPart::OnLeaderActive();

        auto multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsSecondaryMaster()) {
            RegisterAtPrimaryMasterExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
                BIND(&TPart::OnRegisterAtPrimaryMaster, MakeWeak(this)),
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
    }

    void HydraRegisterAtPrimaryMaster(const NProto::TReqRegisterAtPrimaryMaster& /*request*/)
    {
        if (RegisteredAtPrimaryMaster_)
            return;

        LOG_INFO_UNLESS(IsRecovery(), "Registering at primary master");

        RegisteredAtPrimaryMaster_ = true;
        InitializeMailboxes();

        auto multicellManager = Bootstrap_->GetMulticellManager();

        NProto::TReqRegisterSecondaryMaster request;
        request.set_cell_tag(multicellManager->GetCellTag());
        multicellManager->PostToPrimaryMaster(request);
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
            PrimaryMasterMailbox_ = hiveManager->GetOrCreateMailbox(multicellManager->GetPrimaryCellId());
        }

        while (SecondaryMasterMailboxes_.size() < RegisteredSecondaryCellTags_.size()) {
            auto cellTag = RegisteredSecondaryCellTags_[SecondaryMasterMailboxes_.size()];
            auto cellId = ReplaceCellTagInId(multicellManager->GetPrimaryCellId(), cellTag);
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
};

////////////////////////////////////////////////////////////////////////////////

class TMulticellManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TBootstrap* bootstrap,
        TCellMasterConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(config)
    {
        YCHECK(Bootstrap_);
        YCHECK(Config_);
    }

    void Initialize()
    {
        Config_->PrimaryMaster->ValidateAllPeersPresent();
        for (auto cellConfig : Config_->SecondaryMasters) {
            cellConfig->ValidateAllPeersPresent();
        }

        auto localAdddress = BuildServiceAddress(
            TAddressResolver::Get()->GetLocalHostName(),
            Config_->RpcPort);

        auto primaryId = ComputePeerId(Config_->PrimaryMaster, localAdddress);
        if (primaryId == InvalidPeerId) {
            for (auto cellConfig : Config_->SecondaryMasters) {
                auto secondaryId = ComputePeerId(cellConfig, localAdddress);
                if (secondaryId != InvalidPeerId) {
                    SecondaryMaster_ = true;
                    CellConfig_ = cellConfig;
                    PeerId_ = secondaryId;
                    break;
                }
            }
        } else {
            PrimaryMaster_ = true;
            CellConfig_ = Config_->PrimaryMaster;
            PeerId_ = primaryId;
        }

        if (!PrimaryMaster_ && !SecondaryMaster_) {
            THROW_ERROR_EXCEPTION("Local address %v is not recognized as a valid master address",
                localAdddress);
        }

        if (PrimaryMaster_) {
            LOG_INFO("Running as primary master (CellId: %v, CellTag: %v, PeerId: %v)",
                CellId_,
                CellTag_,
                PeerId_);
        } else {
            LOG_INFO("Running as secondary master (CellId: %v, CellTag: %v, PrimaryCellTag: %v, PeerId: %v)",
                CellId_,
                CellTag_,
                PrimaryCellTag_,
                PeerId_);
        }

        Multicell_ = !Config_->SecondaryMasters.empty();

        CellId_ = CellConfig_->CellId;
        CellTag_ = CellTagFromId(CellId_);

        PrimaryCellId_ = Config_->PrimaryMaster->CellId;
        PrimaryCellTag_ = CellTagFromId(PrimaryCellId_);

        for (const auto& cellConfig : Config_->SecondaryMasters) {
            SecondaryCellTags_.push_back(CellTagFromId(cellConfig->CellId));
        }

        auto cellDirectory = Bootstrap_->GetCellDirectory();
        YCHECK(cellDirectory->ReconfigureCell(Config_->PrimaryMaster));
        for (const auto& cellConfig : Config_->SecondaryMasters) {
            YCHECK(cellDirectory->ReconfigureCell(cellConfig));
        }

    }

    void Start()
    {
        Part_ = New<TPart>(Bootstrap_);
    }


    bool IsPrimaryMaster() const
    {
        return PrimaryMaster_;
    }

    bool IsSecondaryMaster() const
    {
        return SecondaryMaster_;
    }

    bool IsMulticell() const
    {
        return Multicell_;
    }


    const TCellId& GetCellId() const
    {
        return CellId_;
    }

    TCellTag GetCellTag() const
    {
        return CellTag_;
    }

    const TCellId& GetPrimaryCellId() const
    {
        return PrimaryCellId_;
    }

    TCellTag GetPrimaryCellTag() const
    {
        return PrimaryCellTag_;
    }


    const std::vector<TCellTag>& GetSecondaryCellTags() const
    {
        return SecondaryCellTags_;
    }


    TCellConfigPtr GetCellConfig() const
    {
        return CellConfig_;
    }

    TPeerId GetPeerId() const
    {
        return PeerId_;
    }



    void PostToPrimaryMaster(
        const ::google::protobuf::MessageLite& requestMessage,
        bool reliable)
    {
        YCHECK(IsSecondaryMaster());

        auto* mailbox = Part_->GetPrimaryMasterMailbox();
        if (!reliable && !mailbox)
            return;

        // Failure here indicates an attempt to send a reliable message to the primary master
        // before registering.
        YCHECK(mailbox);

        auto hiveManager = Bootstrap_->GetHiveManager();
        hiveManager->PostMessage(mailbox, requestMessage, reliable);
    }

    void PostToSecondaryMasters(
        IClientRequestPtr request,
        bool reliable)
    {
        YCHECK(IsPrimaryMaster());

        if (!Multicell_)
            return;

        PostToSecondaryMasters(request->Serialize(), reliable);
    }

    void PostToSecondaryMasters(
        const TObjectId& objectId,
        IServiceContextPtr context,
        bool reliable)
    {
        YCHECK(IsPrimaryMaster());

        if (!Multicell_)
            return;

        auto requestMessage = context->GetRequestMessage();
        NRpc::NProto::TRequestHeader requestHeader;
        ParseRequestHeader(requestMessage, &requestHeader);

        auto updatedYPath = FromObjectId(objectId) + GetRequestYPath(context);
        SetRequestYPath(&requestHeader, updatedYPath);
        auto updatedRequestMessage = SetRequestHeader(requestMessage, requestHeader);

        PostToSecondaryMasters(std::move(updatedRequestMessage), reliable);
    }

    void PostToSecondaryMasters(
        TSharedRefArray requestMessage,
        bool reliable)
    {
        YCHECK(IsPrimaryMaster());

        if (!Multicell_)
            return;

        auto securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();

        NObjectServer::NProto::TReqExecute hydraReq;
        ToProto(hydraReq.mutable_user_id(), user->GetId());
        for (const auto& part : requestMessage) {
            hydraReq.add_request_parts(part.Begin(), part.Size());
        }

        auto hiveManager = Bootstrap_->GetHiveManager();
        for (auto* mailbox : Part_->SecondaryMasterMailboxes()) {
            hiveManager->PostMessage(mailbox, hydraReq, reliable);
        }
    }

    void PostToSecondaryMasters(
        const ::google::protobuf::MessageLite& requestMessage,
        bool reliable)
    {
        YCHECK(IsPrimaryMaster());

        if (!Multicell_)
            return;

        auto hiveManager = Bootstrap_->GetHiveManager();
        for (auto* mailbox : Part_->SecondaryMasterMailboxes()) {
            hiveManager->PostMessage(mailbox, requestMessage, reliable);
        }
    }

private:
    TBootstrap* const Bootstrap_;
    const TCellMasterConfigPtr Config_;

    TIntrusivePtr<TPart> Part_;

    bool PrimaryMaster_ = false;
    bool SecondaryMaster_ = false;
    bool Multicell_ = false;

    TCellId CellId_;
    TCellTag CellTag_;
    TCellId PrimaryCellId_;
    TCellTag PrimaryCellTag_;
    std::vector<TCellTag> SecondaryCellTags_;

    TCellConfigPtr CellConfig_;
    TPeerId PeerId_ = InvalidPeerId;


    static TPeerId ComputePeerId(TCellConfigPtr config, const Stroka& localAddress)
    {
        for (TPeerId id = 0; id < config->Addresses.size(); ++id) {
            const auto& peerAddress = config->Addresses[id];
            if (peerAddress && to_lower(*peerAddress) == to_lower(localAddress)) {
                return id;
            }
        }
        return InvalidPeerId;
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

void TMulticellManager::Initialize()
{
    Impl_->Initialize();
}

void TMulticellManager::Start()
{
    Impl_->Start();
}

bool TMulticellManager::IsPrimaryMaster() const
{
    return Impl_->IsPrimaryMaster();
}

bool TMulticellManager::IsSecondaryMaster() const
{
    return Impl_->IsSecondaryMaster();
}

bool TMulticellManager::IsMulticell() const
{
    return Impl_->IsMulticell();
}

const TCellId& TMulticellManager::GetCellId() const
{
    return Impl_->GetCellId();
}

TCellTag TMulticellManager::GetCellTag() const
{
    return Impl_->GetCellTag();
}

const TCellId& TMulticellManager::GetPrimaryCellId() const
{
    return Impl_->GetPrimaryCellId();
}

TCellTag TMulticellManager::GetPrimaryCellTag() const
{
    return Impl_->GetPrimaryCellTag();
}

const std::vector<NObjectClient::TCellTag>& TMulticellManager::GetSecondaryCellTags() const
{
    return Impl_->GetSecondaryCellTags();
}

TCellConfigPtr TMulticellManager::GetCellConfig() const
{
    return Impl_->GetCellConfig();
}

TPeerId TMulticellManager::GetPeerId() const
{
    return Impl_->GetPeerId();
}

void TMulticellManager::PostToPrimaryMaster(
    const ::google::protobuf::MessageLite& requestMessage,
    bool reliable)
{
    Impl_->PostToPrimaryMaster(requestMessage, reliable);
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
    TSharedRefArray requestMessage,
    bool reliable)
{
    Impl_->PostToSecondaryMasters(std::move(requestMessage), reliable);
}

void TMulticellManager::PostToSecondaryMasters(
    const ::google::protobuf::MessageLite& requestMessage,
    bool reliable)
{
    Impl_->PostToSecondaryMasters(requestMessage, reliable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
