#include "stdafx.h"
#include "hydra_facade.h"
#include "automaton.h"
#include "config.h"
#include "private.h"

#include <core/misc/fs.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/ypath_client.h>

#include <core/ypath/token.h>

#include <core/rpc/bus_channel.h>
#include <core/rpc/server.h>
#include <core/rpc/response_keeper.h>
#include <core/rpc/message.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/periodic_executor.h>

#include <core/logging/log.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hive/cell_directory.h>

#include <server/election/election_manager.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/changelog.h>
#include <server/hydra/snapshot.h>
#include <server/hydra/distributed_hydra_manager.h>
#include <server/hydra/file_helpers.h>
#include <server/hydra/private.h>

#include <server/hive/transaction_supervisor.h>
#include <server/hive/hive_manager.h>
#include <server/hive/mailbox.h>

#include <server/cell_master/bootstrap.h>

#include <server/cypress_server/cypress_manager.h>
#include <server/cypress_server/node_detail.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/acl.h>
#include <server/security_server/user.h>
#include <server/security_server/group.h>

#include <server/object_server/private.h>
#include <server/object_server/object_manager.pb.h>

namespace NYT {
namespace NCellMaster {

using namespace NConcurrency;
using namespace NRpc;
using namespace NElection;
using namespace NHydra;
using namespace NYTree;
using namespace NYPath;
using namespace NRpc;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NHive;
using namespace NHive::NProto;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellMasterLogger;
static const auto SnapshotCleanupPeriod = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

class THydraFacade::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TCellMasterConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
    {
        YCHECK(Config_);
        YCHECK(Bootstrap_);

        CellId_ = Config_->Master->CellId;
        CellTag_ = CellTagFromId(CellId_);

        auto primaryMasterConfig = Config_->PrimaryMaster ? Config_->PrimaryMaster : Config_->Master;
        PrimaryCellId_ = primaryMasterConfig->CellId;
        PrimaryCellTag_ = CellTagFromId(PrimaryCellId_);

        for (const auto& secondaryMaster : Config_->SecondaryMasters) {
            SecondaryCellTags_.push_back(CellTagFromId(secondaryMaster->CellId));
        }

        Multicell_ = !Config_->SecondaryMasters.empty();

        AutomatonQueue_ = New<TFairShareActionQueue>("Automaton", TEnumTraits<EAutomatonThreadQueue>::GetDomainNames());
        Automaton_ = New<TMasterAutomaton>(Bootstrap_);

        ResponseKeeper_ = New<TResponseKeeper>(
            Config_->HydraManager->ResponseKeeper,
            NObjectServer::ObjectServerLogger,
            NObjectServer::ObjectServerProfiler);

        TDistributedHydraManagerOptions hydraManagerOptions;
        hydraManagerOptions.ResponseKeeper = ResponseKeeper_;
        hydraManagerOptions.UseFork = true;
        HydraManager_ = CreateDistributedHydraManager(
            Config_->HydraManager,
            Bootstrap_->GetControlInvoker(),
            GetAutomatonInvoker(EAutomatonThreadQueue::Mutation),
            Automaton_,
            Bootstrap_->GetRpcServer(),
            Bootstrap_->GetCellManager(),
            Bootstrap_->GetChangelogStore(),
            Bootstrap_->GetSnapshotStore(),
            hydraManagerOptions);

        HydraManager_->SubscribeStartLeading(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
        HydraManager_->SubscribeLeaderActive(BIND(&TImpl::OnLeaderActive, MakeWeak(this)));
        HydraManager_->SubscribeStopLeading(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));

        HydraManager_->SubscribeStartFollowing(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
        HydraManager_->SubscribeStopFollowing(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));

        for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
            auto unguardedInvoker = GetAutomatonInvoker(queue);
            GuardedInvokers_[queue] = HydraManager_->CreateGuardedAutomatonInvoker(unguardedInvoker);
        }
    }

    void Start()
    {
        if (IsPrimaryMaster()) {
            LOG_INFO("Running as primary master (CellId: %v, CellTag: %v)",
                CellId_,
                CellTag_);
        } else {
            LOG_INFO("Running as secondary master (CellId: %v, CellTag: %v, PrimaryCellTag: %v)",
                CellId_,
                CellTag_,
                PrimaryCellTag_);
        }

        HydraManager_->Initialize();

        SnapshotCleanupExecutor_ = New<TPeriodicExecutor>(
            GetHydraIOInvoker(),
            BIND(&TImpl::OnSnapshotCleanup, MakeWeak(this)),
            SnapshotCleanupPeriod);
        SnapshotCleanupExecutor_->Start();
    }

    void DumpSnapshot(ISnapshotReaderPtr reader)
    {
        WaitFor(reader->Open())
            .ThrowOnError();

        Automaton_->SetSerializationDumpEnabled(true);
        Automaton_->Clear();
        Automaton_->LoadSnapshot(reader);
    }


    TMasterAutomatonPtr GetAutomaton() const
    {
        return Automaton_;
    }

    IHydraManagerPtr GetHydraManager() const
    {
        return HydraManager_;
    }

    TResponseKeeperPtr GetResponseKeeper() const
    {
        return ResponseKeeper_;
    }


    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        return AutomatonQueue_->GetInvoker(static_cast<int>(queue));
    }

    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        return EpochInvokers_[queue];
    }

    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        return GuardedInvokers_[queue];
    }


    bool IsPrimaryMaster() const
    {
        return CellTag_ == PrimaryCellTag_;
    }

    bool IsSecondaryMaster() const
    {
        return CellTag_ != PrimaryCellTag_;
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


    void PostToPrimaryMaster(
        const ::google::protobuf::MessageLite& requestMessage,
        bool reliable)
    {
        YCHECK(IsSecondaryMaster());

        InitializeMailboxes();

        auto hiveManager = Bootstrap_->GetHiveManager();
        hiveManager->PostMessage(PrimaryMasterMailbox_, requestMessage, reliable);
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

        InitializeMailboxes();

        auto hiveManager = Bootstrap_->GetHiveManager();
        for (auto* mailbox : SecondaryMasterMailboxes_) {
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

        InitializeMailboxes();

        auto hiveManager = Bootstrap_->GetHiveManager();
        for (auto* mailbox : SecondaryMasterMailboxes_) {
            hiveManager->PostMessage(mailbox, requestMessage, reliable);
        }
    }

private:
    const TCellMasterConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    TCellId CellId_;
    TCellTag CellTag_;
    TCellId PrimaryCellId_;
    TCellTag PrimaryCellTag_;
    std::vector<TCellTag> SecondaryCellTags_;

    bool Multicell_ = false;
    bool MailboxesInitialized_ = false;
    std::vector<TMailbox*> SecondaryMasterMailboxes_;
    TMailbox* PrimaryMasterMailbox_ = nullptr;

    TFairShareActionQueuePtr AutomatonQueue_;
    TMasterAutomatonPtr Automaton_;
    IHydraManagerPtr HydraManager_;

    TResponseKeeperPtr ResponseKeeper_;

    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> GuardedInvokers_;
    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> EpochInvokers_;

    TPeriodicExecutorPtr SnapshotCleanupExecutor_;


    void OnStartEpoch()
    {
        auto cancelableContext = HydraManager_->GetAutomatonCancelableContext();
        for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
            auto unguardedInvoker = GetAutomatonInvoker(queue);
            EpochInvokers_[queue] = cancelableContext->CreateInvoker(unguardedInvoker);
        }

        auto cellDirectory = Bootstrap_->GetCellDirectory();
        cellDirectory->Clear();
    }

    void OnLeaderActive()
    {
        auto cellDirectory = Bootstrap_->GetCellDirectory();
        YCHECK(cellDirectory->ReconfigureCell(Config_->Master));
        if (Config_->PrimaryMaster) {
            YCHECK(cellDirectory->ReconfigureCell(Config_->PrimaryMaster));
        }
        for (const auto& secondaryMaster : Config_->SecondaryMasters) {
            YCHECK(cellDirectory->ReconfigureCell(secondaryMaster));
        }
    }

    void OnStopEpoch()
    {
        std::fill(EpochInvokers_.begin(), EpochInvokers_.end(), nullptr);
    }


    void OnSnapshotCleanup()
    {
        auto snapshotsPath = Config_->Snapshots->Path;

        std::vector<int> snapshotIds;
        auto snapshotFileNames = NFS::EnumerateFiles(snapshotsPath);
        for (const auto& fileName : snapshotFileNames) {
            if (NFS::GetFileExtension(fileName) != SnapshotExtension)
                continue;

            int snapshotId;
            try {
                snapshotId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
            } catch (const std::exception& ex) {
                LOG_WARNING("Unrecognized item %v in snapshot store",
                    fileName);
                continue;
            }
            snapshotIds.push_back(snapshotId);
        }

        if (snapshotIds.size() <= Config_->HydraManager->MaxSnapshotsToKeep)
            return;

        std::sort(snapshotIds.begin(), snapshotIds.end());
        int thresholdId = snapshotIds[snapshotIds.size() - Config_->HydraManager->MaxSnapshotsToKeep];

        for (const auto& fileName : snapshotFileNames) {
            if (NFS::GetFileExtension(fileName) != SnapshotExtension)
                continue;

            int snapshotId;
            try {
                snapshotId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
            } catch (const std::exception& ex) {
                // Ignore, cf. logging above.
                continue;
            }

            if (snapshotId < thresholdId) {
                LOG_INFO("Removing snapshot %v",
                    snapshotId);

                try {
                    NFS::Remove(NFS::CombinePaths(snapshotsPath, fileName));
                } catch (const std::exception& ex) {
                    LOG_WARNING(ex, "Error removing %v from snapshot store",
                        fileName);
                }
            }
        }

        auto changelogsPath = Config_->Changelogs->Path;
        auto changelogFileNames = NFS::EnumerateFiles(changelogsPath);
        for (const auto& fileName : changelogFileNames) {
            if (NFS::GetFileExtension(fileName) != ChangelogExtension)
                continue;

            int changelogId;
            try {
                changelogId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
            } catch (const std::exception& ex) {
                LOG_WARNING("Unrecognized item %v in changelog store",
                    fileName);
                continue;
            }

            if (changelogId < thresholdId) {
                LOG_INFO("Removing changelog %v",
                    changelogId);
                try {
                    RemoveChangelogFiles(NFS::CombinePaths(changelogsPath, fileName));
                } catch (const std::exception& ex) {
                    LOG_WARNING(ex, "Error removing %v from changelog store",
                        fileName);
                }
            }
        }
    }


    void InitializeMailboxes()
    {
        if (MailboxesInitialized_)
            return;

        auto hiveManager = Bootstrap_->GetHiveManager();
        if (IsPrimaryMaster()) {
            for (const auto& secondaryMaster : Config_->SecondaryMasters) {
                auto* mailbox = hiveManager->GetOrCreateMailbox(secondaryMaster->CellId);
                SecondaryMasterMailboxes_.push_back(mailbox);
            }
        } else {
            PrimaryMasterMailbox_ = hiveManager->GetOrCreateMailbox(Config_->PrimaryMaster->CellId);
        }

        MailboxesInitialized_ = true;
    }

};

////////////////////////////////////////////////////////////////////////////////

THydraFacade::THydraFacade(
    TCellMasterConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

THydraFacade::~THydraFacade()
{ }

void THydraFacade::Start()
{
    Impl_->Start();
}

void THydraFacade::DumpSnapshot(ISnapshotReaderPtr reader)
{
    Impl_->DumpSnapshot(reader);
}

TMasterAutomatonPtr THydraFacade::GetAutomaton() const
{
    return Impl_->GetAutomaton();
}

IHydraManagerPtr THydraFacade::GetHydraManager() const
{
    return Impl_->GetHydraManager();
}

TResponseKeeperPtr THydraFacade::GetResponseKeeper() const
{
    return Impl_->GetResponseKeeper();
}

IInvokerPtr THydraFacade::GetAutomatonInvoker(EAutomatonThreadQueue queue) const
{
    return Impl_->GetAutomatonInvoker(queue);
}

IInvokerPtr THydraFacade::GetEpochAutomatonInvoker(EAutomatonThreadQueue queue) const
{
    return Impl_->GetEpochAutomatonInvoker(queue);
}

IInvokerPtr THydraFacade::GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue) const
{
    return Impl_->GetGuardedAutomatonInvoker(queue);
}

bool THydraFacade::IsPrimaryMaster() const
{
    return Impl_->IsPrimaryMaster();
}

bool THydraFacade::IsSecondaryMaster() const
{
    return Impl_->IsSecondaryMaster();
}

const TCellId& THydraFacade::GetCellId() const
{
    return Impl_->GetCellId();
}

TCellTag THydraFacade::GetCellTag() const
{
    return Impl_->GetCellTag();
}

const TCellId& THydraFacade::GetPrimaryCellId() const
{
    return Impl_->GetPrimaryCellId();
}

TCellTag THydraFacade::GetPrimaryCellTag() const
{
    return Impl_->GetPrimaryCellTag();
}

const std::vector<NObjectClient::TCellTag>& THydraFacade::GetSecondaryCellTags() const
{
    return Impl_->GetSecondaryCellTags();
}

void THydraFacade::PostToPrimaryMaster(
    const ::google::protobuf::MessageLite& requestMessage,
    bool reliable)
{
    Impl_->PostToPrimaryMaster(requestMessage, reliable);
}

void THydraFacade::PostToSecondaryMasters(
    IClientRequestPtr request,
    bool reliable)
{
    Impl_->PostToSecondaryMasters(std::move(request), reliable);
}

void THydraFacade::PostToSecondaryMasters(
    const TObjectId& objectId,
    IServiceContextPtr context,
    bool reliable)
{
    Impl_->PostToSecondaryMasters(objectId, std::move(context), reliable);
}

void THydraFacade::PostToSecondaryMasters(
    TSharedRefArray requestMessage,
    bool reliable)
{
    Impl_->PostToSecondaryMasters(std::move(requestMessage), reliable);
}

void THydraFacade::PostToSecondaryMasters(
    const ::google::protobuf::MessageLite& requestMessage,
    bool reliable)
{
    Impl_->PostToSecondaryMasters(requestMessage, reliable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

