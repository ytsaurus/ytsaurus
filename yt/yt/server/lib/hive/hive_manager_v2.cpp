#include "hive_manager_v2.h"

#include "avenue_directory.h"
#include "hive_manager.h"
#include "persistent_mailbox_state_cookie.h"
#include "config.h"
#include "helpers.h"
#include "logical_time_registry.h"
#include "mailbox_v2.h"
#include "private_v2.h"

#include <yt/yt/server/lib/election/election_manager.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/hydra_manager.h>
#include <yt/yt/server/lib/hydra/hydra_service.h>
#include <yt/yt/server/lib/hydra/mutation.h>
#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/hive_service_proxy.h>

#include <yt/yt/ytlib/hydra/config.h>
#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/async_batcher.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NHiveServer::NV2 {

using namespace NNet;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NHydra;
using namespace NHydra::NProto;
using namespace NHiveClient;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NTracing;
using namespace NProfiling;
using namespace NObjectClient;

using NYT::ToProto;
using NYT::FromProto;

using NHiveClient::NProto::TEncapsulatedMessage;

////////////////////////////////////////////////////////////////////////////////

class THiveManager
    : public IHiveManager
    , public THydraServiceBase
    , public TCompositeAutomatonPart
{
public:
    THiveManager(
        THiveManagerConfigPtr config,
        ICellDirectoryPtr cellDirectory,
        NCellMasterClient::ICellDirectoryPtr masterDirectory,
        IAvenueDirectoryPtr avenueDirectory,
        TCellId selfCellId,
        IInvokerPtr automatonInvoker,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        IUpstreamSynchronizerPtr upstreamSynchronizer,
        IAuthenticatorPtr authenticator)
        : THydraServiceBase(
            hydraManager,
            hydraManager->CreateGuardedAutomatonInvoker(automatonInvoker),
            THiveServiceProxy::GetDescriptor(),
            HiveServerLogger(),
            std::move(upstreamSynchronizer),
            TServiceOptions{
                .RealmId = selfCellId,
                .Authenticator = std::move(authenticator),
            })
        , TCompositeAutomatonPart(
            hydraManager,
            automaton,
            automatonInvoker)
        , SelfCellId_(selfCellId)
        , Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , MasterDirectory_(std::move(masterDirectory))
        , AvenueDirectory_(std::move(avenueDirectory))
        , AutomatonInvoker_(std::move(automatonInvoker))
        , GuardedAutomatonInvoker_(hydraManager->CreateGuardedAutomatonInvoker(AutomatonInvoker_))
        , HydraManager_(std::move(hydraManager))
        , Profiler_(HiveServerProfiler()
            .WithGlobal()
            .WithSparse()
            .WithTag("cell_id", ToString(selfCellId)))
        , LogicalTimeRegistry_(New<TLogicalTimeRegistry>(
            Config_->LogicalTimeRegistry,
            AutomatonInvoker_,
            HydraManager_,
            Profiler_))
        , LamportClock_(LogicalTimeRegistry_->GetClock())
        , BackgroundInvoker_(CreateSerializedInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker()))
    {
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(Ping)
            .SetInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SyncCells)
            .SetHeavy(true));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PostMessages)
            .SetInvoker(BackgroundInvoker_));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SendMessages)
            .SetInvoker(BackgroundInvoker_));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SyncWithOthers));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(GetConsistentState));

        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&THiveManager::HydraAcknowledgeMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&THiveManager::HydraPostMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&THiveManager::HydraSendMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&THiveManager::HydraRegisterMailbox, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&THiveManager::HydraUnregisterMailbox, Unretained(this)));

        RegisterLoader(
            "HiveManager.Keys",
            BIND_NO_PROPAGATE(&THiveManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "HiveManager.Values",
            BIND_NO_PROPAGATE(&THiveManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "HiveManager.Keys",
            BIND_NO_PROPAGATE(&THiveManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "HiveManager.Values",
            BIND_NO_PROPAGATE(&THiveManager::SaveValues, Unretained(this)));

        OrchidService_ = CreateOrchidService();

        if (AvenueDirectory_) {
            AvenueDirectory_->SubscribeEndpointUpdated(
                BIND_NO_PROPAGATE(&THiveManager::OnAvenueDirectoryEndpointUpdated, MakeWeak(this))
                    .Via(BackgroundInvoker_));
        }
    }

    IServicePtr GetRpcService() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return this;
    }

    IYPathServicePtr GetOrchidService() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return OrchidService_;
    }

    TCellId GetSelfCellId() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return SelfCellId_;
    }

    TMailboxHandle CreateCellMailbox(TCellId cellId, bool allowResurrection = false) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (UnregisteredCellIds_.erase(cellId) != 0 && !allowResurrection) {
            YT_LOG_ALERT("Mailbox has been resurrected (SelfCellId: %v, CellId: %v)",
                SelfCellId_,
                cellId);
        }

        auto mailboxHolder = std::make_unique<TCellMailbox>(cellId);
        auto* mailbox = CellMailboxMap_.Insert(cellId, std::move(mailboxHolder));

        if (!IsRecovery()) {
            auto cellRuntimeData = CreateMailboxRuntimeData(mailbox);

            RuntimeData_.Transform([&] (auto& runtimeData) {
                EmplaceOrCrash(runtimeData.CellIdToCellRuntimeData, cellId, mailbox->GetRuntimeData());
            });

            BackgroundInvoker_->Invoke(
                BIND_NO_PROPAGATE(&THiveManager::RunPeriodicCellCheck, MakeWeak(this), cellRuntimeData));
        }

        YT_LOG_INFO("Mailbox created (SelfCellId: %v, CellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());
        return AsUntyped(mailbox);
    }

    TMailboxHandle FindMailbox(TEndpointId endpointId) const override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (IsAvenueEndpointType(TypeFromId(endpointId))) {
            return AsUntyped(AvenueMailboxMap_.Find(endpointId));
        } else {
            return AsUntyped(CellMailboxMap_.Find(endpointId));
        }
    }

    TMailboxHandle GetMailbox(TEndpointId endpointId) const override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto mailbox = FindMailbox(endpointId);
        YT_VERIFY(mailbox);
        return mailbox;
    }

    TMailboxHandle GetOrCreateCellMailbox(TCellId cellId) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto* mailbox = CellMailboxMap_.Find(cellId);
        if (!mailbox) {
            mailbox = AsTyped(CreateCellMailbox(cellId))->AsCell();
        }
        return AsUntyped(mailbox);
    }

    TMailboxHandle GetMailboxOrThrow(TEndpointId endpointId) const override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto mailbox = FindMailbox(endpointId);
        if (!mailbox) {
            THROW_ERROR_EXCEPTION("No such %v mailbox %v",
                IsAvenueEndpointType(TypeFromId(endpointId)) ? "avenue" : "cell",
                endpointId);
        }
        return mailbox;
    }

    void FreezeEdges(std::vector<THiveEdge> edgesToFreeze) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        FrozenEdges_.Transform([&] (auto& frozenEdges) {
            if (!frozenEdges.empty()) {
                YT_LOG_INFO("Unfreezing Hive edges (Edges: %v)",
                    frozenEdges);
                frozenEdges.clear();
            }

            if (edgesToFreeze.empty()) {
                return;
            }

            frozenEdges = std::move(edgesToFreeze);
            YT_LOG_INFO("Freezing Hive edges (Edges: %v)",
                edgesToFreeze);
        });
    }

    bool TryRemoveCellMailbox(TCellId cellId) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (!CellMailboxMap_.Contains(cellId)) {
            return false;
        }

        RemoveCellMailbox(cellId);

        YT_LOG_INFO("Mailbox removed (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            cellId);

        return true;
    }

    bool TryUnregisterCellMailbox(TCellId cellId) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!CellMailboxMap_.Contains(cellId)) {
            return false;
        }

        RemoveCellMailbox(cellId);

        if (!UnregisteredCellIds_.insert(cellId).second) {
            YT_LOG_ALERT("Mailbox is already unregistered (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                cellId);
        }

        YT_LOG_INFO("Mailbox unregistered (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            cellId);

        return true;
    }

    void RemoveCellMailbox(TCellId cellId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto mailboxHolder = CellMailboxMap_.Release(cellId);

        if (!IsRecovery()) {
            RuntimeData_.Transform([&] (auto& runtimeData) {
                EraseOrCrash(runtimeData.CellIdToCellRuntimeData, cellId);
            });

            BackgroundInvoker_->Invoke(BIND([this, this_ = MakeStrong(this), cellRuntimeData = mailboxHolder->GetRuntimeData()] {
                YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

                // Following updates will change the map so we make a copy.
                auto registeredAvenues = cellRuntimeData->RegisteredAvenues;
                for (const auto& avenueRuntimeData : registeredAvenues) {
                    UpdateAvenueCellConnection(avenueRuntimeData, /*cellRuntimeData*/ nullptr);
                }
            }));
        }
    }

    void RegisterAvenueEndpoint(
        TAvenueEndpointId selfEndpointId,
        TPersistentMailboxStateCookie&& cookie) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto otherEndpointId = GetSiblingAvenueEndpointId(selfEndpointId);
        if (AvenueMailboxMap_.Find(otherEndpointId)) {
            YT_LOG_ALERT("Attempted to register already existing avenue endpoint, ignored "
                "(SelfCellId: %v, SelfEndpointId: %v)",
                SelfCellId_,
                selfEndpointId);
            return;
        }

        auto holder = std::make_unique<TAvenueMailbox>(otherEndpointId);
        auto* mailbox = AvenueMailboxMap_.Insert(otherEndpointId, std::move(holder));
        mailbox->GetPersistentState()->LoadFromCookie(std::move(cookie));

        auto outcomingMessageIdRange = mailbox->GetPersistentState()->GetOutcomingMessageIdRange();
        auto nextPersistentIncomingMessageId = mailbox->GetPersistentState()->GetNextPersistentIncomingMessageId();
        YT_LOG_DEBUG("Avenue mailbox registered "
            "(SelfCellId: %v, SelfEndpointId: %v, OtherEndpointId: %v, "
            "OutcomingMessageId: %v, OutcomingMessageCount: %v, "
            "NextPersistentIncomingMessageId: %v)",
            SelfCellId_,
            selfEndpointId,
            otherEndpointId,
            outcomingMessageIdRange.Begin,
            outcomingMessageIdRange.GetCount(),
            nextPersistentIncomingMessageId);

        if (!IsRecovery()) {
            auto avenueRuntimeData = CreateMailboxRuntimeData(mailbox);

            RuntimeData_.Transform([&] (auto& runtimeData) {
                EmplaceOrCrash(runtimeData.EndpointIdToAvenueRuntimeData, otherEndpointId, avenueRuntimeData);
            });

            BackgroundInvoker_->Invoke(BIND([this, this_ = MakeStrong(this), avenueRuntimeData] {
                YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

                UpdateAvenueCellConnection(avenueRuntimeData);
            }));
        }
    }

    TPersistentMailboxStateCookie UnregisterAvenueEndpoint(
        TAvenueEndpointId selfEndpointId,
        bool allowDestructionInMessageToSelf) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto otherEndpointId = GetSiblingAvenueEndpointId(selfEndpointId);

        auto* mailbox = AvenueMailboxMap_.Find(otherEndpointId);
        if (!mailbox) {
            YT_LOG_ALERT("Attempted to remove a non-existing avenue endpoint, ignored "
                "(SelfCellId: %v, SelfEndpointId: %v)",
                SelfCellId_,
                selfEndpointId);
            return {};
        }

        if (GetHiveMutationSenderId() == otherEndpointId) {
            if (allowDestructionInMessageToSelf) {
                YT_LOG_DEBUG("Attempted to unregister avenue endpoint in the mutation "
                    "received by itself, unregistration will be delayed (%v)",
                    FormatIncomingMailboxEndpoints(otherEndpointId));
                mailbox->SetRemovalScheduled(true);
                return {};
            } else {
                YT_LOG_FATAL("Attempted to unregister avenue endpoint in the mutation "
                    "received by itself (%v)",
                    FormatIncomingMailboxEndpoints(otherEndpointId));
            }
        }

        return DoUnregisterAvenueEndpoint(mailbox);
    }

    TPersistentMailboxStateCookie DoUnregisterAvenueEndpoint(TAvenueMailbox* mailbox)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto otherEndpointId = mailbox->GetEndpointId();
        auto selfEndpointId = GetSiblingAvenueEndpointId(otherEndpointId);

        if (!IsRecovery()) {
            RuntimeData_.Transform([&] (auto& runtimeData) {
                EraseOrCrash(runtimeData.EndpointIdToAvenueRuntimeData, otherEndpointId);
            });

            BackgroundInvoker_->Invoke(BIND([this, this_ = MakeStrong(this), avenueRuntimeData = mailbox->GetRuntimeData()] {
                YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

                UpdateAvenueCellConnection(avenueRuntimeData, /*cellRuntimeData*/ nullptr);
            }));
        }

        auto cookie = mailbox->GetPersistentState()->SaveToCookie();

        AvenueMailboxMap_.Remove(otherEndpointId);

        YT_LOG_DEBUG("Avenue mailbox unregistered "
            "(SelfCellId: %v, SelfEndpointId: %v, OtherEndpointId: %v, "
            "FirstOutcomingMessageId: %v, OutcomingMessageCount: %v, "
            "NextPersistentIncomingMessageId: %v)",
            SelfCellId_,
            selfEndpointId,
            otherEndpointId,
            cookie.FirstOutcomingMessageId,
            cookie.OutcomingMessages.size(),
            cookie.NextPersistentIncomingMessageId);

        return cookie;
    }

    void PostMessage(TMailboxHandle mailbox, const TSerializedMessagePtr& message, bool reliable) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        PostMessage(std::array{mailbox}, message, reliable);
    }

    void PostMessage(TRange<TMailboxHandle> mailboxes, const TSerializedMessagePtr& message, bool reliable) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (reliable) {
            ReliablePostMessage(mailboxes, message);
        } else {
            UnreliablePostMessage(mailboxes, message);
        }
    }

    void PostMessage(TMailboxHandle mailbox, const ::google::protobuf::MessageLite& message, bool reliable) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        PostMessage(mailbox, SerializeOutcomingMessage(message), reliable);
    }

    void PostMessage(TRange<TMailboxHandle> mailboxes, const ::google::protobuf::MessageLite& message, bool reliable) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        PostMessage(mailboxes, SerializeOutcomingMessage(message), reliable);
    }

    TFuture<void> SyncWith(TCellId cellId, bool enableBatching) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (cellId == SelfCellId_) {
            return VoidFuture;
        }

        if (enableBatching) {
            try {
                auto cellRuntimeData = FindCellMailboxRuntimeData(cellId, /*throwIfUnavailable*/ true);
                if (!cellRuntimeData) {
                    THROW_ERROR_EXCEPTION(
                        NRpc::EErrorCode::Unavailable,
                        "Cannot synchronize with cell %v since it is not known",
                        cellId);
                }
                return cellRuntimeData->SyncBatcher->Run();
            } catch (const std::exception& ex) {
                return MakeFuture<void>(ex);
            }
        } else {
            return DoSyncWith(cellId).ToImmediatelyCancelable();
        }
    }

private:
    const TCellId SelfCellId_;
    const THiveManagerConfigPtr Config_;
    const ICellDirectoryPtr CellDirectory_;
    const NCellMasterClient::ICellDirectoryPtr MasterDirectory_;
    const IAvenueDirectoryPtr AvenueDirectory_;
    const IInvokerPtr AutomatonInvoker_;
    const IInvokerPtr GuardedAutomatonInvoker_;
    const IHydraManagerPtr HydraManager_;
    const TProfiler Profiler_;

    const TLogicalTimeRegistryPtr LogicalTimeRegistry_;
    TLogicalTimeRegistry::TLamportClock* const LamportClock_;

    const IInvokerPtr BackgroundInvoker_;

    IYPathServicePtr OrchidService_;

    TEntityMap<TCellMailbox> CellMailboxMap_;
    TEntityMap<TAvenueMailbox> AvenueMailboxMap_;

    struct THiveRuntimeData
    {
        bool Available = false;
        THashMap<TCellId, TCellMailboxRuntimeDataPtr> CellIdToCellRuntimeData;
        THashMap<TEndpointId, TAvenueMailboxRuntimeDataPtr> EndpointIdToAvenueRuntimeData;
    };

    NThreading::TAtomicObject<THiveRuntimeData> RuntimeData_;

    THashSet<TCellId> UnregisteredCellIds_;

    NThreading::TAtomicObject<std::vector<THiveEdge>> FrozenEdges_;

    TPeriodicExecutorPtr ReadOnlyCheckExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, Ping)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto srcCellId = FromProto<TCellId>(request->src_cell_id());

        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v",
            srcCellId,
            SelfCellId_);

        HydraManager_->ValidatePeer(EPeerKind::Leader);

        auto runtimeData = FindCellMailboxRuntimeData(srcCellId, /*throwIfUnavailable*/ true);
        auto lastOutcomingMessageId = runtimeData
            ? std::optional(runtimeData->PersistentState->GetOutcomingMessageIdRange().End - 1)
            : std::nullopt;

        if (lastOutcomingMessageId.has_value()) {
            response->set_last_outcoming_message_id(*lastOutcomingMessageId);
        }

        context->SetResponseInfo("LastOutcomingMessageId: %v",
            lastOutcomingMessageId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, SyncCells)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        context->SetRequestInfo();

        ValidatePeer(EPeerKind::LeaderOrFollower);
        SyncWithUpstream();

        context->ReplyFrom(
            BIND([=, this, this_ = MakeStrong(this)] {
                auto knownCells = FromProto<std::vector<TCellInfo>>(request->known_cells());
                auto syncResult = CellDirectory_->Synchronize(knownCells);

                for (const auto& request : syncResult.ReconfigureRequests) {
                    YT_LOG_DEBUG("Requesting cell reconfiguration (CellId: %v, ConfigVersion: %v -> %v)",
                        request.NewDescriptor->CellId,
                        request.OldConfigVersion,
                        request.NewDescriptor->ConfigVersion);
                    auto* protoInfo = response->add_cells_to_reconfigure();
                    ToProto(protoInfo->mutable_cell_descriptor(), *request.NewDescriptor);
                }

                for (const auto& request : syncResult.UnregisterRequests) {
                    YT_LOG_DEBUG("Requesting cell unregistration (CellId: %v)",
                        request.CellId);
                    auto* unregisterInfo = response->add_cells_to_unregister();
                    ToProto(unregisterInfo->mutable_cell_id(), request.CellId);
                }
            })
                .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
                .Run());
    }

    bool DoPostMessages(
        const NHiveClient::NProto::TReqPostMessages* request,
        NHiveClient::NProto::TRspPostMessages* response)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto srcEndpointId = FromProto<TCellId>(request->src_endpoint_id());
        auto firstMessageId = request->first_message_id();
        int messageCount = request->messages_size();

        auto runtimeData = FindMailboxRuntimeData(srcEndpointId, /*throwIfUnavailable*/ true);
        if (!runtimeData) {
            if (IsAvenueEndpointType(TypeFromId(srcEndpointId))) {
                YT_LOG_DEBUG(
                    "Received a message to a non-registered avenue mailbox "
                    "(SrcEndpointId: %v, DstEndpointId: %v)",
                    srcEndpointId,
                    GetSiblingAvenueEndpointId(srcEndpointId));
            } else {
                YT_LOG_DEBUG(
                    "Received a message to a non-registered cell mailbox "
                    "(SrcCellId: %v, DstCellId: %v)",
                    srcEndpointId,
                    SelfCellId_);
            }
            response->set_next_transient_incoming_message_id(-1);
            return false;
        }

        YT_VERIFY(runtimeData->NextTransientIncomingMessageId >= 0);

        bool shouldCommit = runtimeData->NextTransientIncomingMessageId == firstMessageId && messageCount > 0;
        if (shouldCommit) {
            YT_LOG_DEBUG("Committing reliable incoming messages (%v, "
                "MessageIds: %v-%v)",
                FormatIncomingMailboxEndpoints(srcEndpointId),
                firstMessageId,
                firstMessageId + messageCount - 1);

            runtimeData->NextTransientIncomingMessageId += messageCount;
        }

        response->set_next_transient_incoming_message_id(runtimeData->NextTransientIncomingMessageId);
        response->set_next_persistent_incoming_message_id(runtimeData->PersistentState->GetNextPersistentIncomingMessageId());

        return shouldCommit;
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, PostMessages)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto srcCellId = FromProto<TCellId>(request->src_endpoint_id());
        auto firstMessageId = request->first_message_id();
        int messageCount = request->messages_size();
        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v, MessageIds: %v-%v",
            srcCellId,
            SelfCellId_,
            firstMessageId,
            firstMessageId + messageCount - 1);

        ValidatePeer(EPeerKind::Leader);

        if (IsEdgeFrozen({.SourceCellId = srcCellId, .DestinationCellId = SelfCellId_})) {
            context->Reply(TError("Edge is frozen; not posting messages along it")
                << TErrorAttribute("self_cell_id", SelfCellId_)
                << TErrorAttribute("src_endpoint_id", srcCellId));
            return;
        }

        auto cellRuntimeData = FindCellMailboxRuntimeData(srcCellId, /*throwIfUnavailable*/ true);
        if (!cellRuntimeData) {
            NHiveServer::NProto::TReqRegisterMailbox hydraRequest;
            ToProto(hydraRequest.mutable_cell_id(), srcCellId);
            YT_UNUSED_FUTURE(CreateMutation(HydraManager_, hydraRequest)
                ->CommitAndLog(Logger));

            THROW_ERROR_EXCEPTION(
                NHiveClient::EErrorCode::MailboxNotCreatedYet,
                "Mailbox %v is not created yet",
                srcCellId);
        }

        bool shouldCommit = false;

        auto handleRequest = [&] (auto* request, auto* response) {
            if (DoPostMessages(request, response)) {
                shouldCommit = true;
            } else {
                request->clear_messages();
            }
        };

        handleRequest(request, response);
        for (auto& subrequest : *request->mutable_avenue_subrequests()) {
            handleRequest(&subrequest, response->add_avenue_subresponses());
        }

        if (shouldCommit) {
            YT_UNUSED_FUTURE(CreatePostMessagesMutation(*request)
                ->CommitAndLog(Logger));
        }

        auto nextPersistentIncomingMessageId = cellRuntimeData->PersistentState->GetNextPersistentIncomingMessageId();
        auto nextTransientIncomingMessageId = cellRuntimeData->NextTransientIncomingMessageId;
        context->SetResponseInfo("NextPersistentIncomingMessageId: %v, NextTransientIncomingMessageId: %v",
            nextPersistentIncomingMessageId,
            nextTransientIncomingMessageId);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, SendMessages)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto srcCellId = FromProto<TCellId>(request->src_cell_id());
        int messageCount = request->messages_size();

        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v, MessageCount: %v",
            srcCellId,
            SelfCellId_,
            messageCount);

        ValidatePeer(EPeerKind::Leader);

        YT_LOG_DEBUG("Committing unreliable incoming messages (SrcCellId: %v, DstCellId: %v, "
            "MessageCount: %v)",
            srcCellId,
            SelfCellId_,
            messageCount);

        auto mutation = CreateSendMessagesMutation(context);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, SyncWithOthers)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto srcCellIds = FromProto<std::vector<TCellId>>(request->src_cell_ids());

        context->SetRequestInfo("SrcCellIds: %v",
            srcCellIds);

        ValidatePeer(EPeerKind::Leader);

        std::vector<TFuture<void>> asyncResults;
        for (auto cellId : srcCellIds) {
            asyncResults.push_back(SyncWith(cellId, true));
        }

        context->ReplyFrom(AllSucceeded(asyncResults));
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, GetConsistentState)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto requestTime = request->has_logical_time()
            ? std::optional<TLogicalTime>(request->logical_time())
            : std::nullopt;

        context->SetRequestInfo("LogicalTime: %v",
            requestTime);

        auto [logicalTime, state] = LogicalTimeRegistry_->GetConsistentState(requestTime);
        response->set_logical_time(logicalTime.Underlying());
        response->set_sequence_number(state.SequenceNumber);
        response->set_segment_id(state.SegmentId);

        context->SetResponseInfo("StateLogicalTime: %v, StateSequenceNumber: %v, StateSegmentId: %v",
            logicalTime,
            state.SequenceNumber,
            state.SegmentId);
        context->Reply();
    }

    // Hydra handlers.

    void HydraAcknowledgeMessages(NHiveServer::NProto::TReqAcknowledgeMessages* request)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TCellId>(request->cell_id());
        auto* mailbox = AsTyped(FindMailbox(cellId));
        if (!mailbox) {
            return;
        }

        if (!IsRecovery()) {
            BackgroundInvoker_->Invoke(
                BIND([this, this_ = MakeStrong(this), runtimeData = mailbox->GetRuntimeData()] {
                    Y_UNUSED(this);
                    YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

                    runtimeData->AcknowledgeInProgress = false;
                }));
        }

        auto nextPersistentIncomingMessageId = request->next_persistent_incoming_message_id();
        auto outcomingMessageIdRange = mailbox->GetPersistentState()->GetOutcomingMessageIdRange();
        auto oldFirstOutcomingMessageId = outcomingMessageIdRange.Begin;
        auto acknowledgeCount = nextPersistentIncomingMessageId - oldFirstOutcomingMessageId;
        if (acknowledgeCount <= 0) {
            YT_LOG_DEBUG("No messages acknowledged (%v, "
                "NextPersistentIncomingMessageId: %v, FirstOutcomingMessageId: %v)",
                FormatOutgoingMailboxEndpoints(cellId),
                nextPersistentIncomingMessageId,
                outcomingMessageIdRange.Begin);
            return;
        }

        if (acknowledgeCount > outcomingMessageIdRange.GetCount()) {
            YT_LOG_ALERT("Requested to acknowledge too many messages (%v, "
                "NextPersistentIncomingMessageId: %v, FirstOutcomingMessageId: %v, OutcomingMessageCount: %v)",
                FormatOutgoingMailboxEndpoints(cellId),
                nextPersistentIncomingMessageId,
                outcomingMessageIdRange.Begin,
                outcomingMessageIdRange.GetCount());
            return;
        }

        auto newFirstOutcomingMessageId = mailbox->GetPersistentState()->TrimLastestOutcomingMessages(acknowledgeCount);
        YT_LOG_DEBUG("Messages acknowledged (%v, FirstOutcomingMessageId: %v -> %v)",
            FormatOutgoingMailboxEndpoints(cellId),
            oldFirstOutcomingMessageId,
            newFirstOutcomingMessageId);

        if (IsLeader() && mailbox->IsAvenue()) {
            BackgroundInvoker_->Invoke(
                BIND([this, this_ = MakeStrong(this), avenueRuntimeData = mailbox->AsAvenue()->GetRuntimeData()] {
                    YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

                    UpdateAvenueCellActiveness(avenueRuntimeData);
                }));
        }
    }

    void HydraPostMessages(NHiveClient::NProto::TReqPostMessages* request)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto srcCellId = FromProto<TCellId>(request->src_endpoint_id());

        ValidateCellNotUnregistered(srcCellId);

        auto firstMessageId = request->first_message_id();
        auto* mailbox = AsTyped(FindMailbox(srcCellId));
        if (!mailbox) {
            if (firstMessageId != 0) {
                YT_LOG_ALERT("Received a non-initial message to a missing mailbox (SrcCellId: %v, MessageId: %v)",
                    srcCellId,
                    firstMessageId);
                return;
            }
            mailbox = AsTyped(CreateCellMailbox(srcCellId));
        }

        ApplyReliableIncomingMessages(mailbox, request);

        for (const auto& subrequest : request->avenue_subrequests()) {
            auto srcId = FromProto<TAvenueEndpointId>(subrequest.src_endpoint_id());
            auto* mailbox = AsTyped(FindMailbox(srcId));
            if (!mailbox) {
                YT_LOG_INFO(
                    "Received a message to a missing avenue mailbox (SrcId: %v, MessageId: %v)",
                    srcId,
                    firstMessageId);
                continue;
            }
            ApplyReliableIncomingMessages(mailbox, &subrequest);
        }
    }

    void HydraSendMessages(
        const TCtxSendMessagesPtr& /*context*/,
        NHiveClient::NProto::TReqSendMessages* request,
        NHiveClient::NProto::TRspSendMessages* /*response*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto srcCellId = FromProto<TCellId>(request->src_cell_id());
        auto* mailbox = AsTyped(GetMailboxOrThrow(srcCellId))->AsCell();
        ApplyUnreliableIncomingMessages(mailbox, request);
    }

    void HydraRegisterMailbox(NHiveServer::NProto::TReqRegisterMailbox* request)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TCellId>(request->cell_id());
        if (UnregisteredCellIds_.contains(cellId)) {
            YT_LOG_INFO("Mailbox is already unregistered (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                cellId);
            return;
        }

        GetOrCreateCellMailbox(cellId);
    }

    void HydraUnregisterMailbox(NHiveServer::NProto::TReqUnregisterMailbox* request)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TCellId>(request->cell_id());
        Y_UNUSED(TryUnregisterCellMailbox(cellId));
    }

    IChannelPtr FindMailboxChannel(const TCellMailboxRuntimeDataPtr& runtimeData)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto now = GetCpuInstant();
        if (runtimeData->CachedChannel && now < runtimeData->CachedChannelDeadline) {
            return runtimeData->CachedChannel;
        }

        auto channel = CellDirectory_->FindChannelByCellId(runtimeData->EndpointId);
        if (!channel) {
            return nullptr;
        }

        runtimeData->CachedChannel = channel;
        runtimeData->CachedChannelDeadline = now + DurationToCpuDuration(Config_->CachedChannelTimeout);

        return channel;
    }

    void ReliablePostMessage(TRange<TMailboxHandle> mailboxes, const TSerializedMessagePtr& message)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        TStringBuilder logMessageBuilder;
        logMessageBuilder.AppendFormat("Reliable outcoming message added (MutationType: %v, SrcCellId: %v, DstIds: {",
            message->Type,
            SelfCellId_);

        auto* traceContext = NTracing::TryGetCurrentTraceContext();

        auto* mutationContext = TryGetCurrentMutationContext();

        auto logicalTime = LamportClock_->GetTime();
        if (mutationContext) {
            logicalTime = LamportClock_->Tick();
            mutationContext->CombineStateHash(message->Type, message->Data);
        }

        for (auto mailboxHandle : mailboxes) {
            auto* mailbox = AsTyped(mailboxHandle);
            auto messageId = mailbox->GetPersistentState()->AddOutcomingMessage(TOutcomingMessage{
                .SerializedMessage = message,
                .TraceContext = traceContext,
                .Time = logicalTime,
            });

            if (mutationContext) {
                mutationContext->CombineStateHash(messageId, mailbox->GetEndpointId());
            }

            if (mailbox != AsTyped(mailboxes.Front())) {
                logMessageBuilder.AppendString(TStringBuf(", "));
            }
            logMessageBuilder.AppendFormat("%v=>%v",
                mailbox->GetEndpointId(),
                messageId);

            if (IsLeader()) {
                if (mailbox->IsAvenue()) {
                    BackgroundInvoker_->Invoke(BIND([this, this_ = MakeStrong(this), avenueRuntimeData = mailbox->AsAvenue()->GetRuntimeData()] {
                        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

                        UpdateAvenueCellActiveness(avenueRuntimeData);
                        if (auto cellRuntimeData = avenueRuntimeData->Cell.Lock()) {
                            SchedulePostCellOutcomingMessages(cellRuntimeData);
                        }
                    }));
                } else {
                    BackgroundInvoker_->Invoke(BIND([this, this_ = MakeStrong(this), cellRuntimeData = mailbox->AsCell()->GetRuntimeData()] {
                        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

                        SchedulePostCellOutcomingMessages(cellRuntimeData);
                    }));
                }
            }
        }

        if (mutationContext) {
            logMessageBuilder.AppendFormat("}, LogicalTime: %v, SequenceNumber: %v)",
                logicalTime,
                mutationContext->GetSequenceNumber());
        }
        YT_LOG_DEBUG(logMessageBuilder.Flush());
    }

    void UnreliablePostMessage(TRange<TMailboxHandle> mailboxes, const TSerializedMessagePtr& message)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(!HasHydraContext());

        TStringBuilder logMessageBuilder;
        logMessageBuilder.AppendFormat("Sending unreliable outcoming message (MutationType: %v, SrcCellId: %v, DstCellIds: [",
            message->Type,
            SelfCellId_);

        std::vector<TCellMailboxRuntimeDataPtr> cellRuntimeDatas;
        for (auto mailboxHandle : mailboxes) {
            auto* mailbox = AsTyped(mailboxHandle);
            if (!mailbox->IsCell()) {
                YT_LOG_ALERT("Attempt to post unreliable message to a non-cell mailbox (EndpointId: %v)",
                    mailbox->GetEndpointId());
                continue;
            }

            auto* cellMailbox = mailbox->AsCell();
            const auto& cellRuntimeData = cellMailbox->GetRuntimeData();
            if (!cellRuntimeData->Connected.load()) {
                continue;
            }

            if (cellMailbox != AsTyped(mailboxes.Front())) {
                logMessageBuilder.AppendString(TStringBuf(", "));
            }
            logMessageBuilder.AppendFormat("%v", cellMailbox->GetCellId());

            cellRuntimeDatas.push_back(cellRuntimeData);
        }

        logMessageBuilder.AppendString(TStringBuf("])"));
        YT_LOG_DEBUG(logMessageBuilder.Flush());

        BackgroundInvoker_->Invoke(
            BIND(&THiveManager::DoUnreliablePostMessage, MakeStrong(this), std::move(cellRuntimeDatas), message));
    }

    void DoUnreliablePostMessage(
        const std::vector<TCellMailboxRuntimeDataPtr>& cellRuntimeDatas,
        const TSerializedMessagePtr& message)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto traceContext = MakeStrong(TryGetCurrentTraceContext());
        for (const auto& cellRuntimeData : cellRuntimeDatas) {
            if (!cellRuntimeData->Connected.load()) {
                return;
            }

            auto channel = FindMailboxChannel(cellRuntimeData);
            if (!channel) {
                return;
            }

            THiveServiceProxy proxy(std::move(channel));

            auto req = proxy.SendMessages();
            req->SetTimeout(Config_->SendRpcTimeout);
            ToProto(req->mutable_src_cell_id(), SelfCellId_);
            auto* protoMessage = req->add_messages();
            protoMessage->set_type(message->Type);
            protoMessage->set_data(message->Data);
            if (traceContext) {
                ToProto(
                    protoMessage->mutable_tracing_ext(),
                    traceContext,
                    Config_->SendTracingBaggage);
            }

            req->Invoke().Subscribe(
                BIND(&THiveManager::OnSendMessagesResponse, MakeStrong(this), MakeWeak(cellRuntimeData))
                    .Via(BackgroundInvoker_));
        }
    }

    bool IsEdgeFrozen(const THiveEdge& edge)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return FrozenEdges_.Read([&] (const auto& frozenEdges) {
            return std::find(frozenEdges.begin(), frozenEdges.end(), edge) != frozenEdges.end();
        });
    }

    void SetMailboxConnected(const TCellMailboxRuntimeDataPtr& cellRuntimeData)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        if (cellRuntimeData->Connected.load()) {
            return;
        }

        ++cellRuntimeData->ConnectionEpoch;
        cellRuntimeData->Connected.store(true);
        YT_VERIFY(cellRuntimeData->SyncRequests.empty());

        auto doSetConnected = [&] (const TMailboxRuntimeDataPtr& runtimeData) {
            runtimeData->FirstInFlightOutcomingMessageId = runtimeData->PersistentState->GetOutcomingMessageIdRange().Begin;
            YT_VERIFY(runtimeData->InFlightOutcomingMessageCount == 0);

            YT_LOG_INFO("Mailbox connected (%v, FirstInFlightOutcomingMessageId: %v)",
                FormatOutgoingMailboxEndpoints(runtimeData->EndpointId),
                runtimeData->FirstInFlightOutcomingMessageId);
        };

        doSetConnected(cellRuntimeData);
        for (const auto& avenueRuntimeData : cellRuntimeData->RegisteredAvenues) {
            doSetConnected(avenueRuntimeData);
        }

        if (cellRuntimeData->IsLeader) {
            PostOutcomingMessages(cellRuntimeData, /*allowIdle*/ true);
        }
    }

    void MaybeDisconnectMailboxOnError(const TCellMailboxRuntimeDataPtr& cellRuntimeData, const TError& error)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        if (error.FindMatching(NHydra::EErrorCode::ReadOnly)) {
            return;
        }

        SetMailboxDisconnected(cellRuntimeData->EndpointId, error);
    }

    void SetMailboxDisconnected(TEndpointId endpointId, const TError& error)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        if (auto cellRuntimeData = FindCellMailboxRuntimeData(endpointId, /*throwIfUnavailable*/ false)) {
            SetMailboxDisconnected(cellRuntimeData, error);
        }
    }

    void SetMailboxDisconnected(const TCellMailboxRuntimeDataPtr& cellRuntimeData, const TError& error)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        if (!cellRuntimeData->Connected.exchange(false)) {
            return;
        }

        auto wrappedError = TError(
            NRpc::EErrorCode::Unavailable,
            "Failed to synchronize with cell %v since it has disconnected",
            cellRuntimeData->EndpointId)
            << error;

        {
            NTracing::TNullTraceContextGuard guard;
            for (const auto& [_, promise] : std::exchange(cellRuntimeData->SyncRequests, {})) {
                promise.Set(wrappedError);
            }
        }

        cellRuntimeData->SyncBatcher->Cancel(wrappedError);

        auto doSetDisconnected = [&] (const TMailboxRuntimeDataPtr& runtimeData) {
            runtimeData->PostInProgress = false;
            ++runtimeData->ConnectionEpoch;
            runtimeData->FirstInFlightOutcomingMessageId.reset();
            runtimeData->InFlightOutcomingMessageCount = 0;

            YT_LOG_INFO("Mailbox disconnected (%v)",
                FormatOutgoingMailboxEndpoints(runtimeData->EndpointId));
        };

        doSetDisconnected(cellRuntimeData);
        for (const auto& avenueRuntimeData : cellRuntimeData->RegisteredAvenues) {
            doSetDisconnected(avenueRuntimeData);
        }

        TDelayedExecutor::CancelAndClear(cellRuntimeData->IdlePostCookie);
    }

    TCellMailboxRuntimeDataPtr CreateMailboxRuntimeData(TCellMailbox* mailbox)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(!IsRecovery());

        auto synchronizationBatcher = New<TAsyncBatcher<void>>(
            BIND_NO_PROPAGATE(&THiveManager::DoSyncWithThunk, MakeWeak(this), mailbox->GetCellId()),
            Config_->SyncDelay);
        auto cellRuntimeData = New<TCellMailboxRuntimeData>(
            Profiler_,
            IsLeader(),
            mailbox->GetEndpointId(),
            mailbox->GetPersistentState(),
            std::move(synchronizationBatcher));
        mailbox->SetRuntimeData(cellRuntimeData);
        return cellRuntimeData;
    }

    TAvenueMailboxRuntimeDataPtr CreateMailboxRuntimeData(TAvenueMailbox* mailbox)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(!IsRecovery());

        auto avenueRuntimeData = New<TAvenueMailboxRuntimeData>(
            IsLeader(),
            mailbox->GetEndpointId(),
            mailbox->GetPersistentState());
        mailbox->SetRuntimeData(avenueRuntimeData);
        return avenueRuntimeData;
    }

    void InitializeRuntimeData()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        THiveRuntimeData newRuntimeData{.Available = true};
        auto secondaryMasterCellIds = MasterDirectory_->GetSecondaryMasterCellIds();
        std::vector<TCellId> secondaryMasterCellsForRemoval;
        secondaryMasterCellsForRemoval.reserve(secondaryMasterCellIds.size());
        for (const auto& [cellId, _] : CellMailboxMap_) {
            if (TypeFromId(cellId) == EObjectType::MasterCell) {
                if (!secondaryMasterCellIds.contains(cellId) && cellId != MasterDirectory_->GetPrimaryMasterCellId()) {
                    secondaryMasterCellsForRemoval.push_back(cellId);
                }
            }
        }

        // NB: Avenues are not removed here, since we can delete only cells without chunks and tablets on it, so without avenue channels to tablets.
        for (auto cellId : secondaryMasterCellsForRemoval) {
            if (Config_->AllowedForRemovalMasterCells.contains(CellTagFromId(cellId))) {
                CellMailboxMap_.Release(cellId);
            }
        }

        for (auto [cellId, mailbox] : CellMailboxMap_) {
            auto cellRuntimeData = CreateMailboxRuntimeData(mailbox);
            EmplaceOrCrash(newRuntimeData.CellIdToCellRuntimeData, cellId, cellRuntimeData);
        }

        std::vector<TAvenueMailboxRuntimeDataPtr> avenueRuntimeDataList;
        for (auto [endpointId, mailbox] : AvenueMailboxMap_) {
            auto avenueRuntimeData = CreateMailboxRuntimeData(mailbox);
            avenueRuntimeDataList.push_back(avenueRuntimeData);
            EmplaceOrCrash(newRuntimeData.EndpointIdToAvenueRuntimeData, endpointId, avenueRuntimeData);
        }

        RuntimeData_.Store(std::move(newRuntimeData));

        BackgroundInvoker_->Invoke(
            BIND([this, this_ = MakeStrong(this), avenueRuntimeDataList = std::move(avenueRuntimeDataList)] {
                YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

                for (const auto& avenueRuntimeData : avenueRuntimeDataList) {
                    UpdateAvenueCellConnection(avenueRuntimeData);
                    UpdateAvenueCellActiveness(avenueRuntimeData);
                }
            }));

        ReadOnlyCheckExecutor_ = New<TPeriodicExecutor>(
            EpochAutomatonInvoker_,
            BIND_NO_PROPAGATE(&THiveManager::OnReadOnlyCheck, MakeWeak(this)),
            Config_->ReadOnlyCheckPeriod);
        ReadOnlyCheckExecutor_->Start();
    }

    void FinalizeRuntimeData()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        // Prevent stale background activities from seeing any more updates to persistent state.
        for (auto [_, mailbox] : CellMailboxMap_) {
            mailbox->RecreatePersistentState();
        }
        for (auto [_, mailbox] : AvenueMailboxMap_) {
            mailbox->RecreatePersistentState();
        }

        auto oldRuntimeData = RuntimeData_.Exchange(THiveRuntimeData{});

        BackgroundInvoker_->Invoke(
            BIND([this, this_ = MakeStrong(this), oldRuntimeData = std::move(oldRuntimeData)] {
                YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

                auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
                for (const auto& [_, cellRuntimeData] : oldRuntimeData.CellIdToCellRuntimeData) {
                    SetMailboxDisconnected(cellRuntimeData, error);
                }
            }));

        ReadOnlyCheckExecutor_.Reset();
    }

    void ValidateCellNotUnregistered(TCellId cellId)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (UnregisteredCellIds_.contains(cellId)) {
            THROW_ERROR_EXCEPTION("Cell %v is unregistered",
                cellId);
        }
    }

    void StartPeriodicPings()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        std::vector<TCellMailboxRuntimeDataPtr> cellRuntimeDataList;
        for (auto [_, mailbox] : CellMailboxMap_) {
            cellRuntimeDataList.push_back(mailbox->GetRuntimeData());
        }

        BackgroundInvoker_->Invoke(
            BIND_NO_PROPAGATE([this, this_ = MakeStrong(this), cellRuntimeDataList = std::move(cellRuntimeDataList)] {
                YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

                for (const auto& cellRuntimeData : cellRuntimeDataList) {
                    RunPeriodicCellCheck(cellRuntimeData);
                }
            }));
    }

    void SchedulePeriodicCellCheck(const TCellMailboxRuntimeDataPtr& cellRuntimeData)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        TDelayedExecutor::Submit(
            BIND_NO_PROPAGATE(&THiveManager::RunPeriodicCellCheckThunk, MakeWeak(this), MakeWeak(cellRuntimeData))
                .Via(BackgroundInvoker_),
            Config_->PingPeriod);
    }

    void RunPeriodicCellCheckThunk(const TWeakPtr<TCellMailboxRuntimeData>& weakCellRuntimeData)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        if (auto cellRuntimeData = weakCellRuntimeData.Lock()) {
            RunPeriodicCellCheck(cellRuntimeData);
        }
    }

    void RunPeriodicCellCheck(const TCellMailboxRuntimeDataPtr& cellRuntimeData)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto cellId = cellRuntimeData->EndpointId;
        if (IsActiveLeader() && CellDirectory_->IsCellUnregistered(cellId)) {
            // This cell is known (via Cell Directory) to be globally unregistered.
            // Let's stop all ping activities and attempt to unregister it locally from Hive Manager state.
            NHiveServer::NProto::TReqUnregisterMailbox req;
            ToProto(req.mutable_cell_id(), cellId);
            YT_UNUSED_FUTURE(CreateUnregisterMailboxMutation(req)
                ->CommitAndLog(Logger));
            return;
        }

        if (cellRuntimeData->Connected.load()) {
            // This cell mailbox is currently connected.
            // We do not ping connected cells, so let's just schedule another check at later time.
            SchedulePeriodicCellCheck(cellRuntimeData);
            return;
        }

        auto channel = FindMailboxChannel(cellRuntimeData);
        if (!channel) {
            // No endpoint is currently known for this cell mailbox.
            // Let's register a dummy descriptor so as to ask about it during the next Cell Directory sync.
            // Meanwhile, we just back off and wait for another ping check.
            CellDirectory_->RegisterCell(cellId);
            SchedulePeriodicCellCheck(cellRuntimeData);
            return;
        }

        // The cell has a known endpoint but its mailbox is not connected, hence we ping it.
        YT_LOG_DEBUG("Sending periodic ping (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            cellId);

        THiveServiceProxy proxy(std::move(channel));
        auto req = proxy.Ping();
        req->SetTimeout(Config_->PingRpcTimeout);
        ToProto(req->mutable_src_cell_id(), SelfCellId_);
        req->Invoke().Subscribe(
            BIND(&THiveManager::OnCellPingResponse, MakeStrong(this), cellRuntimeData)
                .Via(BackgroundInvoker_));
    }

    void OnCellPingResponse(
        const TWeakPtr<TCellMailboxRuntimeData>& weakCellRuntimeData,
        const THiveServiceProxy::TErrorOrRspPingPtr& rspOrError)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto cellRuntimeData = weakCellRuntimeData.Lock();
        if (!cellRuntimeData) {
            return;
        }

        // Schedule a new check anyway.
        SchedulePeriodicCellCheck(cellRuntimeData);

        if (!rspOrError.IsOK()) {
            // Ping failed -- the cell mailbox will remain disconnected.
            YT_LOG_DEBUG(rspOrError, "Periodic ping failed (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                cellRuntimeData->EndpointId);
            return;
        }

        // Ping succeeded -- let's make the cell mailbox connected.
        const auto& rsp = rspOrError.Value();
        auto lastOutcomingMessageId = rsp->last_outcoming_message_id();

        YT_LOG_DEBUG("Periodic ping succeeded (SrcCellId: %v, DstCellId: %v, LastOutcomingMessageId: %v)",
            SelfCellId_,
            cellRuntimeData->EndpointId,
            lastOutcomingMessageId);

        SetMailboxConnected(cellRuntimeData);
    }

    void CheckRuntimeDataAvailability(const THiveRuntimeData& runtimeData, bool throwIfUnavailable)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (throwIfUnavailable && !runtimeData.Available) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Peer is not active");
        }
    }

    TCellMailboxRuntimeDataPtr FindCellMailboxRuntimeData(TCellId cellId, bool throwIfUnavailable)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return RuntimeData_.Read([&] (const auto& runtimeData) {
            CheckRuntimeDataAvailability(runtimeData, throwIfUnavailable);
            return GetOrDefault(runtimeData.CellIdToCellRuntimeData, cellId);
        });
    }

    TAvenueMailboxRuntimeDataPtr FindAvenueMailboxRuntimeData(TEndpointId endpointId, bool throwIfUnavailable)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return RuntimeData_.Read([&] (const auto& runtimeData) {
            CheckRuntimeDataAvailability(runtimeData, throwIfUnavailable);
            return GetOrDefault(runtimeData.EndpointIdToAvenueRuntimeData, endpointId);
        });
    }

    TMailboxRuntimeDataPtr FindMailboxRuntimeData(TEndpointId endpointId, bool throwIfUnavailable)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (IsAvenueEndpointType(TypeFromId(endpointId))) {
            return FindAvenueMailboxRuntimeData(endpointId, throwIfUnavailable);
        } else {
            return FindCellMailboxRuntimeData(endpointId, throwIfUnavailable);
        }
    }

    static TFuture<void> DoSyncWithThunk(const TWeakPtr<THiveManager>& weakThis, TCellId cellId)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto this_ = weakThis.Lock();
        if (!this_) {
            return MakeFuture(TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped"));
        }

        return this_->DoSyncWith(cellId);
    }

    TFuture<void> DoSyncWith(TCellId cellId)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        try {
            auto channel = CellDirectory_->FindChannelByCellId(cellId, EPeerKind::Leader);
            if (!channel) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Cannot synchronize with cell %v since it is not connected",
                    cellId);
            }

            auto cellRuntimeData = FindCellMailboxRuntimeData(cellId, /*throwIfUnavailable*/ true);
            if (!cellRuntimeData) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Cannot synchronize with cell %v since it is not known",
                    cellId);
            }

            YT_LOG_DEBUG("Synchronizing with another instance (SrcCellId: %v, DstCellId: %v)",
                cellId,
                SelfCellId_);

            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            TWallTimer timer;

            THiveServiceProxy proxy(std::move(channel));
            auto req = proxy.Ping();
            req->SetTimeout(Config_->PingRpcTimeout);
            ToProto(req->mutable_src_cell_id(), SelfCellId_);
            return req->Invoke()
                .Apply(
                    BIND(&THiveManager::OnSyncPingResponse, MakeStrong(this), cellId)
                        .AsyncVia(BackgroundInvoker_))
                .WithTimeout(Config_->SyncTimeout)
                // NB: Many subscribers are typically waiting for the sync to complete.
                // Make sure the promise is set in a large thread pool.
                .Apply(
                    BIND([cellRuntimeData, timer = std::move(timer)] (const TError& error) {
                        cellRuntimeData->SyncTimeGauge.Update(timer.GetElapsedTime());
                        error.ThrowOnError();
                    })
                        .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
        } catch (const std::exception& ex) {
            return MakeFuture<void>(ex);
        }
    }

    TFuture<void> OnSyncPingResponse(
        TCellId cellId,
        const THiveServiceProxy::TErrorOrRspPingPtr& rspOrError)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        try {
            auto cellRuntimeData = FindCellMailboxRuntimeData(cellId, /*throwIfUnavailable*/ true);
            if (!cellRuntimeData) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Mailbox of cell %v is no longer available",
                    cellId);
            }

            if (!rspOrError.IsOK()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Failed to synchronize with cell %v",
                    cellId)
                    << rspOrError;
            }

            if (!cellRuntimeData->Connected.load()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Unable to synchronize with cell %v since it is not connected",
                    cellId);
            }

            const auto& rsp = rspOrError.Value();
            if (!rsp->has_last_outcoming_message_id()) {
                YT_LOG_DEBUG("Remote instance has no mailbox; no synchronization needed (SrcCellId: %v, DstCellId: %v)",
                    cellId,
                    SelfCellId_);
                return VoidFuture;
            }

            auto messageId = rsp->last_outcoming_message_id();
            auto nextPersistentIncomingMessageId = cellRuntimeData->PersistentState->GetNextPersistentIncomingMessageId();
            if (messageId < nextPersistentIncomingMessageId) {
                YT_LOG_DEBUG("Already synchronized with remote instance (SrcCellId: %v, DstCellId: %v, "
                    "SyncMessageId: %v, NextPersistentIncomingMessageId: %v)",
                    cellId,
                    SelfCellId_,
                    messageId,
                    nextPersistentIncomingMessageId);
                return VoidFuture;
            }

            YT_LOG_DEBUG("Waiting for synchronization with remote instance (SrcCellId: %v, DstCellId: %v, "
                "SyncMessageId: %v, NextPersistentIncomingMessageId: %v)",
                cellId,
                SelfCellId_,
                messageId,
                nextPersistentIncomingMessageId);

            auto it = cellRuntimeData->SyncRequests.find(messageId);
            if (it != cellRuntimeData->SyncRequests.end()) {
                return it->second.ToFuture();
            }

            auto promise = NewPromise<void>();
            EmplaceOrCrash(cellRuntimeData->SyncRequests, messageId, promise);
            return promise.ToFuture();
        } catch (const std::exception& ex) {
            return MakeFuture<void>(ex);
        }
    }

    void FlushSyncRequests(const TCellMailboxRuntimeDataPtr& cellRuntimeData)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto nextPersistentIncomingMessageId = cellRuntimeData->PersistentState->GetNextPersistentIncomingMessageId();
        while (!cellRuntimeData->SyncRequests.empty()) {
            auto it = cellRuntimeData->SyncRequests.begin();
            auto messageId = it->first;
            if (messageId >= nextPersistentIncomingMessageId) {
                break;
            }

            YT_LOG_DEBUG("Synchronization complete (SrcCellId: %v, DstCellId: %v, MessageId: %v)",
                SelfCellId_,
                cellRuntimeData->EndpointId,
                messageId);

            it->second.Set();
            cellRuntimeData->SyncRequests.erase(it);
        }
    }

    void SchedulePostAvenueOutcomingMessages(const TAvenueMailboxRuntimeDataPtr& avenueRuntimeData)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto cellRuntimeData = avenueRuntimeData->Cell.Lock();
        if (!cellRuntimeData) {
            return;
        }

        SchedulePostCellOutcomingMessages(cellRuntimeData);
    }

    void SchedulePostCellOutcomingMessages(const TCellMailboxRuntimeDataPtr& cellRuntimeData)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        if (!cellRuntimeData->IsLeader) {
            return;
        }
        if (cellRuntimeData->PostBatchingCookie) {
            return;
        }

        cellRuntimeData->PostBatchingCookie = TDelayedExecutor::Submit(
            BIND_NO_PROPAGATE([=, this, weakThis = MakeWeak(this), weakCellRuntimeData = MakeWeak(cellRuntimeData)] {
                auto this_ = weakThis.Lock();
                if (!this_) {
                    return;
                }

                YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

                auto cellRuntimeData = weakCellRuntimeData.Lock();
                if (!cellRuntimeData) {
                    return;
                }

                cellRuntimeData->PostBatchingCookie.Reset();
                PostOutcomingMessages(cellRuntimeData, /*allowIdle*/ false);
            }).Via(BackgroundInvoker_),
            Config_->PostBatchingPeriod);
    }

    struct TAvenuePostInfo
    {
        TAvenueEndpointId SrcEndpointId;
        TMailboxConnectionEpoch Epoch;
    };

    void PostOutcomingMessagesThunk(const TWeakPtr<TCellMailboxRuntimeData>& weakCellRuntimeData, bool allowIdle)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        if (auto cellRuntimeData = weakCellRuntimeData.Lock()) {
            PostOutcomingMessages(cellRuntimeData, allowIdle);
        }
    }

    void PostOutcomingMessages(const TCellMailboxRuntimeDataPtr& cellRuntimeData, bool allowIdle)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        if (!cellRuntimeData->IsLeader) {
            return;
        }
        if (!cellRuntimeData->Connected.load()) {
            return;
        }
        if (cellRuntimeData->InFlightOutcomingMessageCount > 0) {
            return;
        }
        if (cellRuntimeData->PostInProgress) {
            return;
        }

        NTracing::TNullTraceContextGuard nullTraceContextGuard;

        bool hasOutcomingMessages = false;
        auto checkOutcomingMessages = [&] (const TMailboxRuntimeDataPtr& runtimeData) {
            if (runtimeData->InFlightOutcomingMessageCount > 0) {
                return;
            }

            YT_VERIFY(runtimeData->FirstInFlightOutcomingMessageId);
            auto firstInFlightOutcomingMessageId = *runtimeData->FirstInFlightOutcomingMessageId;
            auto outcomingMessageIdRange = runtimeData->PersistentState->GetOutcomingMessageIdRange();
            YT_VERIFY(firstInFlightOutcomingMessageId <= outcomingMessageIdRange.End);
            if (firstInFlightOutcomingMessageId < outcomingMessageIdRange.End) {
                hasOutcomingMessages = true;
            }
        };

        checkOutcomingMessages(cellRuntimeData);
        for (const auto& avenueRuntimeData : cellRuntimeData->ActiveAvenues) {
            checkOutcomingMessages(avenueRuntimeData);
        }

        TDelayedExecutor::CancelAndClear(cellRuntimeData->IdlePostCookie);
        if (!allowIdle && !hasOutcomingMessages) {
            // Typically we do not send PostMessages requests without any payload.
            // However, since outcoming messages are only trimmed when their delivery is
            // confirmed by the receipient (see |next_persistent_incoming_message_id|),
            // we must still send empty ("idle") PostMessage periodically.
            cellRuntimeData->IdlePostCookie = TDelayedExecutor::Submit(
                BIND_NO_PROPAGATE(&THiveManager::PostOutcomingMessagesThunk, MakeStrong(this), MakeWeak(cellRuntimeData), /*allowidle*/ true)
                    .Via(BackgroundInvoker_),
                Config_->IdlePostPeriod);
            return;
        }

        auto channel = FindMailboxChannel(cellRuntimeData);
        if (!channel) {
            return;
        }

        struct TEnvelope
        {
            TEndpointId SrcEndpointId;
            i64 FirstMessageId;
            std::vector<TOutcomingMessage> MessagesToPost;
        };

        i64 messageBytesToPost = 0;
        int messageCountToPost = 0;

        auto isOverflown = [&] {
            return
                messageCountToPost >= Config_->MaxMessagesPerPost ||
                messageBytesToPost >= Config_->MaxBytesPerPost;
        };

        auto buildEnvelope = [&] (const TMailboxRuntimeDataPtr& runtimeData, TEndpointId srcEndpointId) {
            YT_VERIFY(runtimeData->FirstInFlightOutcomingMessageId.has_value());
            TEnvelope envelope{
                .SrcEndpointId = srcEndpointId,
                .FirstMessageId = *runtimeData->FirstInFlightOutcomingMessageId,
            };
            runtimeData->PersistentState->IterateOutcomingMessages(
                envelope.FirstMessageId,
                [&] (const TOutcomingMessage& message) {
                    if (isOverflown()) {
                        return false;
                    }
                    envelope.MessagesToPost.push_back(message);
                    messageBytesToPost += message.SerializedMessage->Data.size();
                    messageCountToPost += 1;
                    return true;
                });
            runtimeData->InFlightOutcomingMessageCount = std::ssize(envelope.MessagesToPost);
            runtimeData->PostInProgress = true;
            return envelope;
        };

        std::vector<TEnvelope> avenueEnvelopes;
        std::vector<TAvenuePostInfo> avenuePostInfos;
        for (const auto& avenueRuntimeData : cellRuntimeData->ActiveAvenues) {
            if (avenueRuntimeData->InFlightOutcomingMessageCount > 0) {
                continue;
            }

            auto srcEndpointId = GetSiblingAvenueEndpointId(avenueRuntimeData->EndpointId);
            avenueEnvelopes.push_back(buildEnvelope(avenueRuntimeData, srcEndpointId));
            avenuePostInfos.push_back({.SrcEndpointId = srcEndpointId, .Epoch = avenueRuntimeData->ConnectionEpoch});
        }

        auto cellEnvelope = buildEnvelope(cellRuntimeData, SelfCellId_);

        auto dstCellId = cellRuntimeData->EndpointId;
        if (messageCountToPost == 0) {
            YT_LOG_DEBUG("Checking mailbox synchronization (SrcCellId: %v, DstCellId: %v, AvenueEndpointIds: %v)",
                SelfCellId_,
                dstCellId,
                MakeFormattableView(
                    avenueEnvelopes,
                    [] (auto* builder, const auto& envelope) {
                        builder->AppendFormat("%v", GetSiblingAvenueEndpointId(envelope.SrcEndpointId));
                    }));
        } else {
            YT_LOG_DEBUG("Posting reliable outcoming messages "
                "(SrcCellId: %v, DstCellId: %v, MessageIds: %v-%v, Avenues: %v)",
                SelfCellId_,
                dstCellId,
                cellEnvelope.FirstMessageId,
                cellEnvelope.FirstMessageId + std::ssize(cellEnvelope.MessagesToPost) - 1,
                MakeFormattableView(
                    avenueEnvelopes,
                    [] (auto* builder, const auto& envelope) {
                        builder->AppendFormat("%v:", GetSiblingAvenueEndpointId(envelope.SrcEndpointId));
                        if (envelope.MessagesToPost.empty()) {
                            builder->AppendString("sync");
                        } else {
                            builder->AppendFormat(
                                "%v-%v",
                                envelope.FirstMessageId,
                                envelope.FirstMessageId + ssize(envelope.MessagesToPost) - 1);
                        }
                    }));
        }

        THiveServiceProxy proxy(std::move(channel));
        auto req = proxy.PostMessages();
        req->SetTimeout(Config_->PostRpcTimeout);

        auto fillSubrequest = [&] (auto* req, const auto& envelope) {
            ToProto(req->mutable_src_endpoint_id(), envelope.SrcEndpointId);
            req->set_first_message_id(envelope.FirstMessageId);
            for (const auto& message : envelope.MessagesToPost) {
                auto* protoMessage = req->add_messages();
                protoMessage->set_type(message.SerializedMessage->Type);
                protoMessage->set_data(message.SerializedMessage->Data);
                if (message.TraceContext) {
                    ToProto(
                        protoMessage->mutable_tracing_ext(),
                        message.TraceContext,
                        Config_->SendTracingBaggage);
                }
                protoMessage->set_logical_time(message.Time.Underlying());
            }
        };

        fillSubrequest(req.Get(), cellEnvelope);
        for (const auto& avenueEnvelope : avenueEnvelopes) {
            fillSubrequest(req->add_avenue_subrequests(), avenueEnvelope);
        }

        req->Invoke().Subscribe(
            BIND(
                &THiveManager::OnPostMessagesResponse,
                MakeStrong(this),
                MakeWeak(cellRuntimeData),
                cellRuntimeData->ConnectionEpoch,
                Passed(std::move(avenuePostInfos)))
                .Via(BackgroundInvoker_));
    }

    void OnPostMessagesResponse(
        const TWeakPtr<TCellMailboxRuntimeData>& weakCellRuntimeData,
        TMailboxConnectionEpoch cellEpoch,
        const std::vector<TAvenuePostInfo>& avenuePostInfos,
        const THiveServiceProxy::TErrorOrRspPostMessagesPtr& rspOrError)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto cellRuntimeData = weakCellRuntimeData.Lock();
        if (!cellRuntimeData) {
            return;
        }

        DoPostMessagesResponse(cellRuntimeData, cellEpoch, rspOrError);
        for (const auto& [index, postInfo] : Enumerate(avenuePostInfos)) {
            auto dstEndpointId = GetSiblingAvenueEndpointId(postInfo.SrcEndpointId);
            DoPostMessagesResponse(cellRuntimeData, postInfo.Epoch, rspOrError, dstEndpointId, index);
        }
    }

    void DoPostMessagesResponse(
        const TCellMailboxRuntimeDataPtr& cellRuntimeData,
        TMailboxConnectionEpoch epoch,
        const THiveServiceProxy::TErrorOrRspPostMessagesPtr& rspOrError,
        TAvenueEndpointId dstEndpointId = {},
        std::optional<int> avenueSubrequestIndex = {})
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        TAvenueMailboxRuntimeDataPtr avenueRuntimeData;
        if (avenueSubrequestIndex) {
            avenueRuntimeData = FindAvenueMailboxRuntimeData(dstEndpointId, /*throwIfUnavailable*/ false);
            if (!avenueRuntimeData) {
                return;
            }
        }

        auto runtimeData = avenueRuntimeData
            ? TMailboxRuntimeDataPtr(avenueRuntimeData)
            : TMailboxRuntimeDataPtr(cellRuntimeData);

        if (runtimeData->ConnectionEpoch != epoch) {
            YT_LOG_DEBUG("Ignoring stale post messages response (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                runtimeData->EndpointId);
            return;
        }

        if (!std::exchange(runtimeData->PostInProgress, false)) {
            return;
        }

        runtimeData->InFlightOutcomingMessageCount = 0;

        if (!rspOrError.IsOK()) {
            if (runtimeData == cellRuntimeData) {
                auto dstCellId = cellRuntimeData->EndpointId;
                if (rspOrError.GetCode() == NHiveClient::EErrorCode::MailboxNotCreatedYet) {
                    YT_LOG_DEBUG(rspOrError, "Mailbox is not created yet; will retry (SrcCellId: %v, DstCellId: %v)",
                        SelfCellId_,
                        dstCellId);
                    SchedulePostCellOutcomingMessages(cellRuntimeData);
                } else {
                    YT_LOG_DEBUG(rspOrError, "Failed to post reliable outcoming messages (SrcCellId: %v, DstCellId: %v)",
                        SelfCellId_,
                        dstCellId);
                    MaybeDisconnectMailboxOnError(cellRuntimeData, rspOrError);
                }
            }
            return;
        }

        const auto& rsp = avenueSubrequestIndex
            ? rspOrError.Value()->avenue_subresponses(*avenueSubrequestIndex)
            : *rspOrError.Value();

        if (rsp.next_transient_incoming_message_id() == -1) {
            YT_LOG_DEBUG("Destination mailbox does not exist, post cancelled (%v)",
                FormatOutgoingMailboxEndpoints(runtimeData->EndpointId));
            return;
        }

        auto nextPersistentIncomingMessageId = rsp.next_persistent_incoming_message_id();
        auto nextTransientIncomingMessageId = rsp.next_transient_incoming_message_id();
        YT_LOG_DEBUG("Outcoming reliable messages posted "
            "(%v, NextPersistentIncomingMessageId: %v, NextTransientIncomingMessageId: %v)",
            FormatOutgoingMailboxEndpoints(runtimeData->EndpointId),
            nextPersistentIncomingMessageId,
            nextTransientIncomingMessageId);

        auto outcomingMessageIdRange = runtimeData->PersistentState->GetOutcomingMessageIdRange();
        if (!HandlePersistentIncomingMessages(runtimeData, outcomingMessageIdRange, nextPersistentIncomingMessageId)) {
            return;
        }
        if (!HandleTransientIncomingMessages(runtimeData, outcomingMessageIdRange, nextTransientIncomingMessageId)) {
            return;
        }

        if (avenueRuntimeData) {
            SchedulePostAvenueOutcomingMessages(avenueRuntimeData);
        } else {
            SchedulePostCellOutcomingMessages(cellRuntimeData);
        }
    }

    void OnSendMessagesResponse(
        const TWeakPtr<TCellMailboxRuntimeData>& weakCellRuntimeData,
        const THiveServiceProxy::TErrorOrRspSendMessagesPtr& rspOrError)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto cellRuntimeData = weakCellRuntimeData.Lock();
        if (!cellRuntimeData) {
            return;
        }

        auto dstCellId = cellRuntimeData->EndpointId;

        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Failed to send unreliable outcoming messages (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                dstCellId);
            MaybeDisconnectMailboxOnError(cellRuntimeData, rspOrError);
            return;
        }

        YT_LOG_DEBUG("Outcoming unreliable messages sent successfully (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            dstCellId);
    }

    std::unique_ptr<TMutation> CreateAcknowledgeMessagesMutation(const NHiveServer::NProto::TReqAcknowledgeMessages& req)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return CreateMutation(
            HydraManager_,
            req,
            &THiveManager::HydraAcknowledgeMessages,
            this);
    }

    std::unique_ptr<TMutation> CreatePostMessagesMutation(const NHiveClient::NProto::TReqPostMessages& request)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return CreateMutation(
            HydraManager_,
            request,
            &THiveManager::HydraPostMessages,
            this);
    }

    std::unique_ptr<TMutation> CreateSendMessagesMutation(const TCtxSendMessagesPtr& context)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        return CreateMutation(
            HydraManager_,
            context,
            &THiveManager::HydraSendMessages,
            this);
    }

    std::unique_ptr<TMutation> CreateRegisterMailboxMutation(const NHiveServer::NProto::TReqRegisterMailbox& req)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return CreateMutation(
            HydraManager_,
            req,
            &THiveManager::HydraRegisterMailbox,
            this);
    }

    std::unique_ptr<TMutation> CreateUnregisterMailboxMutation(const NHiveServer::NProto::TReqUnregisterMailbox& req)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return CreateMutation(
            HydraManager_,
            req,
            &THiveManager::HydraUnregisterMailbox,
            this);
    }

    bool CheckRequestedMessageIdAgainstMailbox(
        const TMailboxRuntimeDataPtr& runtimeData,
        const TMessageIdRange& outcomingMessageIdRange,
        TMessageId requestedMessageId)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        if (requestedMessageId < outcomingMessageIdRange.Begin) {
            YT_LOG_WARNING("Destination is out of sync: requested to receive already truncated messages (%v, "
                "RequestedMessageId: %v, FirstOutcomingMessageId: %v)",
                FormatOutgoingMailboxEndpoints(runtimeData->EndpointId),
                requestedMessageId,
                outcomingMessageIdRange.Begin);
            auto error = TError("Destination is out of sync: requested to receive already truncated messages");
            SetMailboxDisconnected(runtimeData->EndpointId, error);
            return false;
        }

        if (requestedMessageId > outcomingMessageIdRange.End) {
            YT_LOG_WARNING("Destination is out of sync: requested to receive nonexisting messages (%v, "
                "RequestedMessageId: %v, FirstOutcomingMessageId: %v, OutcomingMessageCount: %v)",
                FormatOutgoingMailboxEndpoints(runtimeData->EndpointId),
                requestedMessageId,
                outcomingMessageIdRange.Begin,
                outcomingMessageIdRange.GetCount());
            auto error = TError("Destination is out of sync: requested to receive nonexisting messages");
            SetMailboxDisconnected(runtimeData->EndpointId, error);
            return false;
        }

        return true;
    }

    bool HandlePersistentIncomingMessages(
        const TMailboxRuntimeDataPtr& runtimeData,
        const TMessageIdRange& outcomingMessageIdRange,
        TMessageId nextPersistentIncomingMessageId)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        if (!CheckRequestedMessageIdAgainstMailbox(runtimeData, outcomingMessageIdRange, nextPersistentIncomingMessageId)) {
            return false;
        }

        auto firstOutcomingMessageId = outcomingMessageIdRange.Begin;
        if (nextPersistentIncomingMessageId == firstOutcomingMessageId) {
            return true;
        }

        if (std::exchange(runtimeData->AcknowledgeInProgress, true)) {
            return true;
        }

        YT_LOG_DEBUG("Committing reliable messages acknowledgement (%v, "
            "MessageIds: %v-%v)",
            FormatOutgoingMailboxEndpoints(runtimeData->EndpointId),
            firstOutcomingMessageId,
            nextPersistentIncomingMessageId - 1);

        NHiveServer::NProto::TReqAcknowledgeMessages req;
        ToProto(req.mutable_cell_id(), runtimeData->EndpointId);
        req.set_next_persistent_incoming_message_id(nextPersistentIncomingMessageId);
        YT_UNUSED_FUTURE(CreateAcknowledgeMessagesMutation(req)
            ->CommitAndLog(Logger));

        return true;
    }

    bool HandleTransientIncomingMessages(
        const TMailboxRuntimeDataPtr& runtimeData,
        const TMessageIdRange& outcomingMessageIdRange,
        TMessageId nextTransientIncomingMessageId)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        if (!CheckRequestedMessageIdAgainstMailbox(runtimeData, outcomingMessageIdRange, nextTransientIncomingMessageId)) {
            return false;
        }

        YT_LOG_TRACE("Updating first in-flight outcoming message id (%v, MessageId: %v -> %v)",
            FormatOutgoingMailboxEndpoints(runtimeData->EndpointId),
            runtimeData->FirstInFlightOutcomingMessageId,
            nextTransientIncomingMessageId);
        runtimeData->FirstInFlightOutcomingMessageId = nextTransientIncomingMessageId;
        return true;
    }

    void ApplyReliableIncomingMessages(TMailbox* mailbox, const NHiveClient::NProto::TReqPostMessages* req)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        for (int index = 0; index < req->messages_size(); ++index) {
            if (mailbox->IsAvenue()) {
                YT_VERIFY(!mailbox->AsAvenue()->IsRemovalScheduled());
            }

            auto messageId = req->first_message_id() + index;
            ApplyReliableIncomingMessage(mailbox, messageId, req->messages(index));

            // Avenue endpoint was unregistered within current mutation,
            // now the time has come to destroy it.
            if (mailbox->IsAvenue() && mailbox->AsAvenue()->IsRemovalScheduled()) {
                DoUnregisterAvenueEndpoint(mailbox->AsAvenue());
            }
        }
    }

    void ApplyReliableIncomingMessage(TMailbox* mailbox, TMessageId messageId, const TEncapsulatedMessage& message)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto nextPersistentIncomingMessageId = mailbox->GetPersistentState()->GetNextPersistentIncomingMessageId();
        if (messageId != nextPersistentIncomingMessageId) {
            YT_LOG_ALERT("Attempt to apply an out-of-order message; ignored (%v, "
                "ExpectedMessageId: %v, ActualMessageId: %v, MutationType: %v)",
                FormatIncomingMailboxEndpoints(mailbox->GetEndpointId()),
                nextPersistentIncomingMessageId,
                messageId,
                message.type());
            return;
        }

        std::optional<TTraceContextGuard> traceContextGuard;
        if (message.has_tracing_ext() && IsLeader()) {
            auto traceContext = NTracing::TTraceContext::NewChildFromRpc(
                message.tracing_ext(),
                ConcatToString(TStringBuf("HiveManager:"), message.type()));
            traceContextGuard.emplace(std::move(traceContext));
        }
        auto logicalTime = LamportClock_->Tick(TLogicalTime(message.logical_time()));

        auto* mutationContext = GetCurrentMutationContext();
        YT_LOG_DEBUG("Applying reliable incoming message (%v, "
            "MessageId: %v, MutationType: %v, LogicalTime: %v, SequenceNumber: %v)",
            FormatIncomingMailboxEndpoints(mailbox->GetEndpointId()),
            messageId,
            message.type(),
            logicalTime,
            mutationContext->GetSequenceNumber());

        ApplyMessage(message, mailbox->GetEndpointId());

        mailbox->GetPersistentState()->SetNextPersistentIncomingMessageId(messageId + 1);

        if (!IsRecovery() && mailbox->IsCell()) {
            BackgroundInvoker_->Invoke(
                BIND_NO_PROPAGATE(&THiveManager::FlushSyncRequests, MakeStrong(this), mailbox->AsCell()->GetRuntimeData()));
        }
    }

    void ApplyUnreliableIncomingMessages(TCellMailbox* mailbox, const NHiveClient::NProto::TReqSendMessages* req)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        for (const auto& message : req->messages()) {
            ApplyUnreliableIncomingMessage(mailbox, message);
        }
    }

    void ApplyUnreliableIncomingMessage(TCellMailbox* mailbox, const TEncapsulatedMessage& message)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        YT_LOG_DEBUG("Applying unreliable incoming message (SrcCellId: %v, DstCellId: %v, MutationType: %v)",
            mailbox->GetCellId(),
            SelfCellId_,
            message.type());
        ApplyMessage(message, mailbox->GetCellId());
    }

    void ApplyMessage(const TEncapsulatedMessage& message, TEndpointId endpointId)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMutationRequest request{
            .Reign = GetCurrentMutationContext()->Request().Reign,
            .Type = message.type(),
            .Data = TSharedRef::FromString(message.data())
         };

        TMutationContext mutationContext(GetCurrentMutationContext(), &request);
        TMutationContextGuard mutationContextGuard(&mutationContext);

        THiveMutationGuard hiveMutationGuard(endpointId);

        static_cast<IAutomaton*>(Automaton_)->ApplyMutation(&mutationContext);
    }

    void OnAvenueDirectoryEndpointUpdated(TAvenueEndpointId endpointId, TCellId cellId)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        if (auto avenueRuntimeData = FindAvenueMailboxRuntimeData(endpointId, /*throwIfUnavailable*/ false)) {
            auto cellRuntimeData = FindCellMailboxRuntimeData(cellId, /*throwIfUnavailable*/ false);
            UpdateAvenueCellConnection(avenueRuntimeData, cellRuntimeData);
        }
    }

    void UpdateAvenueCellConnection(const TAvenueMailboxRuntimeDataPtr& avenueRuntimeData)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto cellId = AvenueDirectory_->FindCellIdByEndpointId(avenueRuntimeData->EndpointId);
        auto cellRuntimeData = cellId ? FindCellMailboxRuntimeData(cellId, /*throwIfUnavailable*/ false) : nullptr;
        UpdateAvenueCellConnection(avenueRuntimeData, cellRuntimeData);
    }

    void UpdateAvenueCellConnection(
        const TAvenueMailboxRuntimeDataPtr& avenueRuntimeData,
        const TCellMailboxRuntimeDataPtr& cellRuntimeData)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto formerCellRuntimeData = avenueRuntimeData->Cell.Lock();
        if (cellRuntimeData == formerCellRuntimeData) {
            return;
        }

        auto avenueOutcomingMessageIdRange = avenueRuntimeData->PersistentState->GetOutcomingMessageIdRange();
        avenueRuntimeData->PostInProgress = false;
        ++avenueRuntimeData->ConnectionEpoch;
        avenueRuntimeData->FirstInFlightOutcomingMessageId = avenueOutcomingMessageIdRange.Begin;
        avenueRuntimeData->InFlightOutcomingMessageCount = 0;

        if (formerCellRuntimeData) {
            EraseOrCrash(formerCellRuntimeData->RegisteredAvenues, avenueRuntimeData);
            formerCellRuntimeData->ActiveAvenues.erase(avenueRuntimeData);
            YT_LOG_DEBUG(
                "Avenue disconnected from cell (AvenueEndpointId: %v, CellId: %v)",
                avenueRuntimeData->EndpointId,
                formerCellRuntimeData ? formerCellRuntimeData->EndpointId : NullObjectId);
        }

        if (cellRuntimeData && !avenueOutcomingMessageIdRange.IsEmpty()) {
            if (cellRuntimeData->ActiveAvenues.insert(avenueRuntimeData).second) {
                YT_LOG_DEBUG(
                    "Active avenue registered at cell (AvenueEndpointId: %v, CellId: %v)",
                    avenueRuntimeData->EndpointId,
                    cellRuntimeData->EndpointId);
            }

            if (cellRuntimeData->IsLeader) {
                SchedulePostCellOutcomingMessages(cellRuntimeData);
            }
        }

        avenueRuntimeData->Cell = cellRuntimeData;

        if (cellRuntimeData) {
            EmplaceOrCrash(cellRuntimeData->RegisteredAvenues, avenueRuntimeData);
            YT_LOG_DEBUG("Avenue connected to cell (AvenueEndpointId: %v, CellId: %v)",
                avenueRuntimeData->EndpointId,
                cellRuntimeData->EndpointId);
        }
    }

    void UpdateAvenueCellActiveness(const TAvenueMailboxRuntimeDataPtr& avenueRuntimeData)
    {
        YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

        auto cellRuntimeData = avenueRuntimeData->Cell.Lock();
        if (!cellRuntimeData) {
            return;
        }

        auto outcomingMessageIdRange = avenueRuntimeData->PersistentState->GetOutcomingMessageIdRange();
        bool active = !outcomingMessageIdRange.IsEmpty();

        if (active) {
            if (cellRuntimeData->ActiveAvenues.insert(avenueRuntimeData).second) {
                YT_LOG_DEBUG(
                    "Avenue became active at cell (AvenueEndpointId: %v, CellId: %v)",
                    avenueRuntimeData->EndpointId,
                    cellRuntimeData->EndpointId);
            }
        } else {
            if (cellRuntimeData->ActiveAvenues.erase(avenueRuntimeData) != 0) {
                YT_LOG_DEBUG(
                    "Avenue became inactive at cell (AvenueEndpointId: %v, CellId: %v)",
                    avenueRuntimeData->EndpointId,
                    cellRuntimeData->EndpointId);
            }
        }
    }

    TString FormatIncomingMailboxEndpoints(TEndpointId endpointId) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return FormatMailboxEndpoints(endpointId, /*outgoing*/ false);
    }

    TString FormatOutgoingMailboxEndpoints(TEndpointId endpointId) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return FormatMailboxEndpoints(endpointId, /*outgoing*/ true);
    }

    TString FormatMailboxEndpoints(TEndpointId endpointId, bool outgoing) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto type = TypeFromId(endpointId);
        if (IsCellType(type)) {
            auto srcId = SelfCellId_;
            auto dstId = endpointId;

            if (!outgoing) {
                std::swap(srcId, dstId);
            }

            return Format("SrcCellId: %v, DstCellId: %v", srcId, dstId);
        } else if (IsAvenueEndpointType(type)) {
            auto dstId = endpointId;
            auto srcId = GetSiblingAvenueEndpointId(dstId);

            if (!outgoing) {
                std::swap(srcId, dstId);
            }

            return Format("SrcEndpointId: %v, DstEndpointId: %v", srcId, dstId);
        } else {
            YT_ABORT();
        }
    }

    void OnReadOnlyCheck()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (!HydraManager_->GetReadOnly()) {
            return;
        }

        std::vector<TCellMailboxRuntimeDataPtr> cellRuntimeDatas;
        cellRuntimeDatas.reserve(CellMailboxMap_.size());
        for (auto [_, mailbox] : CellMailboxMap_) {
            cellRuntimeDatas.push_back(mailbox->GetRuntimeData());
        }

        BackgroundInvoker_->Invoke(
            BIND([this, this_ = MakeStrong(this), cellRuntimeDatas = std::move(cellRuntimeDatas)] {
                YT_ASSERT_INVOKER_AFFINITY(BackgroundInvoker_);

                YT_LOG_DEBUG("Hydra is read-only; canceling all synchronization requests");
                auto error = TError(NHydra::EErrorCode::ReadOnly, "Cannot synchronize with remote instance since Hydra is read-only");
                for (const auto& cellRuntimeData : cellRuntimeDatas) {
                    SetMailboxDisconnected(cellRuntimeData, error);
                }
            }));
    }

    void OnLeaderRecoveryComplete() override
    {
        TCompositeAutomatonPart::OnLeaderRecoveryComplete();
        InitializeRuntimeData();
    }

    void OnLeaderActive() override
    {
        TCompositeAutomatonPart::OnLeaderActive();

        // NB: Leader must wait until it is active before reconnecting mailboxes
        // since no commits are possible before this point.
        StartPeriodicPings();
    }

    void OnStopLeading() override
    {
        TCompositeAutomatonPart::OnStopLeading();
        FinalizeRuntimeData();
    }

    void OnFollowerRecoveryComplete() override
    {
        TCompositeAutomatonPart::OnFollowerRecoveryComplete();
        InitializeRuntimeData();
        StartPeriodicPings();
    }

    void OnStopFollowing() override
    {
        TCompositeAutomatonPart::OnStopFollowing();
        FinalizeRuntimeData();
    }

    bool ValidateSnapshotVersion(int version) override
    {
        return version == 5 ||
            version == 6 || // COMPAT(ifsmirnov): Avenues.
            version == 7 ||
            false;
    }

    int GetCurrentSnapshotVersion() override
    {
        return 7;
    }

    void Clear() override
    {
        TCompositeAutomatonPart::Clear();

        CellMailboxMap_.Clear();
        AvenueMailboxMap_.Clear();
        UnregisteredCellIds_.clear();

        RuntimeData_.Store(THiveRuntimeData{});
    }

    void SaveKeys(TSaveContext& context) const
    {
        CellMailboxMap_.SaveKeys(context);
        AvenueMailboxMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        CellMailboxMap_.SaveValues(context);
        AvenueMailboxMap_.SaveValues(context);
        Save(context, UnregisteredCellIds_);
        Save(context, *LamportClock_);
    }

    void LoadKeys(TLoadContext& context)
    {
        CellMailboxMap_.LoadKeys(context);
        // COMPAT(ifsmirnov)
        if (context.GetVersion() >= 6) {
            AvenueMailboxMap_.LoadKeys(context);
        }
    }

    void LoadValues(TLoadContext& context)
    {
        CellMailboxMap_.LoadValues(context);
        // COMPAT(ifsmirnov)
        if (context.GetVersion() >= 6) {
            AvenueMailboxMap_.LoadValues(context);
        }
        Load(context, UnregisteredCellIds_);
        // COMPAT(danilalexeev)
        if (context.GetVersion() >= 7) {
            Load(context, *LamportClock_);
        }
    }

    int GetUnregisteredCellIdCount() const
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        return std::ssize(UnregisteredCellIds_);
    }

    TLogicalTime GetLamportTimestamp() const
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        return LamportClock_->GetTime();
    }

    IYPathServicePtr CreateOrchidService()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto automatonInvoker = HydraManager_->CreateGuardedAutomatonInvoker(AutomatonInvoker_);
        return New<TCompositeMapService>()
            ->AddChild("cell_mailboxes", New<TMailboxOrchidService<TCellMailboxRuntimeData, &THiveRuntimeData::CellIdToCellRuntimeData>>(
                MakeWeak(this))
                ->Via(BackgroundInvoker_))
            ->AddChild("avenue_mailboxes", New<TMailboxOrchidService<TAvenueMailboxRuntimeData, &THiveRuntimeData::EndpointIdToAvenueRuntimeData>>(
                MakeWeak(this))
                ->Via(BackgroundInvoker_))
            ->AddChild("unregistered_cell_count", IYPathService::FromMethod(
                &THiveManager::GetUnregisteredCellIdCount,
                MakeWeak(this))
                ->Via(automatonInvoker))
            ->AddChild("lamport_timestamp", IYPathService::FromMethod(
                &THiveManager::GetLamportTimestamp,
                MakeWeak(this))
                ->Via(automatonInvoker));
    }

    template <
        class TRuntimeData,
        THashMap<TEndpointId, TIntrusivePtr<TRuntimeData>> THiveRuntimeData::* RuntimeDataMap
    >
    class TMailboxOrchidService
        : public TVirtualMapBase
    {
    public:
        using TRuntimeDataPtr = TIntrusivePtr<TRuntimeData>;

        explicit TMailboxOrchidService(TWeakPtr<THiveManager> hiveManager)
            : Owner_(std::move(hiveManager))
        { }

        std::vector<std::string> GetKeys(i64 limit) const override
        {
            YT_ASSERT_THREAD_AFFINITY_ANY();

            auto owner = Owner_.Lock();
            if (!owner) {
                return {};
            }

            return owner->RuntimeData_.Read([&] (const auto& runtimeData) {
                const auto& runtimeDataMap = runtimeData.*RuntimeDataMap;

                std::vector<std::string> keys;
                keys.reserve(std::min(limit, std::ssize(runtimeDataMap)));
                for (const auto& [_, runtimeData] : runtimeDataMap) {
                    if (std::ssize(keys) >= limit) {
                        break;
                    }
                    keys.push_back(ToString(runtimeData->EndpointId));
                }
                return keys;
            });
        }

        i64 GetSize() const override
        {
            YT_ASSERT_THREAD_AFFINITY_ANY();

            auto owner = Owner_.Lock();
            if (!owner) {
                return 0;
            }

            return owner->RuntimeData_.Read([&] (const auto& runtimeData) {
                const auto& runtimeDataMap = runtimeData.*RuntimeDataMap;
                return std::ssize(runtimeDataMap);
            });
        }

        IYPathServicePtr FindItemService(const std::string& key) const override
        {
            YT_ASSERT_THREAD_AFFINITY_ANY();

            TEndpointId id;
            if (!TEndpointId::FromString(key, &id)) {
                return nullptr;
            }

            auto owner = Owner_.Lock();
            if (!owner) {
                return nullptr;
            }

            YT_ASSERT_INVOKER_AFFINITY(owner->BackgroundInvoker_);

            return owner->RuntimeData_.Read([&] (const auto& runtimeData) -> INodePtr {
                const auto& runtimeDataMap = runtimeData.*RuntimeDataMap;
                auto it = runtimeDataMap.find(id);
                if (it == runtimeDataMap.end()) {
                    return nullptr;
                }

                auto producer = BIND(&THiveManager::BuildMailboxOrchidYson<TRuntimeDataPtr>, owner, it->second);
                return ConvertToNode(producer);
            });
        }

    private:
        const TWeakPtr<THiveManager> Owner_;
    };

    template <class TMailboxRuntimeDataPtr>
    void BuildMailboxOrchidYson(const TMailboxRuntimeDataPtr& runtimeData, IYsonConsumer* consumer) const
    {
        auto outcomingMessageIdRange = runtimeData->PersistentState->GetOutcomingMessageIdRange();
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("acknowledge_in_progress").Value(runtimeData->AcknowledgeInProgress)
                .Item("post_in_progress").Value(runtimeData->PostInProgress)
                .Item("first_outcoming_message_id").Value(outcomingMessageIdRange.Begin)
                .Item("outcoming_message_count").Value(outcomingMessageIdRange.End - outcomingMessageIdRange.Begin)
                .Item("next_persistent_incoming_message_id").Value(runtimeData->PersistentState->GetNextPersistentIncomingMessageId())
                .Item("next_transient_incoming_message_id").Value(runtimeData->NextTransientIncomingMessageId)
                .Item("first_in_flight_outcoming_message_id").Value(runtimeData->FirstInFlightOutcomingMessageId)
                .Item("in_flight_outcoming_message_count").Value(runtimeData->InFlightOutcomingMessageCount)
                .Do([&] (auto fluent) {
                    BuildTypeSpecificMailboxOrchidYson(runtimeData, fluent);
                })
            .EndMap();
    }

    static void BuildTypeSpecificMailboxOrchidYson(const TCellMailboxRuntimeDataPtr& cellRuntimeData, TFluentMap fluent)
    {
        fluent
            .Item("connected").Value(cellRuntimeData->Connected.load())
            .Item("registered_avenue_ids").DoListFor(
                cellRuntimeData->RegisteredAvenues,
                [] (auto fluent, const TAvenueMailboxRuntimeDataPtr& avenueRuntimeData) {
                    fluent.Item().Value(avenueRuntimeData->EndpointId);
                })
            .Item("active_avenue_ids").DoListFor(
                cellRuntimeData->ActiveAvenues,
                [] (auto fluent, const TAvenueMailboxRuntimeDataPtr& avenueRuntimeData) {
                    fluent.Item().Value(avenueRuntimeData->EndpointId);
                });
    }

    static void BuildTypeSpecificMailboxOrchidYson(const TAvenueMailboxRuntimeDataPtr& avenueRuntimeData, TFluentMap fluent)
    {
        auto cellRuntimeData = avenueRuntimeData->Cell.Lock();
        fluent
            .DoIf(static_cast<bool>(cellRuntimeData), [&] (auto fluent) {
                fluent
                    .Item("cell_id").Value(cellRuntimeData->EndpointId);
            });
    }

    static TMailbox* AsTyped(TMailboxHandle handle)
    {
        return static_cast<TMailbox*>(handle.Underlying());
    }

    static TMailboxHandle AsUntyped(TMailbox* mailbox)
    {
        return TMailboxHandle(mailbox);
    }
};

////////////////////////////////////////////////////////////////////////////////

IHiveManagerPtr CreateHiveManager(
    THiveManagerConfigPtr config,
    ICellDirectoryPtr cellDirectory,
    NCellMasterClient::ICellDirectoryPtr masterDirectory,
    IAvenueDirectoryPtr avenueDirectory,
    TCellId selfCellId,
    IInvokerPtr automatonInvoker,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    IUpstreamSynchronizerPtr upstreamSynchronizer,
    IAuthenticatorPtr authenticator)
{
    return New<THiveManager>(
        std::move(config),
        std::move(cellDirectory),
        std::move(masterDirectory),
        std::move(avenueDirectory),
        selfCellId,
        std::move(automatonInvoker),
        std::move(hydraManager),
        std::move(automaton),
        std::move(upstreamSynchronizer),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer::NV2
