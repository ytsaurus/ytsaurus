#include "transaction_replication_session.h"

#include "private.h"
#include "transaction_manager.h"
#include "transaction_presence_cache.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multi_phase_cell_sync_session.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/lib/hydra/mutation_context.h>
#include <yt/yt/server/lib/hydra/persistent_response_keeper.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/range_formatters.h>

#include <yt/yt/core/rpc/service.h>
#include <yt/yt/core/rpc/response_keeper.h>

#include <util/generic/algorithm.h>

namespace NYT::NTransactionServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NRpc;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

TInitiatorRequestLogInfo::TInitiatorRequestLogInfo(
    TRequestId requestId,
    int subrequestIndex)
    : RequestId(requestId)
    , SubrequestIndex(subrequestIndex)
{ }

void FormatValue(TStringBuilderBase* builder, TInitiatorRequestLogInfo logInfo, TStringBuf /*spec*/)
{
    if (logInfo.SubrequestIndex == -1) {
        builder->AppendFormat("%v", logInfo.RequestId);
    } else {
        builder->AppendFormat("%v[%v]", logInfo.RequestId, logInfo.SubrequestIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

TTransactionReplicationSessionBase::TTransactionReplicationSessionBase(
    TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds,
    const TInitiatorRequestLogInfo& logInfo)
    : Bootstrap_(bootstrap)
    , InitiatorRequest_(logInfo)
    , TransactionIds_(std::move(transactionIds))
{
    InitRemoteTransactions();
    InitReplicationRequestCellTags();
}

void TTransactionReplicationSessionBase::Reset(std::vector<TTransactionId> transactionIds)
{
    auto oldTransactionIds = TransactionIds_;
    SortUnique(oldTransactionIds);
    SortUnique(transactionIds);

    // NB: the whole point of having a dedicated Reset method is this optimization.
    if (oldTransactionIds == transactionIds) {
        return;
    }

    TransactionIds_ = std::move(transactionIds);
    InitRemoteTransactions();
    InitReplicationRequestCellTags();
}

void TTransactionReplicationSessionBase::InitRemoteTransactions()
{
    SortUnique(TransactionIds_);

    TransactionIds_.erase(
        std::remove(TransactionIds_.begin(), TransactionIds_.end(), TTransactionId{}),
        TransactionIds_.end());

    ValidateTransactionCellTags();

    // Stability isn't really a requirement, but it's nice to keep remote transactions sorted.
    auto localTransactionBegin = std::stable_partition(
        TransactionIds_.begin(),
        TransactionIds_.end(),
        [&] (TTransactionId transactionId) {
            return IsTransactionRemote(transactionId);
        });

    RemoteTransactionIds_ = MakeRange(TransactionIds_.data(), &*localTransactionBegin);

    UnsyncedLocalTransactionCells_.clear();
    for (auto it = localTransactionBegin; it != TransactionIds_.end(); ++it) {
        auto localTransactionId = *it;
        UnsyncedLocalTransactionCells_.push_back(CellTagFromId(localTransactionId));
    }
    SortUnique(UnsyncedLocalTransactionCells_);
}

void TTransactionReplicationSessionBase::ValidateTransactionCellTags() const
{
    const auto& connection = Bootstrap_->GetClusterConnection();
    const auto primaryCellTag = connection->GetPrimaryMasterCellTag();
    const auto& secondaryCellTags = connection->GetSecondaryMasterCellTags();

    auto isKnownCellTag = [&] (TCellTag cellTag) {
        if (cellTag == primaryCellTag) {
            return true;
        }

        if (std::find(secondaryCellTags.begin(), secondaryCellTags.end(), cellTag) != secondaryCellTags.end()) {
            return true;
        }

        return false;
    };

    for (auto transactionId : TransactionIds_) {
        auto cellTag = CellTagFromId(transactionId);
        if (!isKnownCellTag(cellTag)) {
            THROW_ERROR_EXCEPTION("Unknown transaction cell tag")
                << TErrorAttribute("transaction_id", transactionId)
                << TErrorAttribute("cell_tag", cellTag);
        }
    }
}

void TTransactionReplicationSessionBase::InitReplicationRequestCellTags()
{
    ReplicationRequestCellTags_.clear();
    ReplicationRequestCellTags_.reserve(RemoteTransactionIds_.size());
    std::transform(
        RemoteTransactionIds_.begin(),
        RemoteTransactionIds_.end(),
        std::back_inserter(ReplicationRequestCellTags_),
        &CellTagFromId);
    SortUnique(ReplicationRequestCellTags_);
}

TCellTagList TTransactionReplicationSessionBase::GetCellTagsToSyncWithBeforeInvocation() const
{
    // TODO(shakurov): support tx coordinator decommissioning.
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    auto transactionCoordinatorCells = multicellManager->GetRoleMasterCells(EMasterCellRole::TransactionCoordinator);
    YT_VERIFY(std::is_sorted(transactionCoordinatorCells.begin(), transactionCoordinatorCells.end()));
    auto exTransactionCoordinatorCells = multicellManager->GetRoleMasterCells(EMasterCellRole::ExTransactionCoordinator);
    YT_VERIFY(std::is_sorted(exTransactionCoordinatorCells.begin(), exTransactionCoordinatorCells.end()));

    TCellTagList result;
    std::set_union(
        transactionCoordinatorCells.begin(),
        transactionCoordinatorCells.end(),
        exTransactionCoordinatorCells.begin(),
        exTransactionCoordinatorCells.end(),
        std::back_inserter(result));

    TCellTagList buffer;
    std::set_difference(
        result.begin(),
        result.end(),
        ReplicationRequestCellTags_.begin(),
        ReplicationRequestCellTags_.end(),
        std::back_inserter(buffer));

    YT_VERIFY(std::is_sorted(UnsyncedLocalTransactionCells_.begin(), UnsyncedLocalTransactionCells_.end()));

    result.clear();
    std::set_union(
        buffer.begin(),
        buffer.end(),
        UnsyncedLocalTransactionCells_.begin(),
        UnsyncedLocalTransactionCells_.end(),
        std::back_inserter(result));

    result.erase(
        std::remove(result.begin(), result.end(), Bootstrap_->GetCellTag()),
        result.end());

    return result;
}

std::vector<TRequestId> TTransactionReplicationSessionBase::DoConstructReplicationRequests()
{
    YT_VERIFY(ReplicationRequests_.empty());

    const auto& multicellManager = Bootstrap_->GetMulticellManager();

    std::vector<TRequestId> result;
    auto createReplicationRequest = [&] (TCellTag cellTag) {
        auto channel = multicellManager->GetMasterChannelOrThrow(cellTag, EPeerKind::Leader);
        TTransactionServiceProxy proxy(std::move(channel));
        auto request = proxy.ReplicateTransactions();
        request->set_destination_cell_tag(ToProto<int>(multicellManager->GetCellTag()));

        result.push_back(request->GetRequestId());

        return request;
    };

    auto createOrUpdateReplicationRequest = [&] (TTransactionId transactionId, TReplicationRequestMap* requestMap) {
        auto cellTag = CellTagFromId(transactionId);
        auto& request = (*requestMap)[cellTag];
        if (!request) {
            request = createReplicationRequest(cellTag);
        }
        ToProto(request->add_transaction_ids(), transactionId);
    };

    for (auto transactionId : RemoteTransactionIds_) {
        createOrUpdateReplicationRequest(transactionId, &ReplicationRequests_);
    }

    YT_VERIFY(ReplicationRequests_.size() == ReplicationRequestCellTags_.size());

    return result;
}

bool TTransactionReplicationSessionBase::IsTransactionRemote(TTransactionId transactionId) const
{
    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    const auto& transactionPresenceCache = transactionManager->GetTransactionPresenceCache();

    switch (transactionPresenceCache->GetTransactionPresence(transactionId)) {
        case ETransactionPresence::None:
            return true;

        case ETransactionPresence::Replicated:
        case ETransactionPresence::RecentlyFinished:
            return false;

        case ETransactionPresence::Unknown:
            LogAndThrowUnknownTransactionPresenceError(transactionId);
            break;

        default:
            YT_ABORT();
    }
}

void TTransactionReplicationSessionBase::LogAndThrowUnknownTransactionPresenceError(TTransactionId transactionId) const
{
    YT_LOG_DEBUG("Cannot reliably check transaction presence; probably current epoch ended; the request will be dropped (Request: %v, TransactionId: %v)",
        InitiatorRequest_,
        transactionId);

    // TODO(shakurov): a more specific error code?
    THROW_ERROR_EXCEPTION(NYT::NRpc::EErrorCode::Unavailable, "Cannot reliably check transaction presence; probably current epoch ended; the request will be dropped")
        << TErrorAttribute("request_id", InitiatorRequest_.RequestId)
        << TErrorAttribute("transaction_id", transactionId);
}

std::vector<TFuture<TTransactionServiceProxy::TRspReplicateTransactionsPtr>> TTransactionReplicationSessionBase::DoInvokeReplicationRequests()
{
    ConstructReplicationRequests();

    YT_VERIFY(ReplicationRequestCellTags_.size() == ReplicationRequests_.size());

    std::vector<TFuture<TRspReplicateTransactionsPtr>> asyncResults;
    asyncResults.reserve(ReplicationRequests_.size());
    // NB: process requests in deterministic order because response handler counts on it.
    for (auto cellTag : ReplicationRequestCellTags_) {
        auto it = ReplicationRequests_.find(cellTag);
        YT_VERIFY(it != ReplicationRequests_.end());
        auto& request = it->second;
        asyncResults.emplace_back(request->Invoke());
    }

    return asyncResults;
}

////////////////////////////////////////////////////////////////////////////////

TTransactionReplicationSessionWithoutBoomerangs::TTransactionReplicationSessionWithoutBoomerangs(
    TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds,
    const TInitiatorRequestLogInfo& logInfo)
    : TTransactionReplicationSessionBase(bootstrap, std::move(transactionIds), logInfo)
{ }

void TTransactionReplicationSessionWithoutBoomerangs::ConstructReplicationRequests()
{
    auto requestIds = DoConstructReplicationRequests();

    YT_LOG_DEBUG_UNLESS(ReplicationRequests_.empty(), "Requesting remote transaction replication (InitiatorRequest: %v, RequestIds: %v, TransactionIds: %v)",
        InitiatorRequest_,
        requestIds,
        RemoteTransactionIds_);
}

TFuture<void> TTransactionReplicationSessionWithoutBoomerangs::Run(bool syncWithUpstream)
{
    auto syncSession = New<TMultiPhaseCellSyncSession>(Bootstrap_, InitiatorRequest_.RequestId);
    syncSession->SetSyncWithUpstream(syncWithUpstream);
    auto cellTags = GetCellTagsToSyncWithDuringInvocation();
    auto asyncResult = InvokeReplicationRequests();
    auto syncFuture = asyncResult
        ? syncSession->Sync(cellTags, asyncResult.AsVoid())
        : syncSession->Sync(cellTags);
    return syncFuture
        .Apply(BIND([this, this_ = MakeStrong(this), syncSession = std::move(syncSession), asyncResult = std::move(asyncResult)] () {
            if (!asyncResult) {
                return VoidFuture;
            }

            YT_VERIFY(asyncResult.IsSet());
            const auto& rspOrError = asyncResult.Get();
            if (!rspOrError.IsOK()) {
                return MakeFuture(TError(rspOrError));
            }

            const auto& transactionToReplicationFuture = rspOrError.Value();

            std::vector<TFuture<void>> asyncReplicationResults;
            asyncReplicationResults.reserve(transactionToReplicationFuture.size());
            for (auto& [transactionId, replicationFuture] : transactionToReplicationFuture) {
                YT_VERIFY(replicationFuture);
                asyncReplicationResults.push_back(replicationFuture);
            }

            // Likely to be empty.
            auto cellTags = GetCellTagsToSyncWithAfterInvocation();

            return syncSession->Sync(cellTags, std::move(asyncReplicationResults));
        }));
}

NObjectClient::TCellTagList TTransactionReplicationSessionWithoutBoomerangs::GetCellTagsToSyncWithDuringInvocation() const
{
    return TTransactionReplicationSessionBase::GetCellTagsToSyncWithBeforeInvocation();
}

TFuture<THashMap<TTransactionId, TFuture<void>>> TTransactionReplicationSessionWithoutBoomerangs::InvokeReplicationRequests()
{
    auto asyncResults = DoInvokeReplicationRequests();
    if (asyncResults.empty()) {
        return {};
    }

    return AllSet(std::move(asyncResults))
        .Apply(BIND(
            [this, this_ = MakeStrong(this)]
            (const std::vector<TTransactionServiceProxy::TErrorOrRspReplicateTransactionsPtr>& responsesOrErrors)
            {
                YT_VERIFY(responsesOrErrors.size() == ReplicationRequestCellTags_.size());

                const auto& transactionManager = Bootstrap_->GetTransactionManager();
                const auto& transactionPresenceCache = transactionManager->GetTransactionPresenceCache();

                THashMap<TTransactionId, TFuture<void>> result;

                for (auto i = 0; i < std::ssize(responsesOrErrors); ++i) {
                    const auto& rspOrError = responsesOrErrors[i];
                    const auto cellTag = ReplicationRequestCellTags_[i];

                    if (rspOrError.IsOK() && !rspOrError.Value()->sync_implied()) {
                        UnsyncedRemoteTransactionCells_.push_back(cellTag);
                    }

                    for (auto transactionId : RemoteTransactionIds_) {
                        if (CellTagFromId(transactionId) != cellTag) {
                            continue;
                        }

                        if (rspOrError.IsOK()) {
                            auto transactionReplicationFuture =
                                transactionPresenceCache->SubscribeRemoteTransactionReplicated(transactionId);
                            if (!transactionReplicationFuture) {
                                LogAndThrowUnknownTransactionPresenceError(transactionId);
                            }
                            YT_VERIFY(result.emplace(transactionId, std::move(transactionReplicationFuture)).second);
                        } else {
                            YT_LOG_DEBUG(rspOrError, "Remote transaction replication failed (InitiatorRequest: %v, TransactionId: %v)",
                                InitiatorRequest_,
                                transactionId);

                            YT_VERIFY(result.emplace(transactionId, MakeFuture(TError(rspOrError))).second);
                        }
                    }
                }

                SortUnique(UnsyncedRemoteTransactionCells_);

                return result;
            }));
}

NObjectClient::TCellTagList TTransactionReplicationSessionWithoutBoomerangs::GetCellTagsToSyncWithAfterInvocation() const
{
    return UnsyncedRemoteTransactionCells_;
}

////////////////////////////////////////////////////////////////////////////////

TTransactionReplicationSessionWithBoomerangs::TTransactionReplicationSessionWithBoomerangs(
    TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds,
    const TInitiatorRequestLogInfo& logInfo,
    std::unique_ptr<TMutation> mutation)
    : TTransactionReplicationSessionBase(bootstrap, std::move(transactionIds), logInfo)
{
    if (mutation) {
        SetMutation(std::move(mutation));
    }

    YT_VERIFY(!Mutation_ || Mutation_->GetMutationId());
}

void TTransactionReplicationSessionWithBoomerangs::SetMutation(std::unique_ptr<TMutation> mutation)
{
    YT_VERIFY(!Mutation_);
    YT_VERIFY(mutation);
    Mutation_ = std::move(mutation);

    if (!Mutation_->GetMutationId()) {
        Mutation_->SetMutationId(GenerateMutationId(), Mutation_->IsRetry());
        YT_LOG_DEBUG("Boomerang mutation has empty ID, forcing one (Request: %v, ForcedMutationId: %v)",
            InitiatorRequest_,
            Mutation_->GetMutationId());
    }

    YT_VERIFY(Mutation_->GetMutationId());
}

TFuture<void> TTransactionReplicationSessionWithBoomerangs::Run(bool syncWithUpstream, const NRpc::IServiceContextPtr& context)
{
    auto syncSession = New<TMultiPhaseCellSyncSession>(Bootstrap_, InitiatorRequest_.RequestId);
    syncSession->SetSyncWithUpstream(syncWithUpstream);
    auto cellTags = GetCellTagsToSyncWithBeforeInvocation();
    auto syncFuture = syncSession->Sync(cellTags);
    auto automatonInvoker = Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::TransactionManager);
    return syncFuture
        .Apply(
            BIND(&TTransactionReplicationSessionWithBoomerangs::InvokeReplicationRequests, MakeStrong(this))
            .AsyncVia(std::move(automatonInvoker)))
        .Apply(BIND([context = std::move(context)] (const TErrorOr<TMutationResponse>& result) {
            if (context->IsReplied()) {
                return;
            }
            if (result.IsOK()) {
                const auto& response = result.Value();
                if (response.Data) {
                    context->Reply(response.Data);
                } else {
                    context->Reply(TError());
                }
            } else {
                context->Reply(TError(result));
            }
        }));
}

NObjectClient::TCellTagList TTransactionReplicationSessionWithBoomerangs::GetCellTagsToSyncWithBeforeInvocation() const
{
    return TTransactionReplicationSessionBase::GetCellTagsToSyncWithBeforeInvocation();
}

void TTransactionReplicationSessionWithBoomerangs::ConstructReplicationRequests()
{
    auto requestIds = DoConstructReplicationRequests();

    YT_VERIFY(Mutation_);
    YT_VERIFY(Mutation_->GetMutationId());
    YT_VERIFY(Mutation_->GetData());

    auto boomerangWaveId = TGuid::Create();
    auto boomerangWaveSize = ReplicationRequests_.size();

    for (auto& [cellTag, request] : ReplicationRequests_) {
        ToProto(request->mutable_boomerang_wave_id(), boomerangWaveId);
        request->set_boomerang_wave_size(boomerangWaveSize);

        ToProto(request->mutable_boomerang_mutation_id(), Mutation_->GetMutationId());
        request->set_boomerang_mutation_type(Mutation_->GetType());
        // TODO(shakurov): less copying?
        request->set_boomerang_mutation_data(ToString(Mutation_->GetData()));
    }

    YT_LOG_DEBUG_UNLESS(ReplicationRequests_.empty(), "Requesting remote transaction replication (InitiatorRequest: %v, RequestIds: %v, TransactionIds: %v, BoomerangMutationId: %v, BoomerangWaveId: %v, BoomerangWaveSize: %v)",
        InitiatorRequest_,
        requestIds,
        RemoteTransactionIds_,
        Mutation_->GetMutationId(),
        boomerangWaveId,
        boomerangWaveSize);
}

TFuture<TMutationResponse> TTransactionReplicationSessionWithBoomerangs::InvokeReplicationRequests()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (ReplicationRequestCellTags_.empty()) {
        return Mutation_->Commit();
    }

    auto keptResult = BeginRequestInResponseKeeper();
    if (keptResult) {
        // Highly unlikely, considering that just a few moments ago some of request transactions were remote.
        return keptResult
            .Apply(BIND([] (const TSharedRefArray& data) {
                return TMutationResponse{EMutationResponseOrigin::ResponseKeeper, data};
            }));
    }
    keptResult = FindRequestInResponseKeeper();
    YT_VERIFY(keptResult);

    auto asyncResults = DoInvokeReplicationRequests();
    YT_VERIFY(!asyncResults.empty());
    // NB: this loop is just for logging.
    for (auto requestIndex = 0; requestIndex < std::ssize(asyncResults); ++requestIndex) {
        auto& future = asyncResults[requestIndex];
        future.Subscribe(BIND([requestIndex, this, this_ = MakeStrong(this)] (const TErrorOr<TRspReplicateTransactionsPtr>& rspOrError)
        {
            if (!rspOrError.IsOK()) {
                YT_VERIFY(requestIndex < std::ssize(ReplicationRequestCellTags_));
                auto cellTag = ReplicationRequestCellTags_[requestIndex];

                for (auto transactionId : RemoteTransactionIds_) {
                    if (CellTagFromId(transactionId) != cellTag) {
                        continue;
                    }

                    YT_LOG_DEBUG(rspOrError, "Remote transaction replication failed (InitiatorRequest: %v, TransactionId: %v)",
                        InitiatorRequest_,
                        transactionId);
                }
            }
        }));
    }

    YT_LOG_DEBUG("Request is awaiting boomerang mutation to be applied (Request: %v, MutationId: %v)",
        InitiatorRequest_,
        Mutation_->GetMutationId());

    // NB: the actual responses are irrelevant, because boomerang arrival
    // implicitly signifies a sync with corresponding cell. Absence of errors,
    // on the other hand, is crucial.
    return AllSucceeded(std::move(asyncResults)).AsVoid()
        .Apply(BIND([this, this_ = MakeStrong(this), keptResult = std::move(keptResult)] (const TError& error) {
            if (!error.IsOK()) {
                YT_LOG_DEBUG(error, "Request is no longer awaiting boomerang mutation to be applied (Request: %v, MutationId: %v)",
                    InitiatorRequest_,
                    Mutation_->GetMutationId());

                EndRequestInResponseKeeper(error);

                return MakeFuture<TSharedRefArray>(
                    error.Wrap("Failed to replicate necessary remote transactions")
                        << TErrorAttribute("mutation_id", Mutation_->GetMutationId())
                        << TErrorAttribute("request_id", InitiatorRequest_.RequestId));
            }

            YT_VERIFY(keptResult);
            return keptResult;
        })
        .AsyncVia(GetCurrentInvoker()))
        .Apply(BIND([] (const TSharedRefArray& data) {
            return TMutationResponse{EMutationResponseOrigin::ResponseKeeper, data};
        }));
}

TFuture<TSharedRefArray> TTransactionReplicationSessionWithBoomerangs::BeginRequestInResponseKeeper()
{
    auto mutationId = Mutation_->GetMutationId();
    YT_VERIFY(mutationId);

    const auto& responseKeeper = Bootstrap_->GetHydraFacade()->GetResponseKeeper();
    return responseKeeper->TryBeginRequest(mutationId, Mutation_->IsRetry());
}

TFuture<TSharedRefArray> TTransactionReplicationSessionWithBoomerangs::FindRequestInResponseKeeper()
{
    auto mutationId = Mutation_->GetMutationId();
    YT_VERIFY(mutationId);

    const auto& responseKeeper = Bootstrap_->GetHydraFacade()->GetResponseKeeper();
    return responseKeeper->FindRequest(mutationId, /*isRetry*/ true);
}

void TTransactionReplicationSessionWithBoomerangs::EndRequestInResponseKeeper(const TError& error)
{
    auto mutationId = Mutation_->GetMutationId();
    YT_VERIFY(mutationId);

    const auto& responseKeeper = Bootstrap_->GetHydraFacade()->GetResponseKeeper();
    if (auto setResponseKeeperPromise =
        responseKeeper->EndRequest(mutationId, error, /*remember*/ false))
    {
        setResponseKeeperPromise();
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> RunTransactionReplicationSession(
    bool syncWithUpstream,
    TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds,
    const IServiceContextPtr& context,
    std::unique_ptr<NHydra::TMutation> mutation,
    bool enableMutationBoomerangs)
{
    YT_VERIFY(context);

    if (enableMutationBoomerangs) {
        auto replicationSession = New<TTransactionReplicationSessionWithBoomerangs>(
            bootstrap,
            std::move(transactionIds),
            TInitiatorRequestLogInfo(context->GetRequestId()),
            std::move(mutation));
        return replicationSession->Run(syncWithUpstream, context);
    } else {
        auto automatonInvoker = bootstrap->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::TransactionManager);
        auto replicationSession = New<TTransactionReplicationSessionWithoutBoomerangs>(
            bootstrap,
            std::move(transactionIds),
            TInitiatorRequestLogInfo(context->GetRequestId()));
        return replicationSession->Run(syncWithUpstream)
            .Apply(BIND([=, mutation=std::move(mutation)] (const TError& error) {
                if (error.IsOK()) {
                    YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
                } else {
                    context->Reply(error);
                }
            })
            .AsyncVia(std::move(automatonInvoker)));
    }
}

TFuture<void> RunTransactionReplicationSession(
    bool syncWithUpstream,
    NCellMaster::TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds,
    TRequestId requestId)
{
    auto replicationSession = New<TTransactionReplicationSessionWithoutBoomerangs>(
        bootstrap,
        std::move(transactionIds),
        TInitiatorRequestLogInfo(requestId));
    return replicationSession->Run(syncWithUpstream);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
