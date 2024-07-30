#include "transaction_replication_session.h"

#include "private.h"
#include "sequoia_integration.h"
#include "transaction_manager.h"
#include "transaction_presence_cache.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multi_phase_cell_sync_session.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/lib/hydra/mutation_context.h>
#include <yt/yt/server/lib/hydra/persistent_response_keeper.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/range_formatters.h>

#include <yt/yt/core/rpc/dispatcher.h>
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

namespace {

std::vector<TTransactionId> NormalizeTransactionIds(std::vector<TTransactionId> transactionIds)
{
    transactionIds.erase(
        std::remove(transactionIds.begin(), transactionIds.end(), NullTransactionId),
        transactionIds.end());
    SortUnique(transactionIds);
    return transactionIds;
}

bool IsSubsequenceOf(TRange<TTransactionId> subsequence, TRange<TTransactionId> sequence)
{
    YT_ASSERT(std::is_sorted(subsequence.begin(), subsequence.end()));
    YT_ASSERT(std::is_sorted(sequence.begin(), sequence.end()));

    if (subsequence.size() > sequence.size()) {
        return false;
    }

    auto sequenceIt = sequence.begin();
    for (auto transactionId : subsequence) {
        while (sequenceIt != sequence.end() && *sequenceIt < transactionId) {
            ++sequenceIt;
        }
        if (sequenceIt == sequence.end() || *sequenceIt != transactionId) {
            return false;
        }
    }

    return true;
}

NLogging::TLogger MakeLogger(const std::optional<TTransactionReplicationInitiatorRequestInfo>& requestInfo)
{
    auto logger = TransactionServerLogger();
    if (requestInfo) {
        TStringBuilder builder;
        builder.AppendFormat("InitiatorRequestId: %v", requestInfo->RequestId);
        if (requestInfo->SubrequestIndex) {
            builder.AppendFormat("[%v]", *requestInfo->SubrequestIndex);
        }
        logger.AddRawTag(builder.Flush());
    }
    return logger;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTransactionReplicationSessionBase::TTransactionReplicationSessionBase(
    TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds,
    std::optional<TTransactionReplicationInitiatorRequestInfo> requestInfo,
    bool enableMirroringToSequoia,
    bool enableBoomerangsIdentity)
    : Bootstrap_(bootstrap)
    , RequestInfo_(std::move(requestInfo))
    , Logger(MakeLogger(RequestInfo_))
    , AllTransactionIds_(NormalizeTransactionIds(std::move(transactionIds)))
    , MirroringToSequoiaEnabled_(enableMirroringToSequoia)
    , EnableBoomerangsIdentity_(enableBoomerangsIdentity)
{
    Initialize();
}

void TTransactionReplicationSessionBase::Initialize()
{
    ValidateTransactionCellTags();

    LocalTransactionIds_ = AllTransactionIds_;

    // NB: by default all transactions are treated as local. Next steps
    // segregate mirrored to Sequoia and remote transactions from local ones.

    SegregateMirroredTransactions();
    InitRemoteTransactions();
    InitReplicationRequestCellTags();
}

void TTransactionReplicationSessionBase::Reset(std::vector<TTransactionId> transactionIds)
{
    auto newTransactionIds = NormalizeTransactionIds(std::move(transactionIds));
    auto oldTransactionIdCount =
        LocalTransactionIds_.size() + RemoteTransactionIds_.size() + MirroredTransactionIds_.size();

    // NB: the whole point of having a dedicated Reset method is this optimization.
    if (oldTransactionIdCount == newTransactionIds.size() &&
        IsSubsequenceOf(LocalTransactionIds_, newTransactionIds) &&
        IsSubsequenceOf(RemoteTransactionIds_, newTransactionIds) &&
        IsSubsequenceOf(MirroredTransactionIds_, newTransactionIds))
    {
        return;
    }

    AllTransactionIds_ = std::move(newTransactionIds);
    Initialize();
}

void TTransactionReplicationSessionBase::InitRemoteTransactions()
{
    auto remoteTransactionCount = std::stable_partition(
        LocalTransactionIds_.begin(),
        LocalTransactionIds_.end(),
        [this] (TTransactionId transactionId) {
            return IsTransactionRemote(transactionId);
        }) - LocalTransactionIds_.begin();

    RemoteTransactionIds_ = LocalTransactionIds_.Slice(0, remoteTransactionCount);
    LocalTransactionIds_ = LocalTransactionIds_.Slice(remoteTransactionCount, LocalTransactionIds_.size());

    UnsyncedLocalTransactionCells_.resize(LocalTransactionIds_.size());
    std::transform(
        LocalTransactionIds_.begin(),
        LocalTransactionIds_.end(),
        UnsyncedLocalTransactionCells_.begin(),
        CellTagFromId);

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

    for (auto transactionId : AllTransactionIds_) {
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
        CellTagFromId);
    SortUnique(ReplicationRequestCellTags_);
}

bool TTransactionReplicationSessionBase::IsMirroredToSequoia(TTransactionId transactionId)
{
    return IsCypressTransactionType(TypeFromId(transactionId)) && IsSequoiaId(transactionId);
}

void TTransactionReplicationSessionBase::SegregateMirroredTransactions()
{
    // NB: it's not fast path: when mirroring to Sequoia is disabled there is no
    // way for function IsMirroredToSequoia() to work correctly. See comment
    // near MirroringToSequoiaEnabled_.
    if (!MirroringToSequoiaEnabled_) {
        MirroredTransactionIds_ = {};
        return;
    }

    auto mirroredTransactionCount = std::stable_partition(
        AllTransactionIds_.begin(),
        AllTransactionIds_.end(),
        IsMirroredToSequoia) - AllTransactionIds_.begin();

    MirroredTransactionIds_ = TRange(AllTransactionIds_.data(), mirroredTransactionCount);
    LocalTransactionIds_ = TMutableRange(
        AllTransactionIds_.data() + mirroredTransactionCount,
        AllTransactionIds_.size() - mirroredTransactionCount);
}

TCellTagList TTransactionReplicationSessionBase::GetCellTagsToSyncWithBeforeInvocation() const
{
    // NB: Note that even if |TransactionIds_| is empty sync with tx coordinator
    // is still necessary.

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

TError TTransactionReplicationSessionBase::WrapError(TError error) const
{
    if (RequestInfo_) {
        error <<= TErrorAttribute("request_id", RequestInfo_->RequestId);
    }
    return error;
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
    YT_LOG_DEBUG("Cannot reliably check transaction presence; probably current epoch ended; the request will be dropped (TransactionId: %v)",
        transactionId);

    // TODO(shakurov): a more specific error code?
    auto error = TError(NYT::NRpc::EErrorCode::Unavailable, "Cannot reliably check transaction presence; probably current epoch ended; the request will be dropped")
        << TErrorAttribute("transaction_id", transactionId);
    THROW_ERROR(WrapError(std::move(error)));
}

TTransactionReplicationSessionBase::TReplicationResponse
TTransactionReplicationSessionBase::DoInvokeReplicationRequests()
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

    return {
        .NonMirrored = asyncResults,
        .Mirrored = ReplicateCypressTransactionsInSequoiaAndSyncWithLeader(
            Bootstrap_,
            std::vector<TTransactionId>(
                MirroredTransactionIds_.begin(),
                MirroredTransactionIds_.end())),
    };
}

////////////////////////////////////////////////////////////////////////////////

void TTransactionReplicationSessionWithoutBoomerangs::ConstructReplicationRequests()
{
    auto requestIds = DoConstructReplicationRequests();

    YT_LOG_DEBUG_UNLESS(
        ReplicationRequests_.empty(),
        "Requesting remote transaction replication (RequestIds: %v, TransactionIds: %v)",
        requestIds,
        RemoteTransactionIds_);
}

TFuture<void> TTransactionReplicationSessionWithoutBoomerangs::Run(bool syncWithUpstream)
{
    auto syncSession = New<TMultiPhaseCellSyncSession>(Bootstrap_, Logger);
    syncSession->SetSyncWithUpstream(syncWithUpstream);

    auto cellTags = GetCellTagsToSyncWithDuringInvocation();

    auto asyncResult = InvokeReplicationRequests();

    const auto& transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
    std::vector<TFuture<void>> additionalFutures = {
        transactionSupervisor->WaitUntilPreparedTransactionsFinished(),
    };

    if (asyncResult) {
        additionalFutures.push_back(asyncResult.AsVoid());
    }

    // NB: we always have to wait all current prepared transactions to observe
    // side effects of Sequoia transactions.
    return syncSession->Sync(cellTags, std::move(additionalFutures))
        .Apply(BIND([this, this_ = MakeStrong(this), syncSession = std::move(syncSession), asyncResult = std::move(asyncResult)] {
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

    if (asyncResults.NonMirrored.empty() && !asyncResults.Mirrored) {
        return {};
    }

    return AllSet(std::move(asyncResults.NonMirrored))
        .Apply(BIND(
            [this, this_ = MakeStrong(this), resultForMirrored = std::move(asyncResults.Mirrored)]
            (const std::vector<TTransactionServiceProxy::TErrorOrRspReplicateTransactionsPtr>& responsesOrErrors) {
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
                            EmplaceOrCrash(result, transactionId, std::move(transactionReplicationFuture));
                        } else {
                            YT_LOG_DEBUG(rspOrError, "Remote transaction replication failed (TransactionId: %v)",
                                transactionId);

                            EmplaceOrCrash(result, transactionId, MakeFuture(TError(rspOrError)));
                        }
                    }
                }

                SortUnique(UnsyncedRemoteTransactionCells_);

                return resultForMirrored.Apply(BIND([
                    this, this_ = MakeStrong(this), result = std::move(result)
                ] () mutable {
                    for (auto mirroredTransactionId : MirroredTransactionIds_) {
                        EmplaceOrCrash(result, mirroredTransactionId, VoidFuture);
                    }
                    return result;
                }));
            }));
}

NObjectClient::TCellTagList TTransactionReplicationSessionWithoutBoomerangs::GetCellTagsToSyncWithAfterInvocation() const
{
    return UnsyncedRemoteTransactionCells_;
}

////////////////////////////////////////////////////////////////////////////////

void TTransactionReplicationSessionWithBoomerangs::SetMutation(std::unique_ptr<TMutation> mutation)
{
    YT_VERIFY(!Mutation_);
    YT_VERIFY(mutation);
    Mutation_ = std::move(mutation);

    if (!Mutation_->GetMutationId()) {
        Mutation_->SetMutationId(GenerateMutationId(), Mutation_->IsRetry());
        YT_LOG_DEBUG("Boomerang mutation has empty ID, forcing one (ForcedMutationId: %v)",
            Mutation_->GetMutationId());
    }

    YT_VERIFY(Mutation_->GetMutationId());
}

TFuture<void> TTransactionReplicationSessionWithBoomerangs::Run(bool syncWithUpstream, const NRpc::IServiceContextPtr& context)
{
    auto syncSession = New<TMultiPhaseCellSyncSession>(Bootstrap_, Logger);
    syncSession->SetSyncWithUpstream(syncWithUpstream);

    auto cellTags = GetCellTagsToSyncWithBeforeInvocation();

    const auto& transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
    auto preparedTransactionsFinished = transactionSupervisor->WaitUntilPreparedTransactionsFinished();

    // NB: we always have to wait all current prepared transactions to observe
    // side effects of Sequoia transactions.
    auto syncFuture = syncSession->Sync(cellTags, std::move(preparedTransactionsFinished));
    auto automatonInvoker = Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::TransactionManager);
    return syncFuture
        .Apply(
            BIND(&TTransactionReplicationSessionWithBoomerangs::InvokeReplicationRequests, MakeStrong(this), std::nullopt)
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

        if (RequestInfo_ && EnableBoomerangsIdentity_) {
            WriteAuthenticationIdentityToProto(request.Get(), RequestInfo_->Identity);
        }
    }

    YT_LOG_DEBUG_UNLESS(ReplicationRequests_.empty(),
        "Requesting remote transaction replication (RequestIds: %v, "
        "TransactionIds: %v, BoomerangMutationId: %v, BoomerangWaveId: %v, BoomerangWaveSize: %v)",
        requestIds,
        RemoteTransactionIds_,
        Mutation_->GetMutationId(),
        boomerangWaveId,
        boomerangWaveSize);
}

// TODO(shakurov): refactor. Get rid of .WithTimeout.
TFuture<TMutationResponse> TTransactionReplicationSessionWithBoomerangs::InvokeReplicationRequestsOffloaded(std::optional<TDuration> timeout)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return BIND(&TTransactionReplicationSessionWithBoomerangs::InvokeReplicationRequests, MakeStrong(this), timeout)
        .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
        .Run();
}

TFuture<TMutationResponse> TTransactionReplicationSessionWithBoomerangs::InvokeReplicationRequests(std::optional<TDuration> timeout)
{
    if (ReplicationRequestCellTags_.empty() && MirroredTransactionIds_.empty()) {
        return timeout
            ? Mutation_->Commit().WithTimeout(*timeout)
            : Mutation_->Commit();
    }

    // TODO(kvk1920): Implement boomerang-like optimization for mirrored
    // transactions. See YT-21270.
    if (ReplicationRequestCellTags_.empty()) {
        auto asyncResult = DoInvokeReplicationRequests();
        YT_VERIFY(asyncResult.NonMirrored.empty());
        return asyncResult.Mirrored.Apply(BIND([this, this_ = MakeStrong(this)] {
            return Mutation_->Commit();
        }));
    }

    auto keptResult = BeginRequestInResponseKeeper();
    if (keptResult) {
        // Highly unlikely, considering that just a few moments ago some of request transactions were remote.
        auto result = keptResult
            .Apply(BIND([] (const TSharedRefArray& data) {
                return TMutationResponse{EMutationResponseOrigin::ResponseKeeper, data};
            }));
        return timeout
            ? result.WithTimeout(*timeout)
            : result;
    }
    keptResult = FindRequestInResponseKeeper();
    YT_VERIFY(keptResult);

    auto asyncResults = DoInvokeReplicationRequests();
    YT_VERIFY(!asyncResults.NonMirrored.empty());
    // NB: this loop is just for logging.
    for (auto requestIndex = 0; requestIndex < std::ssize(asyncResults.NonMirrored); ++requestIndex) {
        auto& future = asyncResults.NonMirrored[requestIndex];
        future.Subscribe(BIND([requestIndex, this, this_ = MakeStrong(this)] (const TErrorOr<TRspReplicateTransactionsPtr>& rspOrError)
        {
            if (!rspOrError.IsOK()) {
                YT_VERIFY(requestIndex < std::ssize(ReplicationRequestCellTags_));
                auto cellTag = ReplicationRequestCellTags_[requestIndex];

                for (auto transactionId : RemoteTransactionIds_) {
                    if (CellTagFromId(transactionId) != cellTag) {
                        continue;
                    }

                    YT_LOG_DEBUG(rspOrError, "Remote transaction replication failed (TransactionId: %v)",
                        transactionId);
                }
            }
        }));
    }

    asyncResults.Mirrored.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
        if (error.IsOK()) {
            return;
        }

        for (auto transactionId : MirroredTransactionIds_) {
            YT_LOG_DEBUG(error, "Remote transaction replication failed (TransactionId: %v)",
                transactionId);
        }
    }));

    YT_LOG_DEBUG("Request is awaiting boomerang mutation to be applied (MutationId: %v)",
        Mutation_->GetMutationId());

    // NB: the actual responses are irrelevant, because boomerang arrival
    // implicitly signifies a sync with corresponding cell. Absence of errors,
    // on the other hand, is crucial.
    auto result = AllSucceeded(std::vector{AllSucceeded(std::move(asyncResults.NonMirrored)).AsVoid(), asyncResults.Mirrored})
        .Apply(BIND([this, this_ = MakeStrong(this), keptResult = std::move(keptResult)] (const TError& error) {
            if (!error.IsOK()) {
                YT_LOG_DEBUG(error, "Request is no longer awaiting boomerang mutation to be applied (MutationId: %v)",
                    Mutation_->GetMutationId());

                EndRequestInResponseKeeper(error);

                auto wrappedError = WrapError(
                    error.Wrap("Failed to replicate necessary remote transactions")
                        << TErrorAttribute("mutation_id", Mutation_->GetMutationId()));
                return MakeFuture<TSharedRefArray>(std::move(wrappedError));
            }

            YT_VERIFY(keptResult);
            return keptResult;
        })
        .AsyncVia(GetCurrentInvoker()))
        .Apply(BIND([] (const TSharedRefArray& data) {
            return TMutationResponse{EMutationResponseOrigin::ResponseKeeper, data};
        }));
    return timeout
        ? result.WithTimeout(*timeout)
        : result;
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

void RunTransactionReplicationSessionAndReply(
    bool syncWithUpstream,
    TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds,
    const IServiceContextPtr& context,
    std::unique_ptr<NHydra::TMutation> mutation,
    bool enableMutationBoomerangs,
    bool enableMirroringToSequoia,
    bool enableBoomerangsIdentity)
{
    YT_VERIFY(context);

    TTransactionReplicationInitiatorRequestInfo requestInfo{
        .Identity = context->GetAuthenticationIdentity(),
        .RequestId = context->GetRequestId(),
    };

    if (enableMutationBoomerangs) {
        auto replicationSession = New<TTransactionReplicationSessionWithBoomerangs>(
            bootstrap,
            std::move(transactionIds),
            std::move(requestInfo),
            enableMirroringToSequoia,
            enableBoomerangsIdentity);
        replicationSession->SetMutation(std::move(mutation));
        YT_UNUSED_FUTURE(replicationSession->Run(syncWithUpstream, context));
    } else {
        auto automatonInvoker = bootstrap->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::TransactionManager);
        auto replicationSession = New<TTransactionReplicationSessionWithoutBoomerangs>(
            bootstrap,
            std::move(transactionIds),
            std::move(requestInfo),
            enableMirroringToSequoia,
            enableBoomerangsIdentity);
        YT_UNUSED_FUTURE(replicationSession->Run(syncWithUpstream)
            .Apply(BIND([=, mutation = std::move(mutation)] (const TError& error) {
                if (error.IsOK()) {
                    YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
                } else {
                    context->Reply(error);
                }
            })
            .AsyncVia(std::move(automatonInvoker))));
    }
}

TFuture<void> RunTransactionReplicationSession(
    bool syncWithUpstream,
    NCellMaster::TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds,
    bool enableMirroringToSequoia,
    bool enableBoomerangsIdentity)
{
    auto replicationSession = New<TTransactionReplicationSessionWithoutBoomerangs>(
        bootstrap,
        std::move(transactionIds),
        /*requestInfo*/ std::nullopt,
        enableMirroringToSequoia,
        enableBoomerangsIdentity);
    return replicationSession->Run(syncWithUpstream);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
