#include "expiration_tracker.h"
#include "private.h"
#include "config.h"
#include "cypress_manager.h"

#include <yt/yt/server/master/cypress_server/proto/cypress_manager.pb.h>

#include <yt/yt/server/lib/hydra/mutation.h>
#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/serialize.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/security_server/access_log.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/library/profiling/producer.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NProfiling;
using namespace NSecurityServer;
using namespace NYTree;

using TExpirationTimePropertiesView = TCypressNode::TExpirationTimeProperties::TView;
using TExpirationTimeoutPropertiesView = TCypressNode::TExpirationTimeoutProperties::TView;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TExpirationTracker
    : public IExpirationTracker
    , public TMasterAutomatonPart
{
public:
    explicit TExpirationTracker(NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::ExpirationTracker)
    {
        ExpirationTrackerProfiler()
            .WithDefaultDisabled()
            .WithGlobal()
            .WithTag("cell_tag", ToString(Bootstrap_->GetMulticellManager()->GetCellTag()))
            .AddProducer("", BufferedProducer_);

        RegisterLoader(
            "ExpirationTracker.Keys",
            BIND_NO_PROPAGATE(&TExpirationTracker::LoadKeys, Unretained(this)));
        RegisterLoader(
            "ExpirationTracker.Values",
            BIND_NO_PROPAGATE(&TExpirationTracker::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "ExpirationTracker.Keys",
            BIND_NO_PROPAGATE(&TExpirationTracker::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "ExpirationTracker.Values",
            BIND_NO_PROPAGATE(&TExpirationTracker::SaveValues, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TExpirationTracker::HydraUpdateFailedExpirationAttempts, Unretained(this)));
    }

    void OnLeaderRecoveryComplete() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderRecoveryComplete();

        FailedExpirationAttempts_.clear();
        for (const auto& [key, attempts] : PersistentFailedExpirationAttempts_) {
            const auto& [node, user] = key;
            if (!IsObjectAlive(node) || !IsObjectAlive(user)) {
                continue;
            }
            auto failedAttemptDescriptor = TFailedAttemptDescriptor(node.Get(), user.Get());
            FailedExpirationAttempts_.insert_or_assign(std::move(failedAttemptDescriptor), attempts);
        }
        auto previouslyScheduledForRemovalNodes = std::exchange(ScheduledForRemovalNodes_, {});

        YT_LOG_INFO("Started registering node expiration (Count: %v)", previouslyScheduledForRemovalNodes.size());

        for (auto trunkNode : previouslyScheduledForRemovalNodes) {
            if (trunkNode->GetExpirationTimeIterator()) {
                UnregisterNodeExpirationTime(trunkNode);
            }

            if (trunkNode->GetExpirationTimeoutIterator()) {
                UnregisterNodeExpirationTimeout(trunkNode);
            }

            if (auto expirationTime = trunkNode->GetExpirationTime()) {
                RegisterNodeExpirationTime(trunkNode, *expirationTime);
            }

            if (trunkNode->GetExpirationTimeout() && !IsNodeLocked(trunkNode)) {
                RegisterNodeExpirationTimeout(trunkNode);
            }
        }
        YT_LOG_INFO("Finished registering node expiration");
    }

    void OnLeaderActive() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        BufferedProducer_->SetEnabled(true);

        YT_VERIFY(!CheckExecutor_);
        CheckExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::CypressNodeExpirationTracker),
            BIND(&TExpirationTracker::RunCheckIteration, MakeWeak(this)));
        YT_VERIFY(!UpdatePersistentFailedExpirationAttemptsExecutor_);
        UpdatePersistentFailedExpirationAttemptsExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::CypressNodeExpirationTracker),
            BIND(&TExpirationTracker::UpdatePersistentFailedExpirationAttempts, MakeWeak(this)));

        CheckExecutor_->Start();
        UpdatePersistentFailedExpirationAttemptsExecutor_->Start();

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
    }

    void OnStopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

        BufferedProducer_->SetEnabled(false);

        CheckExecutor_.Reset();
        UpdatePersistentFailedExpirationAttemptsExecutor_.Reset();

        FailedExpirationAttempts_.clear();
    }

    void Clear() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        ExpirationMap_.clear();
        ScheduledForRemovalNodes_.clear();
        FailedExpirationAttempts_.clear();
        PersistentFailedExpirationAttempts_.clear();
    }

    void OnNodeExpirationTimeUpdated(
        TCypressNode* trunkNode,
        std::optional<TExpirationTimePropertiesView> oldExpirationTimeProperties = {}) override
    {
        YT_VERIFY(HasHydraContext());
        YT_ASSERT(trunkNode->IsTrunk());

        if (trunkNode->IsForeign()) {
            return;
        }

        auto expirationTimeProperties = trunkNode->GetExpirationTimePropertiesView();
        if (trunkNode->GetExpirationTimeIterator()) {
            if (expirationTimeProperties == oldExpirationTimeProperties) {
                // Fast path.
                return;
            }

            UnregisterNodeExpirationTime(trunkNode);
        }

        // NB: Sometimes the node can be created with expiration time, but be held past it in the same transaction
        // that created it. In this case the node might end up unregistered from the expiration maps.
        // See TExpirationTracker::OnNodeRemovalFailed().
        if (expirationTimeProperties) {
            const auto& [user, expirationTime] = *expirationTimeProperties;

            YT_LOG_DEBUG("Node expiration time set (NodeId: %v, ExpirationTime: %v)",
                trunkNode->GetId(),
                expirationTime);
            RegisterNodeExpirationTime(trunkNode, expirationTime);
            if (oldExpirationTimeProperties) {
                auto oldUser = oldExpirationTimeProperties->first;
                if (user != oldUser) {
                    FailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode, oldUser));
                    PersistentFailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode, oldUser));
                }
            }
        } else if (oldExpirationTimeProperties) {
            YT_LOG_DEBUG("Node expiration time reset (NodeId: %v)",
                trunkNode->GetId());

            auto oldUser = oldExpirationTimeProperties->first;
            FailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode, oldUser));
            PersistentFailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode, oldUser));
        }
    }

    void OnNodeExpirationTimeoutUpdated(
        TCypressNode* trunkNode,
        std::optional<TExpirationTimeoutPropertiesView> oldExpirationTimeoutProperties = {}) override
    {
        YT_VERIFY(HasHydraContext());
        YT_ASSERT(trunkNode->IsTrunk());

        if (trunkNode->IsForeign()) {
            return;
        }

        auto expirationTimeoutProperties = trunkNode->GetExpirationTimeoutPropertiesView();
        if (trunkNode->GetExpirationTimeoutIterator()) {
            if (expirationTimeoutProperties == oldExpirationTimeoutProperties) {
                // Fast path.
                return;
            }

            UnregisterNodeExpirationTimeout(trunkNode);
        }

        // See TExpirationTracker::OnNodeExpirationTimeUpdated().
        if (expirationTimeoutProperties) {
            const auto& [user, expirationTimeout] = *expirationTimeoutProperties;

            YT_LOG_DEBUG("Node expiration timeout set (NodeId: %v, ExpirationTimeout: %v)",
                trunkNode->GetId(),
                expirationTimeout);
            if (!IsNodeLocked(trunkNode)) {
                RegisterNodeExpirationTimeout(trunkNode);
            }

            if (oldExpirationTimeoutProperties) {
                auto oldUser = oldExpirationTimeoutProperties->first;
                if (user != oldUser) {
                    FailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode, oldUser));
                    PersistentFailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode, oldUser));
                }
            }
        } else if (oldExpirationTimeoutProperties) {
            YT_LOG_DEBUG("Node expiration timeout reset (NodeId: %v)",
                trunkNode->GetId());

            auto oldUser = oldExpirationTimeoutProperties->first;
            FailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode, oldUser));
            PersistentFailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode, oldUser));
        }
    }

    void OnNodeTouched(TCypressNode* trunkNode) override
    {
        YT_VERIFY(HasMutationContext());
        YT_ASSERT(trunkNode->IsTrunk());

        if (trunkNode->IsForeign()) {
            return;
        }

        if (trunkNode->GetExpirationTimeoutIterator()) {
            UnregisterNodeExpirationTimeout(trunkNode);
        }

        auto trunkNodeExpirationTimeout = trunkNode->GetExpirationTimeout();
        if (trunkNodeExpirationTimeout && !IsNodeLocked(trunkNode)) {
            auto expirationTimeout = *trunkNodeExpirationTimeout;
            auto expirationTime = trunkNode->GetTouchTime() + expirationTimeout;
            YT_LOG_TRACE("Node is scheduled to expire by timeout (NodeId: %v, ExpirationTimeout: %v, AnticipatedExpirationTime: %v)",
                trunkNode->GetId(),
                expirationTimeout,
                expirationTime);
            RegisterNodeExpirationTimeout(trunkNode);
        }
    }

    void OnNodeDestroyed(TCypressNode* trunkNode) override
    {
        YT_VERIFY(HasHydraContext());
        YT_ASSERT(trunkNode->IsTrunk());

        if (trunkNode->IsForeign()) {
            return;
        }

        TUser* expirationTimeUser = nullptr;
        if (trunkNode->GetExpirationTimeIterator()) {
            expirationTimeUser = trunkNode->GetExpirationTimeUser().value_or(nullptr);
            UnregisterNodeExpirationTime(trunkNode);
        }

        TUser* expirationTimeoutUser = nullptr;
        if (trunkNode->GetExpirationTimeoutIterator()) {
            expirationTimeoutUser = trunkNode->GetExpirationTimeoutUser().value_or(nullptr);
            UnregisterNodeExpirationTimeout(trunkNode);
        }

        // NB: Typically missing.
        ScheduledForRemovalNodes_.erase(trunkNode);
        if (expirationTimeUser) {
            FailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode, expirationTimeUser));
            PersistentFailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode, expirationTimeUser));
        }
        if (expirationTimeoutUser) {
            FailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode, expirationTimeoutUser));
            PersistentFailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode, expirationTimeoutUser));
        }
    }

private:
    const NProfiling::TBufferedProducerPtr BufferedProducer_ = New<NProfiling::TBufferedProducer>();

    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TExpirationTracker::OnDynamicConfigChanged, MakeWeak(this));

    NConcurrency::TPeriodicExecutorPtr CheckExecutor_;
    NConcurrency::TPeriodicExecutorPtr UpdatePersistentFailedExpirationAttemptsExecutor_;

    // NB: Nodes that have both expiration time and expiration timeout may appear twice here.
    TCypressNodeExpirationMap ExpirationMap_;
    THashSet<TCypressNodeRawPtr> ScheduledForRemovalNodes_;

    using TFailedAttemptDescriptorView = std::pair<TCypressNodeRawPtr, TUserRawPtr>;

    using TFailedAttemptDescriptor = std::pair<TEphemeralObjectPtr<TCypressNode>, TEphemeralObjectPtr<TUser>>;
    THashMap<TFailedAttemptDescriptor, int> FailedExpirationAttempts_;

    // Persistent.
    using TFailedAttemptPersistentDescriptor = std::pair<TWeakObjectPtr<TCypressNode>, TWeakObjectPtr<TUser>>;
    THashMap<TFailedAttemptPersistentDescriptor, int> PersistentFailedExpirationAttempts_;


    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void OnNodeRemovalFailed(const TEphemeralObjectPtr<TCypressNode>& trunkNode)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        if (trunkNode->IsForeign()) {
            return;
        }

        // This usually means that either:
        //   1. Node was created under a transaction, and was held past
        //      it's expiration time by the same tx.
        //   2. Node is already a part of a subtree, that was detached and
        //      scheduled for destruction.
        // In both cases it's fine to just remove it from the expiration map
        // to avoid checking it multiple times. It will either be registered again
        // when merging branches, or be removed on its own.
        if (!trunkNode->GetReachable()) {
            // NB: Typically missing at followers.
            ScheduledForRemovalNodes_.erase(trunkNode.Get());
            return;
        }

        auto trunkNodeExpirationTime = trunkNode->GetExpirationTime();
        if (trunkNodeExpirationTime && !trunkNode->GetExpirationTimeIterator()) {
            // NB: Typically missing at followers.
            ScheduledForRemovalNodes_.erase(trunkNode.Get());

            // NB: Time(out) iterators differ on the leader and followers. Avoid relying
            // on them when updating persistent state. The use of |TInstant::Now()| is ok.
            auto now = TInstant::Now();
            auto newExpirationTime = *trunkNodeExpirationTime > now
                // Expiration time was updated concurrently.
                ? *trunkNodeExpirationTime
                : now + GetDynamicConfig()->ExpirationBackoffTime;
            RegisterNodeExpirationTime(trunkNode.Get(), newExpirationTime);
        }

        auto trunkNodeExpirationTimeout = trunkNode->GetExpirationTimeout();
        if (trunkNodeExpirationTimeout && !trunkNode->GetExpirationTimeoutIterator()) {
            // NB: Typically missing at followers.
            ScheduledForRemovalNodes_.erase(trunkNode.Get());

            if (!IsNodeLocked(trunkNode.Get())) {
                // This is transient, |TInstant::Now()| is ok.
                auto now = TInstant::Now();
                auto expirationTimeout = *trunkNodeExpirationTimeout;
                auto touchTimeOverride = trunkNode->GetTouchTime() + expirationTimeout > now
                    // Either expiration timeout or touch time was updated concurrently.
                    ? std::nullopt
                    : std::optional(now);
                RegisterNodeExpirationTimeout(trunkNode.Get(), touchTimeOverride);
            } // Else expiration will be rescheduled on lock release.
        }

        auto isExpirationTimeReset = !trunkNode->GetExpirationTime();
        auto isExpirationTimeoutReset = !trunkNode->GetExpirationTimeout();

        if (isExpirationTimeReset && !trunkNode->GetExpirationTimeIterator() ||
            isExpirationTimeoutReset && !trunkNode->GetExpirationTimeoutIterator())
        {
            ScheduledForRemovalNodes_.erase(trunkNode.Get());
        }

        // Prolong expiration in case of removal failure. This is debatable but seems safer.
        if (!isExpirationTimeoutReset && !IsNodeLocked(trunkNode.Get())) {
            // When removing via client, failure is processed outside of a mutation.
            if (auto* mutationContext = TryGetCurrentMutationContext()) {
                trunkNode->SetTouchTime(mutationContext->GetTimestamp());
            }
        }
    }

    void RegisterNodeExpirationTime(TCypressNode* trunkNode, TInstant expirationTime)
    {
        YT_ASSERT(!trunkNode->GetExpirationTimeIterator());

        if (!ScheduledForRemovalNodes_.contains(trunkNode)) {
            auto it = ExpirationMap_.emplace(expirationTime, trunkNode);
            trunkNode->SetExpirationTimeIterator(it);
        }
    }

    void RegisterNodeExpirationTimeout(TCypressNode* trunkNode, std::optional<TInstant> touchTimeOverride = {})
    {
        YT_ASSERT(!trunkNode->GetExpirationTimeoutIterator());

        if (!ScheduledForRemovalNodes_.contains(trunkNode)) {
            auto touchTime = touchTimeOverride.value_or(trunkNode->GetTouchTime());
            YT_VERIFY(touchTime);
            auto expirationTimeout = trunkNode->GetExpirationTimeout();
            YT_VERIFY(expirationTimeout);
            auto it = ExpirationMap_.emplace(touchTime + *expirationTimeout, trunkNode);
            trunkNode->SetExpirationTimeoutIterator(it);
        }
    }

    void UnregisterNodeExpirationTime(TCypressNode* trunkNode)
    {
        ExpirationMap_.erase(*trunkNode->GetExpirationTimeIterator());
        trunkNode->SetExpirationTimeIterator(std::nullopt);
    }

    void UnregisterNodeExpirationTimeout(TCypressNode* trunkNode)
    {
        ExpirationMap_.erase(*trunkNode->GetExpirationTimeoutIterator());
        trunkNode->SetExpirationTimeoutIterator(std::nullopt);
    }

    bool IsNodeLocked(TCypressNode* trunkNode) const
    {
        return !trunkNode->LockingState().AcquiredLocks.empty();
    }

    void RunCheckIteration()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        if (!hydraManager->IsActiveLeader()) {
            return;
        }

        auto checkTime = NProfiling::GetInstant();
        CollectAndRemoveExpiredNodes(checkTime);
        UpdateProfiling(checkTime);
    }

    void CollectAndRemoveExpiredNodes(TInstant checkTime)
    {
        std::vector<TCypressNodeRawPtr> expiredTrunkNodes;

        auto maxExpiredNodesRemovalsPerCommit = GetDynamicConfig()->MaxExpiredNodesRemovalsPerCommit;
        std::optional<bool> isSequoia;

        while (!ExpirationMap_.empty() &&
            std::ssize(expiredTrunkNodes) < maxExpiredNodesRemovalsPerCommit)
        {
            auto it = ExpirationMap_.begin();
            auto [expirationTime, trunkNode] = *it;

            if (expirationTime > checkTime) {
                break;
            }

            // See comment for ExpirationMap.
            if (!ScheduledForRemovalNodes_.contains(trunkNode)) {
                if (!isSequoia.has_value()) {
                    isSequoia = trunkNode->IsSequoia();
                }

                // Sequoia and Cypress nodes are not expected to be living at
                // the same cell, so this check is just a precaution.
                if (isSequoia != trunkNode->IsSequoia()) {
                    break;
                }

                if (IsObjectAlive(trunkNode)) {
                    InsertOrCrash(ScheduledForRemovalNodes_, trunkNode);
                    expiredTrunkNodes.push_back(trunkNode);
                }
            }

            if (trunkNode->GetExpirationTimeIterator() && *trunkNode->GetExpirationTimeIterator() == it) {
                UnregisterNodeExpirationTime(trunkNode);
            } else if (trunkNode->GetExpirationTimeoutIterator() && *trunkNode->GetExpirationTimeoutIterator() == it) {
                UnregisterNodeExpirationTimeout(trunkNode);
            } else {
                YT_ABORT();
            }
        }

        if (expiredTrunkNodes.empty()) {
            return;
        }

        YT_LOG_DEBUG("Starting removal of expired %v nodes (Count: %v)",
            *isSequoia ? "Sequoia" : "Cypress",
            std::ssize(expiredTrunkNodes));

        std::vector<TEphemeralObjectPtr<TCypressNode>> trunkNodes;
        trunkNodes.reserve(expiredTrunkNodes.size());
        for (auto trunkNode : expiredTrunkNodes) {
            trunkNodes.emplace_back(trunkNode);
        }

        RemoveExpiredNodesViaClient(trunkNodes);
    }

    void UpdateProfiling(TInstant checkTime)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto checkLag = TDuration::Zero();
        if (!ExpirationMap_.empty()) {
            auto expirationTime = ExpirationMap_.begin()->first;
            checkLag = std::max(checkLag, checkTime - expirationTime);
        }

        TSensorBuffer buffer;
        buffer.AddGauge("/expiration_time_lag", checkLag.MilliSeconds());
        buffer.AddGauge("/failed_expiration_attempts_size", PersistentFailedExpirationAttempts_.size());
        BufferedProducer_->Update(std::move(buffer));
    }

    void RemoveExpiredNodesViaClient(const std::vector<TEphemeralObjectPtr<TCypressNode>>& trunkNodes)
    {
        auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetRootClient());
        auto batchReq = proxy.ExecuteBatch();

        std::vector<TRevision> revisions;
        revisions.reserve(trunkNodes.size());
        std::vector<TUserRawPtr> users;
        users.reserve(trunkNodes.size());

        for (const auto& trunkNode : trunkNodes) {
            TUserRawPtr user;
            if (trunkNode->GetExpirationTime() && !trunkNode->GetExpirationTimeIterator()) {
                user = trunkNode->GetExpirationTimeUser().value_or(nullptr);
            } else if (trunkNode->GetExpirationTimeout() && !trunkNode->GetExpirationTimeoutIterator()) {
                user = trunkNode->GetExpirationTimeoutUser().value_or(nullptr);
            }
            users.push_back(user);

            auto targetPath = FromObjectId(trunkNode->GetId()) + "&";
            auto req = TYPathProxy::Remove(targetPath);
            req->set_recursive(true);

            if (user && GetDynamicConfig()->EnableAuthorizedExpiration) {
                req->Header().set_user(user->GetName());
            }

            revisions.push_back(trunkNode->GetRevision());

            auto* prerequisitesExt = req->Header().MutableExtension(
                NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
            auto* prerequisiteRevision = prerequisitesExt->add_revisions();
            prerequisiteRevision->set_path(targetPath);
            prerequisiteRevision->set_revision(revisions.back().Underlying());

            // TODO(danilalexeev): YT-24752. Support TExpirationExt in Sequoia.
            SetCausedByNodeExpiration(&req->Header(), true);

            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        if (!batchRspOrError.IsOK()) {
            for (const auto& trunkNode : trunkNodes) {
                if (IsObjectAlive(trunkNode)) {
                    OnNodeRemovalFailed(trunkNode);
                }
            }
            return;
        }

        auto rsps = batchRspOrError.Value()->GetResponses<TYPathProxy::TRspRemove>();
        YT_VERIFY(rsps.size() == trunkNodes.size());
        for (const auto& [rspOrError, trunkNode, user, revision] : Zip(rsps, trunkNodes, users, revisions)) {
            if (rspOrError.IsOK()) {
                continue;
            }

            if (!IsObjectAlive(trunkNode)) {
                continue;
            }

            YT_LOG_DEBUG(rspOrError,
                "Cannot remove an expired node; backing off and retrying (NodeId: %v)",
                trunkNode->GetId());

            if (user && GetDynamicConfig()->EnableAuthorizedExpiration) {
                if (!IsObjectAlive(user) || user->IsBeingRemoved()) {
                    OnNodeRemovalAuthorizationFailedViaClient(trunkNode, user, revision);
                } else if (rspOrError.FindMatching(NSecurityClient::EErrorCode::AuthorizationError)) {
                    const auto& securityManager = Bootstrap_->GetSecurityManager();
                    securityManager->IncrementFailedExpirationRequestCount(user);

                    auto [failedAttemptsIt, inserted] =
                        FailedExpirationAttempts_.emplace(TFailedAttemptDescriptor(trunkNode.Get(), user), 0);
                    auto failedAttempts = failedAttemptsIt->second++;

                    YT_LOG_DEBUG(
                        "Failed an attempt to remove the expired node due to an authorization error (NodeId: %v, Attempts: %v)",
                        trunkNode->GetId(),
                        failedAttempts + 1);

                    auto expirationAttemptsExceeded = failedAttempts >= GetDynamicConfig()->ExpirationAttemptLimit;
                    if (expirationAttemptsExceeded) {
                        OnNodeRemovalAuthorizationFailedViaClient(trunkNode, user, revision);
                    }
                }
            }
            OnNodeRemovalFailed(trunkNode);
        }
    }

    void OnNodeRemovalAuthorizationFailedViaClient(
        const TEphemeralObjectPtr<TCypressNode>& trunkNode, TUserRawPtr user, NHydra::TRevision revision)
    {
        // NB: The user is not dereferenced here so it's safe to not check its liveness

        TCompactVector<TInternedAttributeKey, 2> attributesToRemove;

        auto expirationTimeUser = trunkNode->GetExpirationTimeUser();
        if (expirationTimeUser == std::optional(user) && !trunkNode->GetExpirationTimeIterator()) {
            attributesToRemove.emplace_back(NServer::EInternedAttributeKey::ExpirationTimeUser);
        }

        auto expirationTimeoutUser = trunkNode->GetExpirationTimeoutUser();
        if (expirationTimeoutUser == std::optional(user) && !trunkNode->GetExpirationTimeoutIterator()) {
            attributesToRemove.emplace_back(NServer::EInternedAttributeKey::ExpirationTimeoutUser);
        }

        auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetRootClient());
        auto batchReq = proxy.ExecuteBatch();

        auto trunkNodeId = trunkNode->GetId();
        auto targetPath = FromObjectId(trunkNodeId) + "&";
        for (auto attribute : attributesToRemove) {
            auto targetAttribute = targetPath + "/@" + attribute.Unintern();

            auto req = TYPathProxy::Set(targetAttribute);
            req->set_value(ToProto(NYson::ConvertToYsonString(std::string())));
            auto* prerequisitesExt = req->Header()
                .MutableExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
            auto* prerequisiteRevision = prerequisitesExt->add_revisions();
            prerequisiteRevision->set_path(targetPath);
            prerequisiteRevision->set_revision(revision.Underlying());

            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        if (!batchRspOrError.IsOK()) {
            YT_LOG_ERROR(
                batchRspOrError,
                "Failed to reset expiration (NodeId: %v)",
                trunkNodeId);
            return;
        }

        auto rsps = batchRspOrError.Value()->GetResponses<TYPathProxy::TRspRemove>();
        YT_VERIFY(rsps.size() == attributesToRemove.size());
        for (const auto& rspOrError : rsps) {
            if (!rspOrError.IsOK()) {
                YT_LOG_ERROR(
                    rspOrError,
                    "Failed to reset expiration (NodeId: %v)",
                    trunkNodeId);
                return;
            }
        }

        YT_LOG_DEBUG(
            "Expiration was reset (NodeId: %v)",
            trunkNodeId);
        FailedExpirationAttempts_.erase(TFailedAttemptDescriptorView(trunkNode.Get(), user));
    }

    const TDynamicCypressManagerConfigPtr& GetDynamicConfig()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->CypressManager;
    }

    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr /*oldConfig*/)
    {
        const auto& config = GetDynamicConfig();
        if (CheckExecutor_) {
            CheckExecutor_->SetPeriod(config->ExpirationCheckPeriod);
        }
        if (UpdatePersistentFailedExpirationAttemptsExecutor_) {
            UpdatePersistentFailedExpirationAttemptsExecutor_->SetPeriod(config->ExpirationAttemptPersistPeriod);
        }
    }

    void SaveKeys(NCellMaster::TSaveContext& /*context*/)
    { }

    void SaveValues(NCellMaster::TSaveContext& context)
    {
        using NYT::Save;
        THashMap<TFailedAttemptDescriptorView, int> rawPersistentFailedExpirationAttempts;
        rawPersistentFailedExpirationAttempts.reserve(PersistentFailedExpirationAttempts_.size());
        for (const auto& [key, attempts] : PersistentFailedExpirationAttempts_) {
            rawPersistentFailedExpirationAttempts.emplace(
                TFailedAttemptDescriptorView(key.first.Get(), key.second.Get()), attempts);
        }
        Save(context, rawPersistentFailedExpirationAttempts);
    }

    void LoadKeys(NCellMaster::TLoadContext& /*context*/)
    { }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        if (context.GetVersion() < EMasterReign::AuthorizedExpiration) {
            return;
        }

        using NYT::Load;
        Load(context, PersistentFailedExpirationAttempts_);
    }

    void UpdatePersistentFailedExpirationAttempts()
    {
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        if (!hydraManager->IsActiveLeader()) {
            return;
        }

        if (PersistentFailedExpirationAttempts_.empty() && FailedExpirationAttempts_.empty()) {
            return;
        }

        NProto::TReqUpdateFailedExpirationAttempts request;
        for (const auto& [key, attempts] : FailedExpirationAttempts_) {
            const auto& [node, user] = key;

            auto update = request.add_attempts();
            ToProto(update->mutable_node_id(), node->GetId());
            ToProto(update->mutable_user_id(), user->GetId());
            update->set_attempts(attempts);
        }

        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger()));
    }

    void HydraUpdateFailedExpirationAttempts(NProto::TReqUpdateFailedExpirationAttempts* request)
    {
        YT_VERIFY(HasMutationContext());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        PersistentFailedExpirationAttempts_.clear();
        for (const auto& nodeAttempts : request->attempts()) {
            auto nodeId = FromProto<TNodeId>(nodeAttempts.node_id());
            auto userId = FromProto<TUserId>(nodeAttempts.user_id());
            auto node = cypressManager->FindNode(TVersionedNodeId(nodeId));
            auto user = securityManager->FindUser(userId);
            if (!IsObjectAlive(node) || !IsObjectAlive(user)) {
                continue;
            }

            auto failedAttemptDescriptor = TFailedAttemptPersistentDescriptor(node, user);
            PersistentFailedExpirationAttempts_.insert_or_assign(
                std::move(failedAttemptDescriptor), nodeAttempts.attempts());
        }

        YT_LOG_DEBUG("Persisted expiration failed attempts (Count: %v)",
            std::ssize(PersistentFailedExpirationAttempts_));
    }
};

////////////////////////////////////////////////////////////////////////////////

IExpirationTrackerPtr CreateExpirationTracker(NCellMaster::TBootstrap* bootstrap)
{
    return New<TExpirationTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
