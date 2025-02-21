#include "group_coordinator.h"

#include "config.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/guid.h>

#include <atomic>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NKafka;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGroupCoordinatorState,
    (Ok)
    (JoinInProgress)
    (JoinFinished)
    (SyncInProgress)
);

////////////////////////////////////////////////////////////////////////////////

class TGroupCoordinator
    : public IGroupCoordinator
{
public:
    TGroupCoordinator(TString groupId, TGroupCoordinatorConfigPtr config)
        : GroupId_(std::move(groupId))
        , DynamicConfig_(std::move(config))
    { }

    TRspJoinGroup JoinGroup(const NKafka::TReqJoinGroup& request, const NLogging::TLogger& Logger) override
    {
        auto checkState = [&] {
            auto state = State_.load();
            auto acceptNew = AcceptNew_.load();

            if (state == EGroupCoordinatorState::Ok || (state == EGroupCoordinatorState::JoinInProgress && acceptNew)) {
                return std::optional<TRspJoinGroup>(std::nullopt);
            }

            YT_LOG_DEBUG("JoinGroup request cannot be accepted now (MemberId: %v, State: %v, AcceptNew: %v)",
                request.MemberId,
                state,
                acceptNew);
            return std::make_optional(TRspJoinGroup{ .ErrorCode = NKafka::EErrorCode::RebalanceInProgress });
        };

        auto checkStateResponse = checkState();
        if (checkStateResponse) {
            return *checkStateResponse;
        }

        TDuration waitDuration = TDuration::Zero();
        TString memberId;
        bool isLeader = false;
        auto rebalanceTimeout = GetDynamicConfig()->RebalanceTimeout;

        {
            auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&Lock_))
                .ValueOrThrow();

            auto checkStateResponse = checkState();
            if (checkStateResponse) {
                return *checkStateResponse;
            }

            memberId = request.MemberId;
            if (memberId.empty()) {
                memberId = NYT::ToString(TGuid::Create());
                YT_LOG_DEBUG("Member id is assigned (MemberId: %v)", memberId);
            }

            // First member resets everything, becomes a new leader and waits JoinGroup request from the rest of the members.
            if (State_ == EGroupCoordinatorState::Ok) {
                Reset();
                JoinDeadline_ = TInstant::Now() + rebalanceTimeout;
                State_ = EGroupCoordinatorState::JoinInProgress;
                AcceptNew_ = true;
                LeaderMemberId_ = memberId;
                isLeader = true;
            }
            waitDuration = JoinDeadline_ - TInstant::Now();

            JoinedMembers_[memberId] = TMember{
                .Protocols = request.Protocols,
            };

            for (const auto& protocol : request.Protocols) {
                ++ProtocolNameToMemberCount_[protocol.Name];
            }

            YT_LOG_DEBUG("Group member joined (MemberId: %v, JoinDeadline: %v)",
                memberId,
                JoinDeadline_);
        }

        YT_LOG_DEBUG("Waiting for the rest of the members to join (MemberId: %v, WaitDuration: %v)", memberId, waitDuration);

        TDelayedExecutor::WaitForDuration(waitDuration);

        YT_LOG_DEBUG("Wait finished (MemberId: %v)", memberId);

        auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&Lock_))
            .ValueOrThrow();

        // JoinGroup requests will not be accepted during this rebalance.
        if (isLeader) {
            AcceptNew_.store(false);
        }

        if (!isLeader) {
            YT_LOG_DEBUG("Waiting JoinGroup response from the leader (MemberId: %v)", memberId);
            auto response = WaitFor(JoinGroupResponsePromise_.ToFuture()
                .WithTimeout(TDuration::Seconds(1)))  // TODO: config
                .ValueOrThrow();
            response.MemberId = memberId;
            return response;
        }

        YT_LOG_DEBUG("Making JoinGroup response (MemberId: %v)", memberId);

        std::optional<TString> commonProtocolName;
        for (const auto& [protocolName, memberCount] : ProtocolNameToMemberCount_) {
            if (memberCount == std::ssize(JoinedMembers_)) {
                commonProtocolName = protocolName;
                break;
            }
        }

        if (!commonProtocolName) {
            YT_LOG_DEBUG("There is no common protocol (MemberId: %v)", memberId);
            auto response = TRspJoinGroup{ .ErrorCode = NKafka::EErrorCode::InconsistentGroupProtocol };
            JoinGroupResponsePromise_.TrySet(response);
            return response;
        }

        TRspJoinGroup response;
        response.MemberId = memberId;
        response.Leader = LeaderMemberId_;
        response.GenerationId = GenerationId_;
        response.ProtocolName = *commonProtocolName;

        response.Members.reserve(JoinedMembers_.size());
        for (const auto& [memberId, member] : JoinedMembers_) {
            TString metadata;
            for (const auto& protocol : member.Protocols) {
                if (protocol.Name == *commonProtocolName) {
                    metadata = protocol.Metadata;
                }
            }

            response.Members.push_back(TRspJoinGroupMember{
                .MemberId = memberId,
                .Metadata = metadata,
            });
        }

        State_.store(EGroupCoordinatorState::JoinFinished);
        AcceptNew_.store(true);

        JoinGroupResponsePromise_.TrySet(response);

        YT_LOG_DEBUG("Group leader made a JoinGroup response (MemberId: %v)", memberId);

        return response;
    }

    TRspSyncGroup SyncGroup(const NKafka::TReqSyncGroup& request, const NLogging::TLogger& Logger) override
    {
        auto checkState = [&] {
            auto state = State_.load();
            auto acceptNew = AcceptNew_.load();

            if (state == EGroupCoordinatorState::JoinFinished || (state == EGroupCoordinatorState::SyncInProgress && acceptNew)) {
                return std::optional<TRspSyncGroup>(std::nullopt);
            }

            YT_LOG_DEBUG("SyncGroup request cannot be accepted now (State: %v, AcceptNew: %v)",
                state,
                acceptNew);

            return std::make_optional(TRspSyncGroup{ .ErrorCode = NKafka::EErrorCode::RebalanceInProgress });
        };

        auto checkStateResponse = checkState();
        if (checkStateResponse) {
            return *checkStateResponse;
        }

        TDuration waitDuration = TDuration::Zero();
        auto rebalanceTimeout = GetDynamicConfig()->RebalanceTimeout;

        {
            auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&Lock_))
                .ValueOrThrow();

            auto checkStateResponse = checkState();
            if (checkStateResponse) {
                return *checkStateResponse;
            }

            if (GenerationId_ != request.GenerationId) {
                YT_LOG_DEBUG("Invalid generation (CurrentGenerationId: %v, RequestGenerationId: %v)",
                    GenerationId_,
                    request.GenerationId);
                return TRspSyncGroup{ .ErrorCode = NKafka::EErrorCode::IllegalGeneration };
            }

            if (request.MemberId == LeaderMemberId_) {
                for (const auto& assignment : request.Assignments) {
                    Assignments_[assignment.MemberId] = assignment.Assignment;
                }
            }

            SyncedMembers_.insert(request.MemberId);

            // Init last heartbeat time.
            JoinedMembers_[request.MemberId].LastHeartbeat = TInstant::Now();

            if (State_ == EGroupCoordinatorState::JoinFinished) {
                State_ = EGroupCoordinatorState::SyncInProgress;
                SyncDeadline_ = TInstant::Now() + rebalanceTimeout;
            }

            waitDuration = SyncDeadline_ - TInstant::Now();
        }

        YT_LOG_DEBUG("Waiting for the rest of the members to sync (WaitDuration: %v)", waitDuration);

        TDelayedExecutor::WaitForDuration(waitDuration);

        YT_LOG_DEBUG("Wait finished");

        auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&Lock_))
            .ValueOrThrow();

        if (GenerationId_ != request.GenerationId) {
            YT_LOG_DEBUG("During sync other generation started (CurrentGenerationId: %v, RequestGenerationId: %v",
                GenerationId_,
                request.GenerationId);
            return TRspSyncGroup{ .ErrorCode = NKafka::EErrorCode::IllegalGeneration };
        }

        // New SyncGroup requests will not be accepted during this rebalance.
        AcceptNew_.store(false);
        // New JoinGroup requests can be accepted after it. Old SyncGroup requests will have response with error
        // due to invalid generation id.
        State_.store(EGroupCoordinatorState::Ok);

        if (Assignments_.empty()) {
            YT_LOG_DEBUG("Assignments are empty");
            return TRspSyncGroup{ .ErrorCode = NKafka::EErrorCode::UnknownServerError };
        }

        for (const auto& [memberId, _] : JoinedMembers_) {
            if (SyncedMembers_.find(memberId) == SyncedMembers_.end()) {
                YT_LOG_DEBUG("Some joined members were not synced (MissedMemberId: %v)", memberId);
                return TRspSyncGroup{ .ErrorCode = NKafka::EErrorCode::UnknownServerError };
            }
        }

        auto assignmentIt = Assignments_.find(request.MemberId);
        if (assignmentIt == Assignments_.end()) {
            YT_LOG_DEBUG("There is no assignment");
            return TRspSyncGroup{ .ErrorCode = NKafka::EErrorCode::UnknownServerError };
        }

        TRspSyncGroup response;
        response.Assignment = assignmentIt->second;

        return response;
    }

    TRspHeartbeat Heartbeat(const NKafka::TReqHeartbeat& request, const NLogging::TLogger& Logger) override
    {
        auto checkState = [&] {
            auto state = State_.load();

            if (state != EGroupCoordinatorState::Ok) {
                YT_LOG_DEBUG("Rebalance in progress (State: %v)", state);
                return std::make_optional(TRspHeartbeat{ .ErrorCode = NKafka::EErrorCode::RebalanceInProgress });
            }

            return std::optional<TRspHeartbeat>(std::nullopt);
        };

        auto checkStateResponse = checkState();
        if (checkStateResponse) {
            return *checkStateResponse;
        }

        auto now = TInstant::Now();

        {
            auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&Lock_))
                .ValueOrThrow();

            auto checkStateResponse = checkState();
            if (checkStateResponse) {
                return *checkStateResponse;
            }

            if (request.GenerationId != GenerationId_) {
                YT_LOG_DEBUG("Heartbeat with invalid generation id (GenerationId: %v)",
                    request.GenerationId);
                return TRspHeartbeat{ .ErrorCode = NKafka::EErrorCode::RebalanceInProgress };
            }

            auto joinedMembersIt = JoinedMembers_.find(request.MemberId);
            if (joinedMembersIt == JoinedMembers_.end()) {
                YT_LOG_DEBUG("Heartbeat from not joined member");
                return TRspHeartbeat{ .ErrorCode = NKafka::EErrorCode::RebalanceInProgress };
            }

            joinedMembersIt->second.LastHeartbeat = now;
        }

        auto sessionTimeout = GetDynamicConfig()->SessionTimeout;

        {
            auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&Lock_))
                .ValueOrThrow();

            // TODO(nadya73): Rewrite this code. Now every heartbeat is checking if there is an inactive member.
            for (const auto& [memberId, member] : JoinedMembers_) {
                if (now - member.LastHeartbeat > sessionTimeout) {
                    YT_LOG_DEBUG("There are no hearbeats for too long (InactiveMemberId: %v, LastHeartbeat: %v, Now: %v, SessionTimeout: %v)",
                        memberId,
                        member.LastHeartbeat,
                        now,
                        sessionTimeout);
                    return TRspHeartbeat{ .ErrorCode = NKafka::EErrorCode::RebalanceInProgress };
                }
            }
        }

        return TRspHeartbeat{};
    }

    void Reconfigure(const TGroupCoordinatorConfigPtr& config) override
    {
        DynamicConfig_.Store(config);
    }

private:
    TGroupId GroupId_;
    TAtomicIntrusivePtr<TGroupCoordinatorConfig> DynamicConfig_;

    const NLogging::TLogger Logger;

    std::atomic<EGroupCoordinatorState> State_{EGroupCoordinatorState::Ok};
    std::atomic<bool> AcceptNew_ = true;

    TPromise<TRspJoinGroup> JoinGroupResponsePromise_;

    TInstant JoinDeadline_ = TInstant::Max();
    TInstant SyncDeadline_ = TInstant::Max();

    struct TMember
    {
        std::vector<TReqJoinGroupProtocol> Protocols;

        TInstant LastHeartbeat = TInstant::Max();
    };

    NConcurrency::TAsyncReaderWriterLock Lock_;

    int GenerationId_ = 0;

    TString LeaderMemberId_;
    THashMap<TMemberId, TMember> JoinedMembers_;
    THashMap<TString, i64> ProtocolNameToMemberCount_;

    THashSet<TMemberId> SyncedMembers_;
    THashMap<TMemberId, TString> Assignments_;

    void Reset()
    {
        JoinedMembers_.clear();
        SyncedMembers_.clear();
        ProtocolNameToMemberCount_.clear();
        Assignments_.clear();
        ++GenerationId_;
        JoinGroupResponsePromise_ = NewPromise<TRspJoinGroup>();
    }

    TGroupCoordinatorConfigPtr GetDynamicConfig() const
    {
        return DynamicConfig_.Acquire();
    }
};

////////////////////////////////////////////////////////////////////////////////

IGroupCoordinatorPtr CreateGroupCoordinator(TString groupId, TGroupCoordinatorConfigPtr config)
{
    return New<TGroupCoordinator>(std::move(groupId), std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
