#include "group.h"

#include <yt/core/ytree/attributes.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/ytlib/discovery_client/helpers.h>

namespace NYT::NDiscoveryServer {

using namespace NConcurrency;
using namespace NDiscoveryClient;

////////////////////////////////////////////////////////////////////////////////

TGroup::TGroup(
    const TGroupId& id,
    TClosure onGroupEmptied,
    const NLogging::TLogger& Logger)
    : Id_(id)
    , OnGroupEmptied_(std::move(onGroupEmptied))
    , Logger(NLogging::TLogger(Logger)
        .AddTag("GroupId: %v", Id_))
{ }

TMemberPtr TGroup::AddOrUpdateMember(const TMemberInfo& memberInfo, TDuration leaseTimeout)
{
    YT_LOG_DEBUG("Updating member (MemberId: %v, LeaseTimeout: %v)",
        memberInfo.Id,
        leaseTimeout);

    {
        TReaderGuard readerGuard(MembersLock_);

        auto it = IdToMember_.find(memberInfo.Id);
        if (it != IdToMember_.end() && it->second->GetPriority() == memberInfo.Priority) {
            // Fast path.
            const auto& member = it->second;
            UpdateMemberInfo(member, memberInfo, leaseTimeout);
            return member;
        }
    }

    // Slow path.
    TWriterGuard writerGuard(MembersLock_);
    return UpdateMember(memberInfo, leaseTimeout);
}

TMemberPtr TGroup::AddMember(const TMemberInfo& memberInfo, TDuration leaseTimeout)
{
    VERIFY_WRITER_SPINLOCK_AFFINITY(MembersLock_);

    auto onMemberLeaseExpired = BIND([=, memberId = memberInfo.Id, this_ = MakeStrong(this)] {
        YT_LOG_DEBUG("Member lease expired (MemberId: %v)", memberId);

        TWriterGuard guard(MembersLock_);

        auto it = IdToMember_.find(memberId);
        if (it == IdToMember_.end()) {
            YT_LOG_WARNING("Member is already deleted from group (MemberId: %v)", memberId);
            return;
        }

        YT_VERIFY(Members_.erase(it->second) > 0);
        IdToMember_.erase(it);

        if (Members_.empty()) {
            guard.Release();
            OnGroupEmptied_();
        }
    });

    auto member = New<TMember>(memberInfo.Id, Id_, leaseTimeout, std::move(onMemberLeaseExpired));
    member->UpdatePriority(memberInfo.Priority);
    UpdateMemberInfo(member, memberInfo, leaseTimeout);

    Members_.insert(member);
    IdToMember_.emplace(member->GetId(), member);

    YT_LOG_DEBUG("Member added (MemberId: %v, LeaseTimeout: %v)",
        memberInfo.Id,
        leaseTimeout);

    return member;
}

void TGroup::UpdateMemberInfo(const TMemberPtr& member, const TMemberInfo& memberInfo, TDuration leaseTimeout)
{
    member->RenewLease(leaseTimeout);

    {
        auto writer = member->CreateWriter();
        if (memberInfo.Attributes) {
            writer.GetAttributes()->Clear();
            writer.GetAttributes()->MergeFrom(*memberInfo.Attributes);
        }
        writer.SetRevision(memberInfo.Revision);
    }
}

TMemberPtr TGroup::UpdateMember(const TMemberInfo& memberInfo, TDuration leaseTimeout)
{
    VERIFY_WRITER_SPINLOCK_AFFINITY(MembersLock_);

    auto it = IdToMember_.find(memberInfo.Id);
    if (it == IdToMember_.end()) {
        return AddMember(memberInfo, leaseTimeout);
    }

    const auto& member = it->second;
    Members_.erase(member);
    member->UpdatePriority(memberInfo.Priority);
    Members_.insert(member);

    UpdateMemberInfo(member, memberInfo, leaseTimeout);

    return member;
}

std::vector<TMemberPtr> TGroup::ListMembers(std::optional<int> limit)
{
    TReaderGuard guard(MembersLock_);

    if (!limit) {
        return {Members_.begin(), Members_.end()};
    }

    auto it = Members_.begin();
    std::advance(it, std::min<int>(std::max(0, *limit), Members_.size()));
    return {Members_.begin(), it};
}

bool TGroup::HasMember(const TMemberId& memberId)
{
    TReaderGuard guard(MembersLock_);

    return IdToMember_.contains(memberId);
}

TMemberPtr TGroup::FindMember(const TMemberId& memberId)
{
    TReaderGuard guard(MembersLock_);

    auto it = IdToMember_.find(memberId);
    return it == IdToMember_.end() ? nullptr : it->second;
}

TMemberPtr TGroup::GetMemberOrThrow(const TMemberId& memberId)
{
    auto member = FindMember(memberId);
    if (!member) {
        THROW_ERROR_EXCEPTION(NDiscoveryServer::EErrorCode::NoSuchMember,
            "No member %Qv in group %v",
            memberId,
            Id_);
    }
    return member;
}

const TGroupId& TGroup::GetId()
{
    return Id_;
}

int TGroup::GetMemberCount()
{
    TReaderGuard guard(MembersLock_);
    return static_cast<int>(Members_.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer

