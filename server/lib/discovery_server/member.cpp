#include "member.h"

#include <yt/core/concurrency/lease_manager.h>

#include <yt/core/ytree/helpers.h>

namespace NYT::NDiscoveryServer {

using namespace NProfiling;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TMember::TMember(
    TMemberId memberId,
    TGroupId groupId,
    TDuration leaseTimeout,
    TClosure onLeaseExpired)
    : Id_(std::move(memberId))
    , GroupId_(std::move(groupId))
    , Priority_(TInstant::Now().Seconds())
    , Attributes_(CreateEphemeralAttributes())
    , Lease_(TLeaseManager::CreateLease(
        leaseTimeout,
        std::move(onLeaseExpired)))
    , LeaseDeadline_(leaseTimeout.ToDeadLine())
{ }

TMember::TAttributeReader::TAttributeReader(TMemberPtr member)
    : Member_(std::move(member))
    , Guard_(Member_->Lock_)
{ }

const IAttributeDictionary* TMember::TAttributeReader::GetAttributes()
{
    return Member_->Attributes_.get();
}

i64 TMember::TAttributeReader::GetRevision()
{
    return Member_->Revision_;
}

TMember::TAttributeWriter::TAttributeWriter(TMemberPtr member)
    : Member_(std::move(member))
    , Guard_(Member_->Lock_)
{ }

IAttributeDictionary* TMember::TAttributeWriter::GetAttributes()
{
    Member_->LastAttributesUpdateTime_ = TInstant::Now();
    return Member_->Attributes_.get();
}

void TMember::TAttributeWriter::SetRevision(i64 revision)
{
    Member_->Revision_ = revision;
    Member_->LastHeartbeatTime_ = TInstant::Now();
}

TMember::TAttributeReader TMember::CreateReader()
{
    return TAttributeReader(this);
}

TMember::TAttributeWriter TMember::CreateWriter()
{
    return TAttributeWriter(this);
}

void TMember::RenewLease(TDuration timeout)
{
    auto newLeaseDeadline = timeout.ToDeadLine();

    TWriterGuard guard(Lock_);
    if (newLeaseDeadline > LeaseDeadline_) {
        LeaseDeadline_ = newLeaseDeadline;
        TLeaseManager::RenewLease(Lease_, timeout);
    }
}

bool TMember::UpdatePriority(i64 priority)
{
    TWriterGuard guard(Lock_);
    if (Priority_ == priority) {
        return false;
    }
    Priority_ = priority;
    return true;
}

const TMemberId& TMember::GetId() const
{
    return Id_;
}

const TGroupId& TMember::GetGroupId() const
{
    return GroupId_;
}

i64 TMember::GetPriority() const
{
    TReaderGuard guard(Lock_);
    return Priority_;
}

TInstant TMember::GetLeaseDeadline() const
{
    TReaderGuard guard(Lock_);
    return LeaseDeadline_;
}

TInstant TMember::GetLastHeartbeatTime() const
{
    TReaderGuard guard(Lock_);
    return LastHeartbeatTime_;
}

TInstant TMember::GetLastAttributesUpdateTime() const
{
    TReaderGuard guard(Lock_);
    return LastAttributesUpdateTime_;
}

TInstant TMember::GetLastGossipAttributesUpdateTime() const
{
    return LastGossipAttributesUpdateTime_;
}

void TMember::SetLastGossipAttributesUpdateTime(TInstant lastGossipAttributesUpdateTime)
{
    LastGossipAttributesUpdateTime_ = lastGossipAttributesUpdateTime;
}

////////////////////////////////////////////////////////////////////////////////

bool TMemberPtrComparer::operator()(const TMemberPtr& lhs, const TMemberPtr& rhs) const
{
    auto lhsPriority = lhs->GetPriority();
    auto rhsPriority = rhs->GetPriority();
    if (lhsPriority != rhsPriority) {
        return lhsPriority < rhsPriority;
    }
    return lhs->GetId() < rhs->GetId();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer

