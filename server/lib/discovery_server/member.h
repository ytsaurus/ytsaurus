#pragma once

#include "public.h"

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TMember
    : public TRefCounted
{
public:
    TMember(
        TMemberId memberId,
        TGroupId groupId,
        TDuration leaseTimeout,
        TClosure onLeaseExpired);

    class TAttributeReader
    {
    public:
        const NYTree::IAttributeDictionary* GetAttributes();
        i64 GetRevision();

    private:
        const TMemberPtr Member_;

        NConcurrency::TReaderGuard Guard_;

        friend class TMember;
        explicit TAttributeReader(TMemberPtr member);
    };

    class TAttributeWriter
    {
    public:
        NYTree::IAttributeDictionary* GetAttributes();
        void SetRevision(i64 revision);

    private:
        const TMemberPtr Member_;

        NConcurrency::TWriterGuard Guard_;

        friend class TMember;
        explicit TAttributeWriter(TMemberPtr member);
    };

    TAttributeReader CreateReader();
    TAttributeWriter CreateWriter();

    void RenewLease(TDuration timeout);
    bool UpdatePriority(i64 priority);

    const TMemberId& GetId() const;
    const TGroupId& GetGroupId() const;
    i64 GetPriority() const;
    TInstant GetLeaseDeadline() const;
    TInstant GetLastHeartbeatTime() const;
    TInstant GetLastAttributesUpdateTime() const;
    TInstant GetLastGossipAttributesUpdateTime() const;
    void SetLastGossipAttributesUpdateTime(TInstant lastGossipAttributesUpdateTime);

private:
    const TMemberId Id_;
    const TGroupId GroupId_;

    NConcurrency::TReaderWriterSpinLock Lock_;
    i64 Priority_ = 0;
    i64 Revision_ = 0;
    std::unique_ptr<NYTree::IAttributeDictionary> Attributes_;
    NConcurrency::TLease Lease_;
    TInstant LeaseDeadline_;
    TInstant LastHeartbeatTime_;
    TInstant LastAttributesUpdateTime_;
    TInstant LastGossipAttributesUpdateTime_;
};

DEFINE_REFCOUNTED_TYPE(TMember)

////////////////////////////////////////////////////////////////////////////////

struct TMemberPtrComparer
{
    bool operator()(const TMemberPtr& lhs, const TMemberPtr& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer

