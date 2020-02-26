#pragma once

#include "public.h"

#include <yt/server/master/object_server/object_detail.h>

#include <yt/server/master/security_server/public.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

#include <yt/core/yson/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TCypressShardAccountStatistics
{
    int NodeCount = 0;

    bool IsZero() const;

    void Persist(NCellMaster::TPersistenceContext& context);
};

void Serialize(const TCypressShardAccountStatistics& statistics, NYson::IYsonConsumer* consumer);

TCypressShardAccountStatistics& operator +=(
    TCypressShardAccountStatistics& lhs,
    const TCypressShardAccountStatistics& rhs);
TCypressShardAccountStatistics operator +(
    const TCypressShardAccountStatistics& lhs,
    const TCypressShardAccountStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

//! A shard is effectively a Cypress subtree.
//! The root of a shard is either the global Cypress root or a portal exit.
class TCypressShard
    : public NObjectServer::TNonversionedObjectBase
    , public TRefTracked<TCypressShard>
{
public:
    // NB: Pointers to accounts are strong references.
    using TAccountStatistics = THashMap<NSecurityServer::TAccount*, TCypressShardAccountStatistics>;
    DEFINE_BYREF_RW_PROPERTY(TAccountStatistics, AccountStatistics);

    DEFINE_BYVAL_RW_PROPERTY(TCypressNode*, Root);

    DEFINE_BYVAL_RW_PROPERTY(TString, Name);

public:
    using TNonversionedObjectBase::TNonversionedObjectBase;

    TCypressShardAccountStatistics ComputeTotalAccountStatistics() const;

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
