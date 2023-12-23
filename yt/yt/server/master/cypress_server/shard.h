#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TCypressShardAccountStatistics
{
    int NodeCount = 0;

    bool IsZero() const;

    void Persist(const NCellMaster::TPersistenceContext& context);
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
    : public NObjectServer::TObject
    , public TRefTracked<TCypressShard>
{
public:
    using TAccountStatistics = THashMap<NSecurityServer::TAccountPtr, TCypressShardAccountStatistics>;
    DEFINE_BYREF_RW_PROPERTY(TAccountStatistics, AccountStatistics);

    DEFINE_BYVAL_RW_PROPERTY(TCypressNode*, Root);

    DEFINE_BYVAL_RW_PROPERTY(TString, Name);

public:
    using TObject::TObject;

    TCypressShardAccountStatistics ComputeTotalAccountStatistics() const;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    NYPath::TYPath MaybeRewritePath(const NYPath::TYPath& path);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
