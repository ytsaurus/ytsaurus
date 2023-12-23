#pragma once

#include "public.h"

#include <yt/yt/server/node/tablet_node/object_detail.h>

#include <yt/yt/client/chaos_client/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EReplicationCardCollocationState,
    ((Normal)       (0))
    ((Emmigrating)  (1))
    ((Immigrating)  (2))
);

class TReplicationCardCollocation
    : public NTabletNode::TObjectBase
    , public TRefTracked<TReplicationCardCollocation>
{
public:
    using TReplicationCards = THashSet<TReplicationCard*>;
    DEFINE_BYREF_RW_PROPERTY(TReplicationCards, ReplicationCards);

    DEFINE_BYVAL_RW_PROPERTY(int, Size);
    DEFINE_BYVAL_RW_PROPERTY(EReplicationCardCollocationState, State);

public:
    using TObjectBase::TObjectBase;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    bool IsMigrating() const;
    void ValidateNotMigrating() const;

    std::vector<TReplicationCardId> GetReplicationCardIds() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode

