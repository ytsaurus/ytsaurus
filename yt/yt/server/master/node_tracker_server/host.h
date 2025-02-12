#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class THost
    : public NObjectServer::TObject
    , public TRefTracked<THost>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::string, Name);
    DEFINE_BYVAL_RW_PROPERTY(TRackRawPtr, Rack);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TNodeRawPtr>, Nodes);

public:
    using TObject::TObject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
    NYPath::TYPath GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void AddNode(TNode* node);
    void RemoveNode(TNode* node);

    TCompactVector<TNode*, 1> GetNodesWithFlavor(ENodeFlavor flavor) const;
};

DEFINE_MASTER_OBJECT_TYPE(THost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
