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
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);
    DEFINE_BYVAL_RW_PROPERTY(TRack*, Rack, nullptr);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TNode*>, Nodes);

public:
    using TObject::TObject;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void AddNode(TNode* node);
    void RemoveNode(TNode* node);

    TCompactVector<TNode*, 1> GetNodesWithFlavor(ENodeFlavor flavor) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
