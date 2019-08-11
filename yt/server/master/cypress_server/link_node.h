#pragma once

#include "node_detail.h"

#include <yt/core/ypath/public.h>

#include <yt/server/master/cell_master/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TLinkNode
    : public TCypressNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, TargetPath);

public:
    using TCypressNode::TCypressNode;

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    static NYPath::TYPath ComputeEffectiveTargetPath(
        const NYPath::TYPath& targetPath,
        TCypressShard* shard);

    NYPath::TYPath ComputeEffectiveTargetPath() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
