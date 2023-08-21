#pragma once

#include "node.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TLinkNode
    : public TCypressNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, TargetPath);

public:
    using TCypressNode::TCypressNode;

    NYTree::ENodeType GetNodeType() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    static NYPath::TYPath ComputeEffectiveTargetPath(
        const NYPath::TYPath& targetPath,
        TCypressShard* shard);

    NYPath::TYPath ComputeEffectiveTargetPath() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
