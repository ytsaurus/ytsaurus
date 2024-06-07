#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TErrorManagerContext
{
    std::optional<TString> TabletCellBundle;
    NTableClient::TTableId TableId;
    NTabletClient::TTabletId TabletId;

    operator bool() const;
    void Reset();
};

void SetErrorManagerContext(TErrorManagerContext context);
void SetErrorManagerContextFromTabletSnapshot(const TTabletSnapshotPtr& tabletSnapshot);
void ResetErrorManagerContext();
TError EnrichErrorForErrorManager(TError&& error, const TTabletSnapshotPtr& tabletSnapshot);

////////////////////////////////////////////////////////////////////////////////

struct IErrorManager
    : public virtual TRefCounted
{
    virtual void Start() = 0;
    virtual void Reconfigure(const NClusterNode::TClusterNodeDynamicConfigPtr& newConfig) = 0;
    virtual void HandleError(const TError& error, const TString& method) = 0;
};

DEFINE_REFCOUNTED_TYPE(IErrorManager)

IErrorManagerPtr CreateErrorManager(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
