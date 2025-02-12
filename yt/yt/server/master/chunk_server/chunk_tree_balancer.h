#pragma once

#include "private.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct IChunkTreeBalancerCallbacks
    : public virtual TRefCounted
{
    virtual void RefObject(NObjectServer::TObject* object) = 0;
    virtual void UnrefObject(NObjectServer::TObject* object) = 0;
    virtual void FlushObjectUnrefs() = 0;
    virtual int GetObjectRefCounter(NObjectServer::TObject* object) = 0;

    virtual void ScheduleRequisitionUpdate(TChunkTree* chunkTree) = 0;

    virtual const TDynamicChunkTreeBalancerConfigPtr& GetConfig() const = 0;
    virtual TChunkList* CreateChunkList() = 0;
    virtual void ClearChunkList(TChunkList* chunkList) = 0;
    virtual void AttachToChunkList(
        TChunkList* chunkList,
        TRange<TChunkTreeRawPtr> children) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkTreeBalancerCallbacks)

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeBalancer
{
public:
    explicit TChunkTreeBalancer(IChunkTreeBalancerCallbacksPtr callbacks);

    bool IsRebalanceNeeded(TChunkList* root, EChunkTreeBalancerMode settingsMode);
    void Rebalance(TChunkList* root);

private:
    const IChunkTreeBalancerCallbacksPtr Callbacks_;

    void MergeChunkTrees(
        std::vector<TChunkTreeRawPtr>* children,
        TChunkTree* child);

    void AppendChunkTree(
        std::vector<TChunkTreeRawPtr>* children,
        TChunkTree* root);

    void AppendChild(
        std::vector<TChunkTreeRawPtr>* children,
        TChunkTree* child);

    const TDynamicChunkTreeBalancerConfigPtr& GetConfig() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
