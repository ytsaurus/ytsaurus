#pragma once

#include "public.h"

#include <server/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeBalancer
{
public:
    explicit TChunkTreeBalancer(NCellMaster::TBootstrap* bootstrap);

    bool IsRebalanceNeeded(TChunkList* root);
    void Rebalance(TChunkList* root);

private:
    NCellMaster::TBootstrap* Bootstrap;

    static const int MaxChunkTreeRank;
    static const int MinChunkListSize;
    static const int MaxChunkListSize;
    static const double MinChunkListToChunkRatio;

    void MergeChunkTrees(
        std::vector<TChunkTreeRef>* children,
        TChunkTreeRef child);

    void AppendChunkTree(
        std::vector<TChunkTreeRef>* children,
        TChunkTreeRef child);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
