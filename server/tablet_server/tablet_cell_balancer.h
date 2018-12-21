#pragma once

#include "public.h"

#include <yt/server/node_tracker_server/public.h>

#include <yt/server/hydra/entity_map.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellMoveDescriptor
{
    const TTabletCell* Cell;
    int PeerId;
    const NNodeTrackerServer::TNode* Source;
    const NNodeTrackerServer::TNode* Target;

    TTabletCellMoveDescriptor() = default;

    TTabletCellMoveDescriptor(
        const TTabletCell* cell,
        int peerId,
        const NNodeTrackerServer::TNode* source,
        const NNodeTrackerServer::TNode* target);

    bool operator<(const TTabletCellMoveDescriptor& other) const;
    bool operator==(const TTabletCellMoveDescriptor& other) const;
    bool operator!=(const TTabletCellMoveDescriptor& other) const;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeHolder
{
public:
    TNodeHolder(const NNodeTrackerServer::TNode* node, int totalSlots, const TTabletCellSet& slots);

    const NNodeTrackerServer::TNode* GetNode() const;
    int GetTotalSlots() const;
    const TTabletCellSet& GetSlots() const;
    std::pair<const TTabletCell*, int> ExtractCell(int cellIndex);
    void InsertCell(std::pair<const TTabletCell*, int> pair);
    void RemoveCell(const TTabletCell* cell);
    int GetCellCount(const TTabletCellBundle* bundle) const;

private:
    const NNodeTrackerServer::TNode* Node_;
    const int TotalSlots_;
    TTabletCellSet Slots_;
    THashMap<const TTabletCellBundle*, int> CellCount_;

    void CountCells();
};

////////////////////////////////////////////////////////////////////////////////

struct ITabletCellBalancerProvider
    : public TRefCounted
{
    virtual std::vector<TNodeHolder> GetNodes() = 0;
    virtual const NHydra::TReadOnlyEntityMap<TTabletCellBundle>& TabletCellBundles() = 0;
    virtual bool IsPossibleHost(const NNodeTrackerServer::TNode* node, const TTabletCellBundle* bundle) = 0;
    virtual bool IsVerboseLoggingEnabled() = 0;
    virtual bool IsBalancingRequired() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletCellBalancerProvider)

////////////////////////////////////////////////////////////////////////////////

struct ITabletCellBalancer
{
    virtual ~ITabletCellBalancer() = default;
    virtual void AssignPeer(const TTabletCell* cell, int peerId) = 0;
    virtual void RevokePeer(const TTabletCell* cell, int peerId) = 0;
    virtual std::vector<TTabletCellMoveDescriptor> GetTabletCellMoveDescriptors() = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ITabletCellBalancer> CreateTabletCellBalancer(
    ITabletCellBalancerProviderPtr provider);
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

