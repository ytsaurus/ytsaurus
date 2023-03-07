#pragma once

#include "public.h"

#include <yt/server/master/node_tracker_server/public.h>

#include <yt/server/lib/hydra/entity_map.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

struct TCellMoveDescriptor
{
    const TCellBase* Cell;
    int PeerId;
    const NNodeTrackerServer::TNode* Source;
    const NNodeTrackerServer::TNode* Target;
    TError Reason;

    TCellMoveDescriptor() = default;

    TCellMoveDescriptor(
        const TCellBase* cell,
        int peerId,
        const NNodeTrackerServer::TNode* source,
        const NNodeTrackerServer::TNode* target,
        TError reason);

    bool operator<(const TCellMoveDescriptor& other) const;
    bool operator==(const TCellMoveDescriptor& other) const;
    bool operator!=(const TCellMoveDescriptor& other) const;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeHolder
{
public:
    TNodeHolder(const NNodeTrackerServer::TNode* node, int totalSlots, const TCellSet& slots);

    const NNodeTrackerServer::TNode* GetNode() const;
    int GetTotalSlots() const;
    const TCellSet& GetSlots() const;
    std::optional<int> FindCell(const TCellBase* cell);
    std::pair<const TCellBase*, int> ExtractCell(int cellIndex);
    void InsertCell(std::pair<const TCellBase*, int> pair);
    std::pair<const TCellBase*, int> RemoveCell(const TCellBase* cell);
    int GetCellCount(const TCellBundle* bundle) const;

private:
    const NNodeTrackerServer::TNode* Node_;
    const int TotalSlots_;
    TCellSet Slots_;
    THashMap<const TCellBundle*, int> CellCount_;

    void UpdateCellCounts();
};

////////////////////////////////////////////////////////////////////////////////

struct ICellBalancerProvider
    : public TRefCounted
{
    virtual std::vector<TNodeHolder> GetNodes() = 0;
    virtual const NHydra::TReadOnlyEntityMap<TCellBundle>& CellBundles() = 0;
    virtual bool IsPossibleHost(const NNodeTrackerServer::TNode* node, const TCellBundle* bundle) = 0;
    virtual bool IsVerboseLoggingEnabled() = 0;
    virtual bool IsBalancingRequired() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellBalancerProvider)

////////////////////////////////////////////////////////////////////////////////

struct ICellBalancer
{
    virtual ~ICellBalancer() = default;
    virtual void AssignPeer(const TCellBase* cell, int peerId) = 0;
    virtual void RevokePeer(const TCellBase* cell, int peerId, const TError& reason) = 0;
    virtual std::vector<TCellMoveDescriptor> GetCellMoveDescriptors() = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ICellBalancer> CreateCellBalancer(
    ICellBalancerProviderPtr provider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
