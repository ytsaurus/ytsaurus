#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReplicaAnnouncement
{
    NChunkClient::TChunkId ChunkId;
    NChunkClient::TChunkReplicaWithMediumList Replicas;
    NHydra::TRevision Revision;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 * Ally replica manager is capable of knowing, for each chunk stored at the node,
 * where other replicas of that chunk are (so-called ally replicas).
 * This knowledge is eventually consistent with the master (typically within 1-2
 * incremental heartbeat periods).
 *
 * All methods are thread-safe, however |ScheduleAnnouncements| and
 * |TakeUnconfirmedAnnouncementRequests| acquire global lock and thus are discouraged
 * to be called concurrently.
 */
struct IAllyReplicaManager
    : public TRefCounted
{
    virtual void Start() = 0;

    virtual void ScheduleAnnouncements(
        TRange<const NChunkClient::NProto::TChunkReplicaAnnouncementRequest*> requests,
        NHydra::TRevision revision,
        bool onFullHeartbeat) = 0;

    virtual void OnAnnouncementsReceived(
        TRange<const NChunkClient::NProto::TChunkReplicaAnnouncement*> announcements,
        NNodeTrackerClient::TNodeId sourceNodeId) = 0;

    virtual std::vector<std::pair<TChunkId, NHydra::TRevision>>
        TakeUnconfirmedAnnouncementRequests(NObjectClient::TCellTag cellTag) = 0;

    virtual void SetEnableLazyAnnouncements(bool enable) = 0;

    virtual NChunkClient::TAllyReplicasInfo GetAllyReplicas(TChunkId chunkId) const = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    virtual void BuildChunkOrchidYson(NYTree::TFluentMap fluent, TChunkId chunkId) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IAllyReplicaManager)

////////////////////////////////////////////////////////////////////////////////

IAllyReplicaManagerPtr CreateAllyReplicaManager(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
