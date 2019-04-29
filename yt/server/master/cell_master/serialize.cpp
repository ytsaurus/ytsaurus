#include "serialize.h"

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 830;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 711 || // shakurov
        version == 712 || // aozeritsky
        version == 713 || // savrus: Add tablet cell decommission
        version == 714 || // savrus: Change TReqKickOrphanedTabletActions
        version == 715 || // ifsmirnov: Fix tablet_error_count lag
        version == 716 || // savrus: Add dynamic tablet cell options
        version == 717 || // aozeritsky: Add replicated table options
        version == 718 || // shakurov: weak ghosts save/load
        version == 800 || // savrus: Multicell for dynamic tables
        version == 801 || // savrus: Make tablet_state backward-compatible
        version == 802 || // aozeritsky: Add replica options
        version == 803 || // savrus: Add primary last mount transaction id
        version == 804 || // shakurov: Remove TTransaction::System
        version == 805 || // psushin: Add cypress annotations
        version == 806 || // shakurov: same as ver. 718, but in 19.4
        version == 807 || // savrus: Add tablet cell health to tablet cell statistics
        version == 808 || // savrus: Forward start prerequisite transaction to secondary master
        version == 809 || // shakurov: Persist requisition update requests
        version == 810 || // ignat: Persist transaction deadline
        version == 811 || // aozeritsky: Add attributes_revision, content_revision
        version == 812 || // savrus: add reassign peer mutation
        version == 813 || // aozeritsky: YT-9775: master-master protocol change
        version == 814 || // aozeritsky
        version == 815 || // aozeritsky: Add read_request_rate_limit and write_request_rate_limit
        version == 816 || // shakurov: initialize medium-specific max_replication_factor
        version == 817 || // shakurov: persist TNode::Resource{Usage,Limits}_
        version == 818 || // shakurov: int -> i64 for NSecurityServer::TClusterResources::{Node,Chunk}Count
        version == 819 || // savrus: Add tablet cell life stage
        version == 820 || // savrus: Fix snapshot
        version == 821 || // ifsmirnov: Per-table tablet balancer config
        version == 822 || // savrus: Use current mount transaction id to lock table node during mount
        version == 823 || // ifsmirnov: Synchronous handles for tablet balancer
        version == 824 || // savrus: Remove dynamic table attrs from static tables
        version == 825 || // shakurov: In TChunkReplication, replace array with SmallVector
        version == 826 || // babenko: columnar ACLs
        version == 827 || // babenko: security tags
        version == 828 || // ifsmirnov: TCumulativeStatistics in chunk lists
        version == 829 || // shakurov: multiply TUser::ReadRequestRateLimit_ by the number of followers
        version == 830 || // ifsmirnov: Chunk view
        false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
