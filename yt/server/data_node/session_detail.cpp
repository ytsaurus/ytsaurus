#include "stdafx.h"
#include "session_detail.h"
#include "session_manager.h"
#include "private.h"
#include "config.h"
#include "location.h"

#include <core/profiling/scoped_timer.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NRpc;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NCellNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TSession::TSession(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap,
    const TChunkId& chunkId,
    NChunkClient::EWriteSessionType type,
    bool syncOnClose,
    TLocationPtr location)
    : Config(config)
    , Bootstrap(bootstrap)
    , ChunkId(chunkId)
    , Type(type)
    , SyncOnClose(syncOnClose)
    , Location(location)
    , WriteInvoker(CreateSerializedInvoker(Location->GetWriteInvoker()))
    , Logger(DataNodeLogger)
    , Profiler(location->Profiler())
{
    YCHECK(bootstrap);
    YCHECK(location);

    Logger.AddTag(Sprintf("LocationId: %s, ChunkId: %s",
        ~Location->GetId(),
        ~ToString(ChunkId)));

    Location->UpdateSessionCount(+1);
}

TSession::~TSession()
{
    Location->UpdateSessionCount(-1);
}

void TSession::Ping()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TLeaseManager::RenewLease(Lease);
}

const TChunkId& TSession::GetChunkId() const
{
    return ChunkId;
}

EWriteSessionType TSession::GetType() const
{
    return Type;
}

TLocationPtr TSession::GetLocation() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Location;
}

void TSession::Start(TLeaseManager::TLease lease)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_DEBUG("Session started (SessionType: %s)",
        ~ToString(Type));

    Lease = lease;
}

void TSession::CloseLease()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TLeaseManager::CloseLease(Lease);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
