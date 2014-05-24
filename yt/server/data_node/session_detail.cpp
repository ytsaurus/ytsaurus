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
    : Config_(config)
    , Bootstrap_(bootstrap)
    , ChunkId_(chunkId)
    , Type_(type)
    , SyncOnClose_(syncOnClose)
    , Location_(location)
    , WriteInvoker_(CreateSerializedInvoker(Location_->GetWriteInvoker()))
    , Logger(DataNodeLogger)
    , Profiler(location->Profiler())
{
    YCHECK(bootstrap);
    YCHECK(location);

    Logger.AddTag(Sprintf("LocationId: %s, ChunkId: %s",
        ~Location_->GetId(),
        ~ToString(ChunkId_)));

    Location_->UpdateSessionCount(+1);
}

TSession::~TSession()
{
    Location_->UpdateSessionCount(-1);
}

void TSession::Ping()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TLeaseManager::RenewLease(Lease_);
}

const TChunkId& TSession::GetChunkId() const
{
    return ChunkId_;
}

EWriteSessionType TSession::GetType() const
{
    return Type_;
}

TLocationPtr TSession::GetLocation() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Location_;
}

void TSession::Start(TLeaseManager::TLease lease)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_DEBUG("Session started (SessionType: %s)",
        ~ToString(Type_));

    Lease_ = lease;
}

void TSession::CloseLease()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TLeaseManager::CloseLease(Lease_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
