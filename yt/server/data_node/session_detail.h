#pragma once

#include "public.h"
#include "session.h"

#include <core/concurrency/thread_affinity.h>

#include <core/logging/tagged_logger.h>

#include <core/profiling/profiler.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TSession
    : public ISession
{
public:
    TSession(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap,
        const TChunkId& chunkId,
        EWriteSessionType type,
        bool syncOnClose,
        TLocationPtr location);

    ~TSession();

    virtual const TChunkId& GetChunkId() const override;
    virtual EWriteSessionType GetType() const override;
    TLocationPtr GetLocation() const override;

    virtual void Start(TLeaseManager::TLease lease) override;
    virtual void Ping() override;

    DEFINE_SIGNAL(void(const TError& error), Failed);
    DEFINE_SIGNAL(void(IChunkPtr chunk), Completed);

protected:
    TDataNodeConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;
    TChunkId ChunkId;
    EWriteSessionType Type;
    bool SyncOnClose;
    TLocationPtr Location;

    IInvokerPtr WriteInvoker;

    TLeaseManager::TLease Lease;

    NLog::TTaggedLogger Logger;
    NProfiling::TProfiler Profiler;

    
    void CloseLease();

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

