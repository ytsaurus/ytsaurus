#pragma once

#include "private.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGlobalSequoiaChunkRefreshShardStatus,
    (Disabled)
    (Running)
    (Completed)
    (Stopped)
);

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaChunkRefresherShardStatus
    : public NYTree::TYsonStructLite
{
    bool Active = false;
    i64 Epoch = 0;

    EGlobalSequoiaChunkRefreshShardStatus GlobalRefreshStatus = EGlobalSequoiaChunkRefreshShardStatus::Disabled;
    i64 GlobalRefreshChunksProcessed = 0;
    TChunkId GlobalRefreshLastProcessedChunkId = NullChunkId;

    REGISTER_YSON_STRUCT_LITE(TSequoiaChunkRefresherShardStatus);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaChunkRefresherStatus
    : public NYTree::TYsonStructLite
{
    std::array<TSequoiaChunkRefresherShardStatus, ChunkShardCount> Shards;

    bool SequoiaChunkRefreshEnabled = false;
    i64 SequoiaChunkRefreshEpoch = 0;

    bool GlobalRefreshEnabled = false;
    i64 GlobalRefreshEpoch = 0;
    i64 GlobalRefreshIterations = 0;
    i64 GlobalRefreshChunksProcessed = 0;

    bool LocationRefreshEnabled = false;
    int AwaitingRefreshLocationCount = 0;

    REGISTER_YSON_STRUCT_LITE(TSequoiaChunkRefresherStatus);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaChunkRefresher
    : public TRefCounted
{
    virtual void AdjustRefresherState() = 0;

    virtual void RefreshNode(const TNode* node) = 0;

    virtual TSequoiaChunkRefresherStatus GetStatus() const = 0;

    virtual void OnProfiling(NProfiling::TSensorBuffer* buffer) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaChunkRefresher)

////////////////////////////////////////////////////////////////////////////////

ISequoiaChunkRefresherPtr CreateSequoiaChunkRefresher(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
