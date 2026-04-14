#pragma once

#include "private.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGlobalSequoiaChunkRefreshStatus,
    (Disabled)
    (Running)
    (Completed)
    (Aborted)
);

////////////////////////////////////////////////////////////////////////////////

struct TGlobalSequoiaChunkRefreshStatus
    : public NYTree::TYsonStructLite
{
    EGlobalSequoiaChunkRefreshStatus Status = EGlobalSequoiaChunkRefreshStatus::Disabled;
    i64 Epoch = 0;

    i64 ChunksProcessed = 0;
    NChunkClient::TChunkId LastProcessedChunkId = NChunkClient::NullChunkId;
    i64 RefreshIterations = 0;

    REGISTER_YSON_STRUCT_LITE(TGlobalSequoiaChunkRefreshStatus);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct IGlobalSequoiaChunkRefresher
    : public TRefCounted
{
    virtual void AdjustRefresherState() = 0;

    virtual TGlobalSequoiaChunkRefreshStatus GetStatus() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IGlobalSequoiaChunkRefresher)

////////////////////////////////////////////////////////////////////////////////

IGlobalSequoiaChunkRefresherPtr CreateGlobalSequoiaChunkRefresher(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
