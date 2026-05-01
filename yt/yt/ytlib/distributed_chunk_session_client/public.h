#pragma once

#include <yt/yt/core/misc/error_code.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDistributedChunkSessionController)
DECLARE_REFCOUNTED_STRUCT(IDistributedChunkSessionPool)
DECLARE_REFCOUNTED_STRUCT(IDistributedChunkWriter)

DECLARE_REFCOUNTED_STRUCT(TDistributedChunkSessionControllerConfig)
DECLARE_REFCOUNTED_STRUCT(TDistributedChunkSessionPoolConfig)
DECLARE_REFCOUNTED_STRUCT(TDistributedChunkWriterConfig)

DECLARE_REFCOUNTED_STRUCT(IDistributedChunkSessionReader)
DECLARE_REFCOUNTED_STRUCT(TDistributedChunkSessionReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TDistributedChunkSessionReaderStatistics)

using TDistributedChunkSessionReaderStatisticsConstPtr =
    TIntrusivePtr<const TDistributedChunkSessionReaderStatistics>;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, DistributedChunkSessionLogger, "DistributedChunkSessionClient");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
