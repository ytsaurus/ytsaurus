#pragma once

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NPushBasedShuffleClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IPushBasedShuffleWriter)
DECLARE_REFCOUNTED_STRUCT(IPartitionWriteSessionProvider)
DECLARE_REFCOUNTED_STRUCT(TShuffleWriterConfig)

DECLARE_REFCOUNTED_STRUCT(IPushBasedPartitionReader)
DECLARE_REFCOUNTED_STRUCT(TShuffleReadBatch)
DECLARE_REFCOUNTED_STRUCT(TPartitionReaderConfig)

DECLARE_REFCOUNTED_STRUCT(TPushShuffleConfig)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, PushBasedShuffleLogger, "PushBasedShuffleClient");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
