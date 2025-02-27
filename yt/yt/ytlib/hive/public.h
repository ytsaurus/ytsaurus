#pragma once

#include <yt/yt/client/hive/public.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TCellPeerDescriptor;
class TCellDescriptor;
class TCellInfo;
class TEncapsulatedMessage;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using TMessageId = i64;

struct TCellInfo;
class TCellPeerDescriptor;
DECLARE_REFCOUNTED_STRUCT(TCellDescriptor)

DECLARE_REFCOUNTED_STRUCT(ICellDirectory)
DECLARE_REFCOUNTED_STRUCT(ICellDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TClusterDirectory)
DECLARE_REFCOUNTED_STRUCT(IClusterDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TClientDirectory)

DECLARE_REFCOUNTED_STRUCT(TCellDirectoryConfig)
DECLARE_REFCOUNTED_STRUCT(TClusterDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_STRUCT(TCellDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_STRUCT(TDownedCellTrackerConfig)

DECLARE_REFCOUNTED_CLASS(TDownedCellTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
