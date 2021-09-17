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

class TCellPeerDescriptor;
struct TCellDescriptor;
struct TCellInfo;

DECLARE_REFCOUNTED_CLASS(TCellDirectory)

DECLARE_REFCOUNTED_STRUCT(ICellDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TClusterDirectory)
DECLARE_REFCOUNTED_CLASS(TClusterDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TCellDirectoryConfig)
DECLARE_REFCOUNTED_CLASS(TClusterDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizerConfig)

DECLARE_REFCOUNTED_CLASS(TCellTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
