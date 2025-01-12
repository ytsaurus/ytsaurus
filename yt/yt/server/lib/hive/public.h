#pragma once

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;

using NHiveClient::TMessageId;

using TAvenueEndpointId = NObjectClient::TObjectId;
using TEndpointId = NObjectClient::TObjectId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IHiveManager)
DECLARE_REFCOUNTED_STRUCT(IAvenueDirectory)

DECLARE_REFCOUNTED_CLASS(TSimpleAvenueDirectory)
DECLARE_REFCOUNTED_CLASS(TLogicalTimeRegistry)

YT_DEFINE_STRONG_TYPEDEF(TLogicalTime, i64);

YT_DEFINE_STRONG_TYPEDEF(TMailboxHandle, void*);

struct TPersistentMailboxStateCookie;

DECLARE_REFCOUNTED_STRUCT(TSerializedMessage)

DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TLogicalTimeRegistryConfig)
DECLARE_REFCOUNTED_CLASS(THiveManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TClusterDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
