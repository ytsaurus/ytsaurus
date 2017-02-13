#pragma once

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/public.h>

namespace NYT {
namespace NHiveClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TCellPeerDescriptor;
class TCellDescriptor;
class TCellInfo;
class TEncapsulatedMessage;
class TClusterDirectory;
class TTimestampMap;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using TMessageId = i64;

class TCellPeerDescriptor;
struct TCellDescriptor;
struct TCellInfo;

struct TTimestampMap;

DECLARE_REFCOUNTED_CLASS(TCellDirectory)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TClusterDirectory)
DECLARE_REFCOUNTED_CLASS(TClusterDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TCellDirectoryConfig)
DECLARE_REFCOUNTED_CLASS(TClusterDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizerConfig)

DECLARE_REFCOUNTED_STRUCT(ITransactionParticipant)

////////////////////////////////////////////////////////////////////////////////

using NHydra::TCellId;
using NHydra::NullCellId;

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;
using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
