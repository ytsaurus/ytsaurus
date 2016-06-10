#pragma once

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/public.h>

namespace NYT {
namespace NHive {

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
DECLARE_REFCOUNTED_CLASS(TClusterDirectory)

DECLARE_REFCOUNTED_CLASS(TCellDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

using NHydra::TCellId;
using NHydra::NullCellId;

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;
using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
