#pragma once

#include "public.h"

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

namespace NYT {
namespace NApi {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

NHiveClient::ITransactionParticipantPtr CreateTransactionParticipant(
    NHiveClient::TCellDirectoryPtr cellDirectory,
    NHiveClient::TCellDirectorySynchronizerPtr cellDirectorySynchronizer,
    NTransactionClient::ITimestampProviderPtr timestampProvider,
    IConnectionPtr connection,
    const NObjectClient::TCellId& cellId,
    const TTransactionParticipantOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT

