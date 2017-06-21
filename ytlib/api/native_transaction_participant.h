#pragma once

#include "public.h"

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

NHiveClient::ITransactionParticipantPtr CreateNativeTransactionParticipant(
    NHiveClient::TCellDirectoryPtr cellDirectory,
    NHiveClient::TCellDirectorySynchronizerPtr cellDirectorySynchronizer,
    NTransactionClient::ITimestampProviderPtr timestampProvider,
    const NObjectClient::TCellId& cellId,
    const TTransactionParticipantOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

