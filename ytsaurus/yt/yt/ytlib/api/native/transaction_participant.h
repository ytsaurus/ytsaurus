#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

NHiveClient::ITransactionParticipantPtr CreateTransactionParticipant(
    NHiveClient::ICellDirectoryPtr cellDirectory,
    NHiveClient::ICellDirectorySynchronizerPtr cellDirectorySynchronizer,
    NTransactionClient::ITimestampProviderPtr timestampProvider,
    IConnectionPtr connection,
    NObjectClient::TCellId cellId,
    const TTransactionParticipantOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
