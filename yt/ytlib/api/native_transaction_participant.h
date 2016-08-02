#pragma once

#include "public.h"
#include "connection.h"

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/election/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

NHiveClient::ITransactionParticipantPtr CreateNativeTransactionParticipant(
    NHiveClient::TCellDirectoryPtr cellDirectory,
    const NObjectClient::TCellId& cellId,
    const TTransactionParticipantOptions& options);

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

