#include "public.h"

#include <ytlib/api/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <core/logging/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TChunkId CreateChunk(
    NApi::IClientPtr client,
    NObjectClient::TCellTag cellTag,
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    NObjectClient::EObjectType chunkType,
    const NTransactionClient::TTransactionId& transactionId,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
