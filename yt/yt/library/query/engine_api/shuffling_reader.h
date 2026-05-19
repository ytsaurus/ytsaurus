#pragma once

#include <yt/yt/client/table_client/unversioned_reader.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

std::pair<std::vector<NTableClient::ISchemafulUnversionedReaderPtr>, std::vector<TFuture<void>>> ShuffleByPrefixHash(
    TRange<NTableClient::ISchemafulUnversionedReaderPtr> readers,
    int keyPrefix,
    int destinationCount,
    const IInvokerPtr& invoker,
    const IMemoryChunkProviderPtr& memoryChunkProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
