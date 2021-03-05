#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/range.h>
#include <yt/yt/core/misc/ref.h>

#include <yt/yt/core/compression/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ILookupReader
    : public virtual TRefCounted
{
    virtual TFuture<TSharedRef> LookupRows(
        // TODO(akozhikhov): change TClientBlockReadOptions type name,
        // because now it also provides options for lookups.
        const NChunkClient::TClientBlockReadOptions& options,
        TSharedRange<TLegacyKey> lookupKeys,
        NCypressClient::TObjectId tableId,
        NHydra::TRevision revision,
        NTableClient::TTableSchemaPtr tableSchema,
        std::optional<i64> estimatedSize,
        const NTableClient::TColumnFilter& columnFilter,
        NTableClient::TTimestamp timestamp,
        NCompression::ECodec codecId,
        bool produceAllVersions,
        TTimestamp chunkTimestamp,
        bool enablePeerProbing,
        bool enableRejectsIfThrottling) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILookupReader)

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRowLookupReader(
    ILookupReaderPtr underlyingReader,
    NChunkClient::TClientBlockReadOptions blockReadOptions,
    TSharedRange<TLegacyKey> lookupKeys,
    TTabletSnapshotPtr tabletSnapshot,
    TColumnFilter columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    TTimestamp chunkTimestamp,
    bool enablePeerProbing,
    bool enableRejectsIfThrottling);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
