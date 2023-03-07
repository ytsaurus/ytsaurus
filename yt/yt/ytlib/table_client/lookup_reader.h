#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/client/hydra/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/range.h>
#include <yt/core/misc/ref.h>

#include <yt/core/compression/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ILookupReader
    : public virtual TRefCounted
{
    virtual TFuture<TSharedRef> LookupRows(
        // TODO(akozhikhov): change TClientBlockReadOptions type name,
        // because now it also provides options for lookups.
        const NChunkClient::TClientBlockReadOptions& options,
        const TSharedRange<TKey>& lookupKeys,
        NCypressClient::TObjectId tableId,
        NHydra::TRevision revision,
        const NTableClient::TTableSchema& tableSchema,
        std::optional<i64> estimatedSize,
        std::atomic<i64>* uncompressedDataSize,
        const NTableClient::TColumnFilter& columnFilter,
        NTableClient::TTimestamp timestamp,
        NCompression::ECodec codecId,
        bool produceAllVersions) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILookupReader)

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRowLookupReader(
    ILookupReaderPtr underlyingReader,
    NChunkClient::TClientBlockReadOptions blockReadOptions,
    TSharedRange<TKey> lookupKeys,
    TTabletSnapshotPtr tabletSnapshot,
    TColumnFilter columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
