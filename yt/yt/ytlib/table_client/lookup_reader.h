#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/compression/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ILookupReader
    : public virtual TRefCounted
{
    virtual TFuture<TSharedRef> LookupRows(
        const NChunkClient::TClientChunkReadOptions& options,
        TSharedRange<TLegacyKey> lookupKeys,
        NCypressClient::TObjectId tableId,
        NHydra::TRevision revision,
        NTableClient::TTableSchemaPtr tableSchema,
        std::optional<i64> estimatedSize,
        const NTableClient::TColumnFilter& columnFilter,
        NTableClient::TTimestamp timestamp,
        NCompression::ECodec codecId,
        bool produceAllVersions,
        TTimestamp overrideTimestamp,
        bool enablePeerProbing,
        bool enableRejectsIfThrottling,
        IInvokerPtr sessionInvoker = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILookupReader)

////////////////////////////////////////////////////////////////////////////////

//! Converts ILookupReader to IVersionedReader.
IVersionedReaderPtr CreateVersionedLookupReader(
    ILookupReaderPtr lookupReader,
    NChunkClient::TClientChunkReadOptions chunkReadOptions,
    TSharedRange<TLegacyKey> lookupKeys,
    TTabletSnapshotPtr tabletSnapshot,
    TColumnFilter columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    TTimestamp overrideTimestamp,
    bool enablePeerProbing,
    bool enableRejectsIfThrottling);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
