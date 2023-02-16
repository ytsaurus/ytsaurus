#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/compression/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TOffloadingReaderOptions final
{
    NChunkClient::TClientChunkReadOptions ChunkReadOptions;

    //! These two form table schema cache key.
    NCypressClient::TObjectId TableId;
    NHydra::TRevision MountRevision;

    NTableClient::TTableSchemaPtr TableSchema;

    TColumnFilter ColumnFilter;
    TTimestamp Timestamp;
    bool ProduceAllVersions;
    TTimestamp OverrideTimestamp;
    bool EnableHashChunkIndex;
};

DEFINE_REFCOUNTED_TYPE(TOffloadingReaderOptions)

////////////////////////////////////////////////////////////////////////////////

//! It offloads lookups to data node.
struct IOffloadingReader
    : public virtual TRefCounted
{
    virtual TFuture<TSharedRef> LookupRows(
        TOffloadingReaderOptionsPtr options,
        TSharedRange<TLegacyKey> lookupKeys,
        std::optional<i64> estimatedSize,
        NCompression::ECodec codecId,
        IInvokerPtr sessionInvoker = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOffloadingReader)

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedOffloadingLookupReader(
    IOffloadingReaderPtr offloadingReader,
    TOffloadingReaderOptionsPtr options,
    TSharedRange<TLegacyKey> lookupKeys);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
