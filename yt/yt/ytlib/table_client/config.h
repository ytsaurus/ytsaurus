#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/config.h>

#include <yt/yt/client/chunk_client/config.h>

#include <util/generic/size_literals.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableWriterOptions
    : public TChunkWriterOptions
    , public NChunkClient::TMultiChunkWriterOptions
{
public:
    // Whether to compute output digest.
    // May be used to determine job determinism.
    bool ComputeDigest;

    REGISTER_YSON_STRUCT(TTableWriterOptions);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TTableWriterOptions)

////////////////////////////////////////////////////////////////////////////////

struct TBlobTableWriterConfig
    : public NTableClient::TTableWriterConfig
{
    i64 MaxPartSize;

    REGISTER_YSON_STRUCT(TBlobTableWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlobTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBufferedTableWriterConfig
    : public TTableWriterConfig
{
    TDuration RetryBackoffTime;
    TDuration FlushPeriod;
    i64 RowBufferChunkSize;

    REGISTER_YSON_STRUCT(TBufferedTableWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBufferedTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableReaderOptions
    : public TChunkReaderOptions
    , public NChunkClient::TMultiChunkReaderOptions
{
    REGISTER_YSON_STRUCT(TTableReaderOptions);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TTableReaderOptions)

////////////////////////////////////////////////////////////////////////////////

struct TTableColumnarStatisticsCacheConfig
    : public TAsyncExpiringCacheConfig
{
    // Two fields below are for the chunk spec fetcher.
    int MaxChunksPerFetch;
    int MaxChunksPerLocateRequest;

    NChunkClient::TFetcherConfigPtr Fetcher;

    EColumnarStatisticsFetcherMode ColumnarStatisticsFetcherMode;

    REGISTER_YSON_STRUCT(TTableColumnarStatisticsCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableColumnarStatisticsCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct THunkChunkPayloadWriterConfig
    : public virtual NYTree::TYsonStruct
{
    //! Writer will be aiming for blocks of approximately this size.
    i64 DesiredBlockSize;

    REGISTER_YSON_STRUCT(THunkChunkPayloadWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THunkChunkPayloadWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
