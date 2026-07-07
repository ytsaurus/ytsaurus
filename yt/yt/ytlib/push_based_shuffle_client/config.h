#pragma once

#include "public.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NPushBasedShuffleClient {

////////////////////////////////////////////////////////////////////////////////

struct TShuffleWriterConfig
    : public NYTree::TYsonStruct
{
    //! Total memory budget across builders and in-flight records.
    i64 MemoryBudget;

    //! Fraction of the budget reserved for builder allocations (the
    //! remainder is the in-flight bucket). Must be in (0, 1).
    //!
    //! NB: The in-flight bucket holds about (1 - fraction) / fraction records
    //! per partition; this send pipelining depth must stay well above one to
    //! keep the per-partition journal flush loop busy.
    double BuildersBudgetFraction;

    //! Codec used to compress each shuffle record before it leaves the
    //! mapper.
    NCompression::ECodec Codec;

    //! Maximum number of physical send attempts per record before the
    //! writer enters a terminal failed state.
    int MaxSendAttempts;

    //! RPC timeout configuration for the underlying IDistributedChunkWriter.
    NDistributedChunkSessionClient::TDistributedChunkWriterConfigPtr WriterConfig;

    REGISTER_YSON_STRUCT(TShuffleWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TShuffleWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TPartitionReaderConfig
    : public NYTree::TYsonStruct
{
    //! Forwarded to each L1 reader.
    NDistributedChunkSessionClient::TDistributedChunkSessionReaderConfigPtr ChunkSessionReaderConfig;

    //! Codec all chunks in this reader instance were written with; must match the
    //! writer's codec.
    NCompression::ECodec Codec;

    //! Initial chunk size for each batch's TRowBuffer.
    i64 RowBufferStartChunkSize;

    //! Soft cap on compressed input bytes drained per Read(). Checked between
    //! blobs (overshoot <= one blob; the unconsumed tail defers to the next
    //! Read). Counts every blob the drain touches, including ones the filter
    //! rejects. Not a cap on decompressed or returned-batch size.
    i64 MaxBytesPerRead;

    REGISTER_YSON_STRUCT(TPartitionReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPartitionReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TPushShuffleConfig
    : public NYTree::TYsonStruct
{
    //! Mapper-side L2 writer config (client).
    TShuffleWriterConfigPtr WriterConfig;
    //! Reducer-side L2 reader config (client).
    TPartitionReaderConfigPtr ReaderConfig;
    //! Sequencer journal writer config: batch/flush knobs (server).
    NApi::TJournalChunkWriterConfigPtr JournalWriterConfig;
    //! Distributed chunk session pool config, e.g. max_active_sessions_per_slot (server).
    NDistributedChunkSessionClient::TDistributedChunkSessionPoolConfigPtr SessionPoolConfig;

    REGISTER_YSON_STRUCT(TPushShuffleConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPushShuffleConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
