#pragma once

#include "public.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/public.h>

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

} // namespace NYT::NPushBasedShuffleClient
