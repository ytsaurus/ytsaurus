#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <library/cpp/yt/system/spin_lock.h>

namespace NYT::NRowIOBenchmark {

////////////////////////////////////////////////////////////////////////////////

class TMemoryChunkCollector
    : public TRefCounted
{
public:
    NChunkClient::TUnderlyingChunkWriterFactory GetFactory();

    //! Waits for Close; returns only emitted chunks, in chunk-spec order.
    std::vector<NChunkClient::TMemoryWriterPtr> Finish(
        const NTableClient::ISchemalessMultiChunkWriterPtr& writer);

    //! Call only after successful writer Close. Does not include unused trailing sessions.
    std::vector<NChunkClient::TMemoryWriterPtr> GetCompletedChunks(
        const NTableClient::ISchemalessMultiChunkWriterPtr& writer);

private:
    YT_DECLARE_SPIN_LOCK(TSpinLock, SpinLock_);
    THashMap<NChunkClient::TChunkId, NChunkClient::TMemoryWriterPtr> Sinks_;

    NChunkClient::IChunkWriterPtr CreateSink();
};

DEFINE_REFCOUNTED_TYPE(TMemoryChunkCollector)

////////////////////////////////////////////////////////////////////////////////

//! Columnar input is materialized by the production multi-writer before row validation.
//! Retains production chunk/range thresholds; desiredChunkSize is common to all formats.
NTableClient::ISchemalessMultiChunkWriterPtr CreateMemoryMultiChunkWriter(
    const NTableClient::TTableSchemaPtr& schema,
    const NTableClient::TNameTablePtr& nameTable,
    const TMemoryChunkCollectorPtr& collector,
    i64 desiredChunkSize = 256_MB);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
