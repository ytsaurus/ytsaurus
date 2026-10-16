#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

// Unkeyed (per-partition) output chunk storage.
struct ICompactPartitionOutputMessages
    : public TRefCounted
{
    struct TTableKey
    {
        TPartitionId PartitionId;
        TStreamId StreamId;
        i64 ChunkId;
    };

    struct TChunk
    {
        TTableKey Key;
        // Serialized message batch (see common/message_batch.h, TMessageBatchSerializer).
        TSharedRef Data;
        // Bitmask of processed (i.e. distributed and unregistered) message positions
        // within the chunk. Bit `i` set ⇒ the i-th message of the batch need not be
        // re-delivered after restart. Length is ⌈message_count/8⌉ bytes.
        std::string ProcessedMask;
    };

    struct TMaskUpdate
    {
        TTableKey Key;
        std::string ProcessedMask;
    };

    struct TFilter
    {
        std::optional<TPartitionId> PartitionId;
    };

    virtual void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) = 0;

    virtual TFuture<std::vector<TChunk>> LoadAll(
        TFilter filter) = 0;

    // Writes whole chunk rows: (key, data, data_codec, processed_mask).
    virtual void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TChunk>& chunks,
        NCompression::ECodec codecId) = 0;

    // Updates only the `processed_mask` column of existing rows. The `data` /
    // `data_codec` columns are left untouched by ModifyRows.
    virtual void UpdateMask(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TMaskUpdate>& updates) = 0;

    virtual void Erase(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TTableKey>& tableKeys) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICompactPartitionOutputMessages);

////////////////////////////////////////////////////////////////////////////////

ICompactPartitionOutputMessagesPtr CreateCompactPartitionOutputMessages(
    TContextPtr context,
    TDynamicTableRequestSpecPtr dynamicSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
