#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/tables/compact_output_messages.h>

#include <map>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

// In-memory implementation of ICompactOutputMessages for unit testing.
// Transactions are ignored — Write() / UpdateMask() / Erase() are applied immediately.
class TInMemoryCompactOutputMessages
    : public ICompactOutputMessages
{
public:
    TInMemoryCompactOutputMessages() = default;

    void Reconfigure(TDynamicTableRequestSpecPtr /*dynamicSpec*/) override
    { }

    TFuture<std::vector<TChunk>> LoadAll(
        TFilter filter) override;

    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TChunk>& chunks,
        NCompression::ECodec codecId) override;

    void UpdateMask(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TMaskUpdate>& updates) override;

    void Erase(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TTableKey>& tableKeys) override;

    // Test helpers: counters for write / mask-update operations.
    i64 GetWriteChunkCount() const;
    i64 GetUpdateMaskCount() const;

private:
    struct TStorageKey
    {
        TComputationId ComputationId;
        TKey Key;
        TStreamId StreamId;
        i64 ChunkId;

        bool operator<(const TStorageKey& other) const;
    };

    struct TStorageValue
    {
        TSharedRef Data;
        std::string ProcessedMask;
    };

    std::map<TStorageKey, TStorageValue> Storage_;
    i64 WriteChunkCount_ = 0;
    i64 UpdateMaskCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TInMemoryCompactOutputMessages);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
