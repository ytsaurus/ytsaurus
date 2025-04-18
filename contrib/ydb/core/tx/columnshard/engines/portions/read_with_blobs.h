#pragma once
#include "base_with_blobs.h"
#include "common.h"
#include "portion_info.h"

#include <contrib/ydb/core/tx/columnshard/splitter/abstract/chunks.h>

#include <contrib/ydb/library/accessor/accessor.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <map>

namespace NKikimr::NOlap {

class TVersionedIndex;
class TWritePortionInfoWithBlobsResult;

class TReadPortionInfoWithBlobs: public TBasePortionInfoWithBlobs {
private:
    using TBlobChunks = std::map<TChunkAddress, std::shared_ptr<IPortionDataChunk>>;
    YDB_READONLY_DEF(TBlobChunks, Chunks);
    void RestoreChunk(const std::shared_ptr<IPortionDataChunk>& chunk);

    TPortionDataAccessor PortionInfo;

    explicit TReadPortionInfoWithBlobs(TPortionDataAccessor&& portionInfo)
        : PortionInfo(std::move(portionInfo)) {
    }

    explicit TReadPortionInfoWithBlobs(const TPortionDataAccessor& portionInfo)
        : PortionInfo(portionInfo) {
    }

    const TString& GetBlobByAddressVerified(const ui32 columnId, const ui32 chunkId) const;

public:
    static std::vector<TReadPortionInfoWithBlobs> RestorePortions(
        const std::vector<TPortionDataAccessor>& portions, NBlobOperations::NRead::TCompositeReadBlobs& blobs,
        const TVersionedIndex& tables);
    static TReadPortionInfoWithBlobs RestorePortion(
        const TPortionDataAccessor& portion, NBlobOperations::NRead::TCompositeReadBlobs& blobs,
        const TIndexInfo& indexInfo);

    TConclusion<std::shared_ptr<NArrow::TGeneralContainer>> RestoreBatch(
        const ISnapshotSchema& data, const ISnapshotSchema& resultSchema, const std::set<ui32>& seqColumns, const bool restoreAbsent = true) const;
    static std::optional<TWritePortionInfoWithBlobsResult> SyncPortion(TReadPortionInfoWithBlobs&& source,
        const ISnapshotSchema::TPtr& from, const ISnapshotSchema::TPtr& to, const TString& targetTier, const std::shared_ptr<IStoragesManager>& storages,
        std::shared_ptr<NColumnShard::TSplitterCounters> counters);

    std::vector<std::shared_ptr<IPortionDataChunk>> GetEntityChunks(const ui32 entityId) const;

    bool ExtractColumnChunks(const ui32 columnId, std::vector<const TColumnRecord*>& records, std::vector<std::shared_ptr<IPortionDataChunk>>& chunks);

    TString DebugString() const {
        return TStringBuilder() << PortionInfo.DebugString() << ";";
    }

    const TPortionInfo& GetPortionInfo() const {
        return PortionInfo.GetPortionInfo();
    }

    friend IOutputStream& operator << (IOutputStream& out, const TReadPortionInfoWithBlobs& info) {
        out << info.DebugString();
        return out;
    }
};

} // namespace NKikimr::NOlap
