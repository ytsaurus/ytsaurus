#include "indexation.h"

#include "compaction/merger.h"

#include <contrib/ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap {

void TInsertColumnEngineChanges::DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
    TBase::DoWriteIndexOnExecute(self, context);
    if (self) {
        auto removing = BlobsAction.GetRemoving(IStoragesManager::DefaultStorageId);
        for (const auto& insertedData : DataToIndex) {
            self->InsertTable->EraseCommittedOnExecute(context.DBWrapper, insertedData, removing);
        }
    }
}

void TInsertColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);
    Y_ABORT_UNLESS(DataToIndex.size());
    auto reading = BlobsAction.GetReading(IStoragesManager::DefaultStorageId);
    for (auto&& insertedData : DataToIndex) {
        reading->AddRange(insertedData.GetBlobRange(), insertedData.GetBlobData());
    }

    self.BackgroundController.StartIndexing(*this);
}

void TInsertColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexOnComplete(self, context);
    if (self) {
        for (const auto& insertedData : DataToIndex) {
            self->InsertTable->EraseCommittedOnComplete(insertedData);
        }
        if (!DataToIndex.empty()) {
            self->UpdateInsertTableCounters();
        }
        self->Counters.GetTabletCounters()->OnInsertionWriteIndexCompleted(context.BlobsWritten, context.BytesWritten, context.Duration);
    }
}

void TInsertColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishIndexing(*this);
}

namespace {

class TBatchInfo {
private:
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, Batch);

public:
    TBatchInfo(const std::shared_ptr<NArrow::TGeneralContainer>& batch, const NEvWrite::EModificationType /*modificationType*/)
        : Batch(batch) {
    }
};

class TPathFieldsInfo {
private:
    std::set<ui32> UsageColumnIds;
    const ISnapshotSchema::TPtr ResultSchema;
    THashMap<ui64, ISnapshotSchema::TPtr> Schemas;
    bool Finished = false;
    const ui32 FullColumnsCount;

public:
    TPathFieldsInfo(const ISnapshotSchema::TPtr& resultSchema)
        : UsageColumnIds(IIndexInfo::GetNecessarySystemColumnIdsSet())
        , ResultSchema(resultSchema)
        , FullColumnsCount(ResultSchema->GetIndexInfo().GetColumnIds(true).size())
    {
        AFL_VERIFY(FullColumnsCount);
    }

    bool IsFinished() const {
        return Finished;
    }

    bool HasDeletion() const {
        AFL_VERIFY(Finished);
        return UsageColumnIds.contains((ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG);
    }

    void Finish() {
        AFL_VERIFY(UsageColumnIds.size());
        AFL_VERIFY(!Finished);
        Finished = true;
        if (UsageColumnIds.size() == FullColumnsCount) {
            return;
        }
        auto defaultDiffs = ISnapshotSchema::GetColumnsWithDifferentDefaults(Schemas, ResultSchema);
        UsageColumnIds.insert(defaultDiffs.begin(), defaultDiffs.end());
    }

    const std::set<ui32>& GetUsageColumnIds() const {
        AFL_VERIFY(Finished);
        return UsageColumnIds;
    }

    void AddChunkInfo(const TCommittedData& data, const TConstructionContext& context) {
        AFL_VERIFY(!Finished);
        if (UsageColumnIds.size() == FullColumnsCount) {
            return;
        }
        auto blobSchema = context.SchemaVersions.GetSchemaVerified(data.GetSchemaVersion());
        std::set<ui32> columnIdsToDelete = blobSchema->GetColumnIdsToDelete(ResultSchema);
        if (!Schemas.contains(data.GetSchemaVersion())) {
            Schemas.emplace(data.GetSchemaVersion(), blobSchema);
        }
        TColumnIdsView columnIds = blobSchema->GetIndexInfo().GetColumnIds(false);
        std::vector<ui32> filteredIds = data.GetMeta().GetSchemaSubset().Apply(columnIds.begin(), columnIds.end());
        if (data.GetMeta().GetModificationType() == NEvWrite::EModificationType::Delete) {
            filteredIds.emplace_back((ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG);
        }
        for (const auto& filteredId : filteredIds) {
            if (!columnIdsToDelete.contains(filteredId)) {
                UsageColumnIds.insert(filteredId);
            }
        }
    }
};

class TPathData {
private:
    std::vector<TBatchInfo> Batches;
    YDB_READONLY_DEF(std::optional<TGranuleShardingInfo>, ShardingInfo);
    TPathFieldsInfo ColumnsInfo;

public:
    TPathData(const std::optional<TGranuleShardingInfo>& shardingInfo, const ISnapshotSchema::TPtr& resultSchema)
        : ShardingInfo(shardingInfo)
        , ColumnsInfo(resultSchema) {
    }

    const TPathFieldsInfo& GetColumnsInfo() const {
        return ColumnsInfo;
    }

    void FinishChunksInfo() {
        ColumnsInfo.Finish();
    }

    std::vector<std::shared_ptr<NArrow::TGeneralContainer>> GetGeneralContainers() const {
        std::vector<std::shared_ptr<NArrow::TGeneralContainer>> result;
        for (auto&& i : Batches) {
            result.emplace_back(i.GetBatch());
        }
        return result;
    }

    void AddChunkInfo(const NOlap::TCommittedData& data, const TConstructionContext& context) {
        ColumnsInfo.AddChunkInfo(data, context);
    }

    bool HasDeletion() {
        return ColumnsInfo.HasDeletion();
    }

    void AddBatch(const NOlap::TCommittedData& data, const std::shared_ptr<NArrow::TGeneralContainer>& batch) {
        AFL_VERIFY(ColumnsInfo.IsFinished());
        AFL_VERIFY(batch);
        Batches.emplace_back(batch, data.GetMeta().GetModificationType());
    }

    void AddShardingInfo(const std::optional<TGranuleShardingInfo>& info) {
        if (!info) {
            ShardingInfo.reset();
        } else if (ShardingInfo && info->GetSnapshotVersion() < ShardingInfo->GetSnapshotVersion()) {
            ShardingInfo = info;
        }
    }
};

class TPathesData {
private:
    THashMap<TInternalPathId, TPathData> Data;
    const ISnapshotSchema::TPtr ResultSchema;

public:
    TPathesData(const ISnapshotSchema::TPtr& resultSchema)
        : ResultSchema(resultSchema) {
    }

    void FinishChunksInfo() {
        for (auto&& i : Data) {
            i.second.FinishChunksInfo();
        }
    }

    const THashMap<TInternalPathId, TPathData>& GetData() const {
        return Data;
    }

    void AddChunkInfo(const NOlap::TCommittedData& inserted, const TConstructionContext& context) {
        auto shardingFilterCommit = context.SchemaVersions.GetShardingInfoOptional(inserted.GetPathId(), inserted.GetSnapshot());
        auto it = Data.find(inserted.GetPathId());
        if (it == Data.end()) {
            it = Data.emplace(inserted.GetPathId(), TPathData(shardingFilterCommit, ResultSchema)).first;
        }
        it->second.AddChunkInfo(inserted, context);
        it->second.AddShardingInfo(shardingFilterCommit);
    }

    void AddBatch(const NOlap::TCommittedData& inserted, const std::shared_ptr<NArrow::TGeneralContainer>& batch) {
        auto it = Data.find(inserted.GetPathId());
        AFL_VERIFY(it != Data.end());
        it->second.AddBatch(inserted, batch);
    }

    const TPathFieldsInfo& GetPathInfo(const TInternalPathId pathId) const {
        auto it = Data.find(pathId);
        AFL_VERIFY(it != Data.end());
        return it->second.GetColumnsInfo();
    }
};

}   // namespace

TConclusionStatus TInsertColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    Y_ABORT_UNLESS(!DataToIndex.empty());
    Y_ABORT_UNLESS(AppendedPortions.empty());

    auto resultSchema = context.SchemaVersions.GetLastSchema();
    Y_ABORT_UNLESS(resultSchema->GetIndexInfo().IsSorted());

    TPathesData pathBatches(resultSchema);
    for (auto& inserted : DataToIndex) {
        if (inserted.GetRemove()) {
            continue;
        }
        pathBatches.AddChunkInfo(inserted, context);
    }
    NoAppendIsCorrect = pathBatches.GetData().empty();

    pathBatches.FinishChunksInfo();

    for (auto& inserted : DataToIndex) {
        const TBlobRange& blobRange = inserted.GetBlobRange();
        if (inserted.GetRemove()) {
            Blobs.Extract(IStoragesManager::DefaultStorageId, blobRange);
            continue;
        }
        auto blobSchema = context.SchemaVersions.GetSchemaVerified(inserted.GetSchemaVersion());

        std::shared_ptr<NArrow::TGeneralContainer> batch;
        {
            const auto blobData = Blobs.Extract(IStoragesManager::DefaultStorageId, blobRange);

            NArrow::TSchemaLiteView blobSchemaView = blobSchema->GetIndexInfo().ArrowSchema();
            auto batchSchema =
                std::make_shared<arrow::Schema>(inserted.GetMeta().GetSchemaSubset().Apply(blobSchemaView.begin(), blobSchemaView.end()));
            batch = std::make_shared<NArrow::TGeneralContainer>(NArrow::DeserializeBatch(blobData, batchSchema));
            std::set<ui32> columnIdsToDelete = blobSchema->GetColumnIdsToDelete(resultSchema);
            if (!columnIdsToDelete.empty()) {
                batch->DeleteFieldsByIndex(blobSchema->ConvertColumnIdsToIndexes(columnIdsToDelete));
            }
        }
        IIndexInfo::AddSnapshotColumns(*batch, inserted.GetSnapshot(), (ui64)inserted.GetInsertWriteId());

        auto& pathInfo = pathBatches.GetPathInfo(inserted.GetPathId());

        if (pathInfo.HasDeletion()) {
            IIndexInfo::AddDeleteFlagsColumn(*batch, inserted.GetMeta().GetModificationType() == NEvWrite::EModificationType::Delete);
        }

        pathBatches.AddBatch(inserted, batch);
    }

    Y_ABORT_UNLESS(Blobs.IsEmpty());
    auto stats = std::make_shared<NArrow::NSplitter::TSerializationStats>();
    std::vector<std::shared_ptr<NArrow::TColumnFilter>> filters;
    for (auto& [pathId, pathInfo] : pathBatches.GetData()) {
        auto filteredSnapshot = std::make_shared<TFilteredSnapshotSchema>(resultSchema, pathInfo.GetColumnsInfo().GetUsageColumnIds());
        std::optional<ui64> shardingVersion;
        if (pathInfo.GetShardingInfo()) {
            shardingVersion = pathInfo.GetShardingInfo()->GetSnapshotVersion();
        }
        auto batches = pathInfo.GetGeneralContainers();
        filters.resize(batches.size());

        auto itGranule = PathToGranule.find(pathId);
        AFL_VERIFY(itGranule != PathToGranule.end())("path_id", pathId);
        NCompaction::TMerger merger(context, SaverContext, std::move(batches), std::move(filters));
        merger.SetOptimizationWritingPackMode(true);
        auto localAppended = merger.Execute(stats, itGranule->second, filteredSnapshot, pathId, shardingVersion);
        for (auto&& i : localAppended) {
            AppendedPortions.emplace_back(std::move(i));
        }
    }

    Y_ABORT_UNLESS(PathToGranule.size() == pathBatches.GetData().size());
    return TConclusionStatus::Success();
}

NColumnShard::ECumulativeCounters TInsertColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_INDEXING_SUCCESS : NColumnShard::COUNTER_INDEXING_FAIL;
}

}   // namespace NKikimr::NOlap
