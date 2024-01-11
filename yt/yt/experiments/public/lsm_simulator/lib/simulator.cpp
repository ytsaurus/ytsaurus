#include "simulator.h"

#include "config.h"
#include "helpers.h"
#include "store_manager.h"
#include "structured_logger.h"
#include "throttler.h"

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/concurrency/coroutine.h>

#include <random>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

using TRowProvider = NConcurrency::TCoroutine<TRow()>;

////////////////////////////////////////////////////////////////////////////////

class TWriter
{
public:
    TWriter(
        TWriterSpecPtr spec,
        TLsmSimulatorConfigPtr config,
        IActionQueuePtr actionQueue)
        : Spec_(std::move(spec))
        , Config_(std::move(config))
        , Throttler_(actionQueue)
        , ActionQueue_(std::move(actionQueue))
        , CreationTime_(ActionQueue_->GetNow())

    {
        RowProvider_ = std::make_unique<TRowProvider>(
            BIND(&TWriter::RandomRowYielder, this));
        Throttler_.SetLimit(Spec_->WriteRate);
    }

    bool GetBulkInsert() const
    {
        return Spec_->BulkInsert;
    }

    i64 GetBulkInsertChunkCount() const
    {
        return Spec_->BulkInsertChunkCount;
    }

    bool GetDeleteAfterInsert() const
    {
        return Spec_->DeleteAfterInsert;
    }

    i64 GetDeletionWindowSize() const
    {
        return Spec_->DeletionWindowSize;
    }

    int GetRowCount() {
        return ssize(Rows_);
    }

    void AddRow(TRow row) {
        Rows_.push_back(row);
    }

    i64 DeleteRow() {
        auto deleteIndex = RandomNumber<ui32>(ssize(Rows_));
        std::swap(Rows_[deleteIndex], Rows_.back());
        auto key = Rows_.back().Key;
        Rows_.pop_back();
        return key;
    }

    bool IsWritingCompleted() const
    {
        if (Spec_->SleepTime) {
            return ActionQueue_->GetNow() >= CreationTime_ + TDuration::MilliSeconds(Spec_->SleepTime);
        }
        return !RowProvider_ || DataWeightWritten_ >= Spec_->TotalDataSize;
    }

    std::vector<TRow> GenerateRows()
    {
        std::vector<TRow> rows;
        i64 generatedDataSize = 0;
        THashSet<TKey> keys;
        while (generatedDataSize < Spec_->TotalDataSize) {
            auto row = RowProvider_->Run();
            if (!row) {
                RowProvider_.reset();
                continue;
            }
            if (keys.find(row->Key) != keys.end()) {
                continue;
            }
            keys.insert(row->Key);
            generatedDataSize += row->DataSize;
            rows.push_back(*row);
        }
        return rows;
    }

    void Run(std::function<void(TRow)> onRow)
    {
        if (Spec_->SleepTime) {
            return;
        }

        while (!Throttler_.IsOverdraft()) {
            auto row = RowProvider_->Run();
            if (!row) {
                RowProvider_.reset();
                return;
            }
            auto dataSize = row->DataSize;
            onRow(*row);
            Throttler_.Acquire(dataSize);
            DataWeightWritten_ += dataSize;
        }
    }

private:
    TWriterSpecPtr Spec_;
    TLsmSimulatorConfigPtr Config_;
    TThrottler Throttler_;
    IActionQueuePtr ActionQueue_;
    TInstant CreationTime_;
    std::unique_ptr<TRowProvider> RowProvider_;

    i64 DataWeightWritten_ = 0;

    i64 CurrentIncreasingKeyValue_ = 0;
    std::vector<TRow> Rows_;

    void RandomRowYielder(TRowProvider& self)
    {
        auto minKey = Spec_->MinKey.value_or(Config_->MinKey);
        auto maxKey = Spec_->MaxKey.value_or(Config_->MaxKey);
        auto minSize = Spec_->MinValueSize.value_or(Config_->MinValueSize);
        auto maxSize = Spec_->MaxValueSize.value_or(Config_->MaxValueSize);
        while (true) {
            if (Spec_->AlmostIncreasing) {
                minKey = CurrentIncreasingKeyValue_;
                maxKey = CurrentIncreasingKeyValue_ + Spec_->IncreasingSplay;
                ++CurrentIncreasingKeyValue_;
            }
            auto key = RandomNumber<ui32>(maxKey - minKey + 1) + minKey;
            auto size = RandomNumber<ui32>(maxSize - minSize + 1) + minSize;
            auto row = Row(key, size);
            self.Yield(row);
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

class TLsmSimulator
    : public ILsmSimulator
{

public:
    TLsmSimulator(
        TLsmSimulatorConfigPtr config,
        TTableMountConfigPtr mountConfig,
        IActionQueuePtr actionQueue,
        TTabletPtr tablet,
        TStructuredLoggerPtr structuredLogger)
        : Config_(std::move(config))
        , MountConfig_(std::move(mountConfig))
        , ActionQueue_(std::move(actionQueue))
        , StructuredLogger_(std::move(structuredLogger))
    {
        if (!tablet) {
            if (Config_->InitialData) {
                tablet = CreateTabletWithData(Config_->InitialData);
            } else {
                tablet = CreateTablet();
            }
        }
        StoreManager_ = std::make_unique<TStoreManager>(
            std::move(tablet),
            Config_,
            MountConfig_,
            ActionQueue_,
            StructuredLogger_);

        StoreManager_->StartEpoch();

        ActionQueue_->Schedule(
            BIND(&TLsmSimulator::RunLsmCallback, MakeWeak(this)),
            TDuration::Zero());
    }

    TStoreManager* GetStoreManager() const override
    {
        return StoreManager_.get();
    }

    bool IsWritingCompleted() const override
    {
        return NextWriterIndex_ == -1;
    }

    int GetWriterIndex() const override
    {
        return NextWriterIndex_ == -1 ? -1 : NextWriterIndex_ - 1;
    }

    void Start() override
    {
        StartNextWriter();
    }

    void StartNextWriter()
    {
        if (NextWriterIndex_ == ssize(Config_->Writers)) {
            NextWriterIndex_ = -1;
            return;
        }

        Writer_ = std::make_unique<TWriter>(
            Config_->Writers[NextWriterIndex_],
            Config_,
            ActionQueue_);

        ++NextWriterIndex_;

        WriterMain();
    }

    void WriterMain()
    {
        if (Writer_->IsWritingCompleted()) {
            StartNextWriter();
            return;
        }

        if (Writer_->GetBulkInsert()) {
            auto rows = Writer_->GenerateRows();
            std::sort(rows.begin(), rows.end());

            i64 totalSize = 0;
            auto timestamp = GenerateTimestamp();
            for (auto& row : rows) {
                totalSize += row.DataSize;
                for (auto& value : row.Values) {
                    value.Timestamp = timestamp;
                }
            }

            i64 chunkSize = totalSize / Writer_->GetBulkInsertChunkCount();
            std::vector<std::unique_ptr<TStore>> chunks;
            i64 currentSize = 0;
            i64 begin = 0;
            i64 end = 0;
            while (begin < ssize(rows)) {
                while (end < ssize(rows) && currentSize < chunkSize) {
                    currentSize += rows[end].DataSize;
                    ++end;
                }

                auto store = MakeStore(
                    std::vector<TRow>(rows.begin() + begin, rows.begin() + end),
                    Config_->CompressionRatio);
                chunks.push_back(std::move(store));
                currentSize = 0;
                begin = end;
            }

            StoreManager_->BulkAddStores(std::move(chunks));
            StartNextWriter();
            return;
        }

        auto onRow = [&] (TRow row) {
            auto timestamp = GenerateTimestamp();
            for (auto& value : row.Values) {
                value.Timestamp = timestamp;
            }
            StoreManager_->InsertRow(row);
            if (Writer_->GetDeleteAfterInsert()) {
                Writer_->AddRow(row);
                if (Writer_->GetRowCount() > Writer_->GetDeletionWindowSize()) {
                    auto key = Writer_->DeleteRow();
                    StoreManager_->DeleteRow(Tombstone(key, GenerateTimestamp()));
                }
            }
        };

        Writer_->Run(onRow);
        ActionQueue_->Schedule(
            BIND(&TLsmSimulator::WriterMain, MakeStrong(this)),
            Config_->InsertionPeriod);
    }

    /*
    void StartInsertingIncreasingRows(i64 start, i64 end, i64 splay)
    {
        i64 delta = RandomNumber<ui32>(splay);
        i64 key = start + splay / 2 + delta;
        auto size = RandomNumber<ui32>(100);
        auto row = Row(key, size * 10_KB);
        StoreManager_->InsertRow(std::move(row));

        if (start == end) {
            ActionQueue_->Stop();
            return;
        }
        ActionQueue_->Schedule(
            BIND(
                &TLsmSimulator::StartInsertingIncreasingRows,
                MakeWeak(this),
                start + 1,
                end,
                splay),
            Config_->InsertionPeriod);
    }
    */

private:
    std::unique_ptr<TStoreManager> StoreManager_;
    TLsmSimulatorConfigPtr Config_;
    TTableMountConfigPtr MountConfig_;
    IActionQueuePtr ActionQueue_;
    TStructuredLoggerPtr StructuredLogger_;
    TDynamicStore DynamicStore_;
    TTimestamp LastGeneratedTimestamp_ = NTransactionClient::NullTimestamp;
    int NextWriterIndex_ = 0;
    std::unique_ptr<TWriter> Writer_;
    std::mt19937 Generator_ = std::mt19937{179};

    void FillPartition(
        TPartition* partition,
        std::vector<TRow>::iterator rowsBegin,
        std::vector<TRow>::iterator rowsEnd,
        const std::vector<i64>& storeSizes)
    {
        std::vector<i64> prefixSizeSum(ssize(storeSizes) + 1);
        for (int index = 0; index < ssize(storeSizes); ++index) {
            prefixSizeSum[index + 1] = prefixSizeSum[index] + storeSizes[index];
        }

        std::shuffle(rowsBegin, rowsEnd, Generator_);

        i64 currentStoreSize = 0;
        int storeIndex = 0;
        std::vector<TRow> currentStore;
        while (rowsBegin != rowsEnd) {
            while (rowsBegin != rowsEnd && storeIndex < ssize(storeSizes) && currentStoreSize < prefixSizeSum[storeIndex + 1]) {
                currentStoreSize += rowsBegin->DataSize;
                currentStore.push_back(*rowsBegin);
                ++rowsBegin;
            }
            ++storeIndex;

            std::sort(currentStore.begin(), currentStore.end());
            auto store = MakeStore(std::move(currentStore), Config_->CompressionRatio);
            store->SetPartition(partition);
            store->SetIsCompactable(true);
            store->SetCreationTime(ActionQueue_->GetNow());
            partition->AddStore(std::move(store));
        }
    }

    TTabletPtr CreateTabletWithData(TInitialDataDescriptionPtr dataDescription)
    {
        auto tablet = New<TTablet>();
        tablet->SetStructuredLogger(StructuredLogger_);
        tablet->SetPhysicallySorted(true);
        tablet->SetMounted(true);
        tablet->SetMountConfig(MountConfig_);

        auto eden = std::make_unique<TPartition>(tablet.Get());
        eden->SetIndex(NYT::NLsm::EdenIndex);
        eden->PivotKey() = NTableClient::EmptyKey();
        eden->NextPivotKey() = NTableClient::MaxKey();

        i64 edenSize = std::accumulate(dataDescription->Eden.begin(), dataDescription->Eden.end(), 0);
        std::vector<TRow> edenRows;
        i64 currentEdenSize = 0;
        while (currentEdenSize < edenSize) {
            auto key = RandomNumber<ui32>(dataDescription->MaxKey - dataDescription->MinKey + 1) + dataDescription->MinKey;
            auto size = RandomNumber<ui32>(dataDescription->MaxValueSize - dataDescription->MinValueSize + 1) +
                dataDescription->MinValueSize;
            auto row = Row(key, size);
            auto timestamp = GenerateTimestamp();
            currentEdenSize += size;
            for (auto& value : row.Values) {
                value.Timestamp = timestamp;
            }
            edenRows.push_back(row);
        }
        std::sort(edenRows.begin(), edenRows.end());
        edenRows.erase(std::unique(edenRows.begin(), edenRows.end()), edenRows.end());
        FillPartition(
            eden.get(),
            edenRows.begin(),
            edenRows.end(),
            dataDescription->Eden);
        tablet->Eden() = std::move(eden);

        std::vector<i64> prefixSizeSum(ssize(dataDescription->Partitions) + 1);
        for (int index = 0; index < ssize(dataDescription->Partitions); ++index) {
            prefixSizeSum[index + 1] = prefixSizeSum[index] +
                std::accumulate(dataDescription->Partitions[index].begin(), dataDescription->Partitions[index].end(), 0);
        }

        std::vector<TRow> rows;
        i64 currentPartitionsSize = 0;
        while (currentPartitionsSize < prefixSizeSum.back()) {
            auto key = RandomNumber<ui32>(dataDescription->MaxKey - dataDescription->MinKey + 1) + dataDescription->MinKey;
            auto size = RandomNumber<ui32>(dataDescription->MaxValueSize - dataDescription->MinValueSize + 1) +
                dataDescription->MinValueSize;
            auto row = Row(key, size);
            auto timestamp = GenerateTimestamp();
            currentPartitionsSize += size;
            for (auto& value : row.Values) {
                value.Timestamp = timestamp;
            }
            rows.push_back(row);
        }
        std::sort(rows.begin(), rows.end());
        rows.erase(std::unique(rows.begin(), rows.end()), rows.end());

        currentPartitionsSize = 0;
        std::vector<int> pivotIndexes = {0};
        int rowIndex = 0;
        int partitionIndex = 0;
        while (partitionIndex < ssize(dataDescription->Partitions)) {
            while (rowIndex < ssize(rows) && currentPartitionsSize < prefixSizeSum[partitionIndex + 1]) {
                currentPartitionsSize += rows[rowIndex].DataSize;
                ++rowIndex;
            }
            ++partitionIndex;
            pivotIndexes.push_back(rowIndex);
        }

        for (int index = 0; index < ssize(dataDescription->Partitions); ++index) {
            YT_VERIFY(rows[pivotIndexes[index]].Key < (pivotIndexes[index + 1] < ssize(rows)
                ? rows[pivotIndexes[index + 1]].Key
                : std::numeric_limits<TKey>::max()));
        }

        for (int partitionIndex = 0; partitionIndex + 1 < ssize(pivotIndexes); ++partitionIndex) {
            auto partition = std::make_unique<TPartition>(tablet.Get());
            partition->SetIndex(partitionIndex);
            partition->PivotKey() = partitionIndex > 0
                                    ? BuildNativeKey(rows[pivotIndexes[partitionIndex]].Key)
                                    : NTableClient::EmptyKey();
            partition->NextPivotKey() = pivotIndexes[partitionIndex + 1] < ssize(rows)
                                        ? BuildNativeKey(rows[pivotIndexes[partitionIndex + 1]].Key)
                                        : NTableClient::MaxKey();
            FillPartition(
                partition.get(),
                rows.begin() + pivotIndexes[partitionIndex],
                rows.begin() + pivotIndexes[partitionIndex + 1],
                dataDescription->Partitions[partitionIndex]);
            tablet->Partitions().push_back(std::move(partition));
        }

        return tablet;
    }

    TTabletPtr CreateTablet()
    {
        auto tablet = New<TTablet>();
        tablet->SetStructuredLogger(StructuredLogger_);
        tablet->SetPhysicallySorted(true);
        tablet->SetMounted(true);
        tablet->SetMountConfig(MountConfig_);

        auto eden = std::make_unique<TPartition>(tablet.Get());
        eden->SetIndex(NYT::NLsm::EdenIndex);
        eden->PivotKey() = NTableClient::EmptyKey();
        eden->NextPivotKey() = NTableClient::MaxKey();
        tablet->Eden() = std::move(eden);

        auto partition = std::make_unique<TPartition>(tablet.Get());
        partition->SetIndex(0);
        partition->PivotKey() = NTableClient::EmptyKey();
        partition->NextPivotKey() = NTableClient::MaxKey();
        tablet->Partitions().push_back(std::move(partition));

        return tablet;
    }

    void RunLsmCallback()
    {
        YT_LOG_INFO("Running LSM");
        auto backend = NLsm::CreateLsmBackend();

        static auto TabletNodeConfig = New<NTabletNode::TTabletNodeConfig>();
        static auto TabletNodeDynamicConfig = New<NTabletNode::TTabletNodeDynamicConfig>();

        NLsm::TLsmBackendState state;
        state.CurrentTime = ActionQueue_->GetNow();
        state.TabletNodeConfig = TabletNodeConfig;
        state.TabletNodeDynamicConfig = TabletNodeDynamicConfig;

        state.Bundles["default"] = NLsm::TTabletCellBundleState{
            .ForcedRotationMemoryRatio = 0.8,
            .EnablePerBundleMemoryLimit = true,
            .DynamicMemoryLimit = 10_GB,
            .DynamicMemoryUsage = StoreManager_->GetDynamicMemoryUsage(),
        };

        backend->StartNewRound(state);

        auto lsmTablet = StoreManager_->GetLsmTablet();
        auto actions = backend->BuildLsmActions({lsmTablet}, /*bundleName*/ "default");
        actions.MergeWith(backend->BuildOverallLsmActions());
        StoreManager_->GetTablet()->ResetLsmState();
        StoreManager_->ApplyLsmActionBatch(actions);

        ActionQueue_->Schedule(
            BIND(&TLsmSimulator::RunLsmCallback, MakeWeak(this)),
            Config_->LsmScanPeriod);
    }

    TTimestamp GenerateTimestamp()
    {
        auto unixTime = ActionQueue_->GetNow().Seconds();
        if (LastGeneratedTimestamp_ >> 30 == unixTime) {
            return ++LastGeneratedTimestamp_;
        } else {
            LastGeneratedTimestamp_ = unixTime << 30;
            return LastGeneratedTimestamp_;
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TLsmSimulator)

////////////////////////////////////////////////////////////////////////////////

ILsmSimulatorPtr CreateLsmSimulator(
    TLsmSimulatorConfigPtr config,
    TTableMountConfigPtr mountConfig,
    IActionQueuePtr actionQueue,
    TTabletPtr tablet,
    TStructuredLoggerPtr structuredLogger)
{
    return New<TLsmSimulator>(
        std::move(config),
        std::move(mountConfig),
        std::move(actionQueue),
        std::move(tablet),
        std::move(structuredLogger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
