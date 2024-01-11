#pragma once

#include "action_queue.h"
#include "config.h"
#include "statistics.h"
#include "tablet.h"
#include "throttler.h"

#include <yt/yt/server/lib/lsm/lsm_backend.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NLsm::NTesting {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TDynamicStore
{
public:
    std::unique_ptr<TStore> Flush(double CompressionRatio)
    {
        std::vector<TRow> rowsVector(
            std::make_move_iterator(rows.begin()),
            std::make_move_iterator(rows.end()));
        return MakeStore(std::move(rowsVector), CompressionRatio);
    }

    void AddRow(TRow row)
    {
        auto it = rows.find(row);
        if (it != rows.end()) {
            TRow sameKeyRow = *it;
            rows.erase(it);
            if (row.Values.empty()) {
                if (!sameKeyRow.DeleteTimestamps.empty()) {
                    YT_VERIFY(row.DeleteTimestamps[0] > sameKeyRow.DeleteTimestamps[0]);
                }
                sameKeyRow.DeleteTimestamps.insert(sameKeyRow.DeleteTimestamps.begin(), row.DeleteTimestamps[0]);
            } else {
                if (!sameKeyRow.Values.empty()) {
                    YT_VERIFY(row.Values[0].Timestamp > sameKeyRow.Values[0].Timestamp);
                }
                sameKeyRow.Values.insert(sameKeyRow.Values.begin(), row.Values[0]);
                sameKeyRow.DataSize += row.DataSize;
                DataSize_ += row.DataSize;
            }
            rows.insert(sameKeyRow);
        } else {
            DataSize_ += row.DataSize;
            rows.insert(row);
        }
    }

    i64 GetDataSize() const
    {
        return DataSize_;
    }

    i64 GetRowCount() const
    {
        return ssize(rows);
    }

private:
    std::set<TRow> rows;
    i64 DataSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TStoreManager
{
public:
    explicit TStoreManager(
        TTabletPtr tablet,
        TLsmSimulatorConfigPtr config,
        TTableMountConfigPtr mountConfig,
        IActionQueuePtr actionQueue,
        TStructuredLoggerPtr structuredLogger = nullptr);

    void StartEpoch();
    void InitializeRotation();

    void DeleteRow(TRow row);
    void InsertRow(TRow row);

    void BulkAddStores(std::vector<std::unique_ptr<TStore>> chunks);

    TTablet* GetTablet() const;
    NLsm::TTabletPtr GetLsmTablet() const;

    TStatistics GetStatistics() const;
    i64 GetDynamicMemoryUsage() const;
    i64 GetCompressedDataSize() const;

    void ApplyLsmActionBatch(TLsmActionBatch actions);

    void PrintDebug();

    void EmitOutOfBandStructuredHeartbeat() const;

    DEFINE_BYVAL_RW_PROPERTY(std::optional<TInstant>, LastPeriodicRotationTime);

private:
    TTabletPtr Tablet_;
    TLsmSimulatorConfigPtr Config_;
    TTableMountConfigPtr MountConfig_;
    IActionQueuePtr ActionQueue_;
    TDynamicStore DynamicStore_;
    TThrottler CompactionThrottler_;
    TThrottler PartitioningThrottler_;
    TStructuredLoggerPtr StructuredLogger_;

    TStatistics Statistics_;

    TPartition* FindPartition(TStore* store) const;

    static auto GetOrderingTuple(const TCompactionRequest& request);

    void StartCompaction(TCompactionRequest request);
    void FinishCompaction(
        TCompactionRequest request,
        std::vector<std::unique_ptr<TStore>> newStores);

    void StartPartitioning(TCompactionRequest request);
    void FinishPartitioning(
        TCompactionRequest request,
        std::vector<std::unique_ptr<TStore>> newStores);

    void DoGetSamples(
        TPartition* partition,
        int count,
        TCallback<void(std::vector<TNativeKey>)> onSamplesReceived);

    void RotateStore(TRotateStoreRequest request);

    void StartSplitPartition(TSplitPartitionRequest request);
    void FinishSplitPartition(TSplitPartitionRequest request, std::vector<TNativeKey> pivots);

    void MergePartitions(TMergePartitionsRequest request);

    void OnStoreFlushed(std::unique_ptr<TStore> store);

    void ResetLastPeriodicRotationTime();

    void UpdateOverlappingStoreCount();

    void AddDynamicStores(NLsm::TTablet* lsmTablet) const;

    bool IsOverflowRotationNeeded() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
