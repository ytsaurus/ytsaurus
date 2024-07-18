#pragma once

#include "private.h"
#include "dynamic_store_bits.h"
#include "store_detail.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <array>
#include <atomic>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicStore
    : public TDynamicStoreBase
    , public TOrderedStoreBase
{
public:
    TOrderedDynamicStore(
        TTabletManagerConfigPtr config,
        TStoreId id,
        TTablet* tablet);

    //! Returns the reader to be used during flush.
    NTableClient::ISchemafulUnversionedReaderPtr CreateFlushReader();

    //! Returns the reader to be used during store serialization.
    NTableClient::ISchemafulUnversionedReaderPtr CreateSnapshotReader();

    TOrderedDynamicRow WriteRow(
        NTableClient::TUnversionedRow row,
        TWriteContext* context);

    TOrderedDynamicRow GetRow(i64 rowIndex);
    std::vector<TOrderedDynamicRow> GetAllRows();

    // IStore implementation.
    EStoreType GetType() const override;
    i64 GetRowCount() const override;

    void Save(TSaveContext& context) const override;
    void Load(TLoadContext& context) override;

    TCallback<void(TSaveContext&)> AsyncSave() override;
    void AsyncLoad(TLoadContext& context) override;

    TOrderedDynamicStorePtr AsOrderedDynamic() override;

    // IDynamicStore implementation.
    i64 GetTimestampCount() const override;

    // IOrderedStore implementation.
    NTableClient::ISchemafulUnversionedReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        int tabletIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        NTransactionClient::TTimestamp timestamp,
        const NTableClient::TColumnFilter& columnFilter,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        std::optional<EWorkloadCategory> workloadCategory) override;

    std::vector<NTableClient::THunkChunkRef> GetHunkStoreRefs() const;

    void LockHunkStores(const NTableClient::THunkChunksInfo& hunkChunksInfo) override;

    void UpdateCommittedRowCount();

private:
    class TReader;

    const IHunkLockManagerPtr HunkLockManager_;

    const std::optional<int> TimestampColumnId_;
    const std::optional<int> CumulativeDataWeightColumnId_;

    std::atomic<i64> StoreRowCount_ = 0;
    std::atomic<i64> CommittedStoreRowCount_ = 0;

    std::array<std::unique_ptr<TOrderedDynamicRowSegment>, MaxOrderedDynamicSegments> Segments_;
    int CurrentSegmentIndex_ = -1;
    i64 CurrentSegmentCapacity_ = -1;
    i64 CurrentSegmentSize_ = -1;

    i64 FlushRowCount_ = -1;

    THashMap<NChunkClient::TChunkId, NTableClient::THunkChunkRef> HunkStoreRefs_;

    void OnSetPassive() override;
    void OnSetRemoved() override;

    void AllocateCurrentSegment(int index);
    void OnDynamicMemoryUsageUpdated();

    void CommitRow(TOrderedDynamicRow row);
    void LoadRow(NTableClient::TUnversionedRow row);

    NTableClient::ISchemafulUnversionedReaderPtr DoCreateReader(
        int tabletIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        NTransactionClient::TTimestamp timestamp,
        const std::optional<NTableClient::TColumnFilter>& columnFilter);
};

DEFINE_REFCOUNTED_TYPE(TOrderedDynamicStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
