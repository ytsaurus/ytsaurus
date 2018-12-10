#pragma once

#include "private.h"
#include "dynamic_store_bits.h"
#include "store_detail.h"

#include <yt/client/table_client/unversioned_row.h>

#include <array>
#include <atomic>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicStore
    : public TDynamicStoreBase
    , public TOrderedStoreBase
{
public:
    TOrderedDynamicStore(
        TTabletManagerConfigPtr config,
        const TStoreId& id,
        TTablet* tablet);
    virtual ~TOrderedDynamicStore();

    //! Returns the reader to be used during flush.
    NTableClient::ISchemafulReaderPtr CreateFlushReader();

    //! Returns the reader to be used during store serialization.
    NTableClient::ISchemafulReaderPtr CreateSnapshotReader();

    TOrderedDynamicRow WriteRow(
        NTableClient::TUnversionedRow row,
        TWriteContext* context);

    TOrderedDynamicRow GetRow(i64 rowIndex);
    std::vector<TOrderedDynamicRow> GetAllRows();

    // IStore implementation.
    virtual EStoreType GetType() const override;
    virtual i64 GetRowCount() const override;

    virtual TCallback<void(TSaveContext&)> AsyncSave() override;
    virtual void AsyncLoad(TLoadContext& context) override;

    virtual TOrderedDynamicStorePtr AsOrderedDynamic() override;

    // IDynamicStore implementation.
    virtual i64 GetTimestampCount() const override;

    // IOrderedStore implementation.
    virtual NTableClient::ISchemafulReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        int tabletIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        const NTableClient::TColumnFilter& columnFilter,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions) override;

private:
    class TReader;

    const std::optional<int> TimestampColumnId_;

    std::atomic<i64> StoreRowCount_ = {0};

    std::array<std::unique_ptr<TOrderedDynamicRowSegment>, MaxOrderedDynamicSegments> Segments_;
    int CurrentSegmentIndex_ = -1;
    i64 CurrentSegmentCapacity_ = -1;
    i64 CurrentSegmentSize_ = -1;

    i64 FlushRowCount_ = -1;


    virtual void OnSetPassive() override;

    void AllocateCurrentSegment(int index);
    void OnMemoryUsageUpdated();

    void CommitRow(TOrderedDynamicRow row);
    void LoadRow(NTableClient::TUnversionedRow row);

    NTableClient::ISchemafulReaderPtr DoCreateReader(
        int tabletIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        const std::optional<NTableClient::TColumnFilter>& columnFilter);

};

DEFINE_REFCOUNTED_TYPE(TOrderedDynamicStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
