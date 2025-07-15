#include "failing_on_rotation_reader.h"

#include <yt/yt/client/table_client/unversioned_reader.h>

namespace NYT::NTableClient {

using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TFailingOnRotationReader
    : public ISchemafulUnversionedReader
{
public:
    TFailingOnRotationReader(
        ISchemafulUnversionedReaderPtr reader,
        NTabletNode::TTabletSnapshotPtr tabletSnapshot)
        : UnderlyingReader_(std::move(reader))
        , TabletSnapshot_(std::move(tabletSnapshot))
        , ConcurrentStoreRotateErrors_(TabletSnapshot_->TableProfiler->GetSelectRowsCounters(std::nullopt)->ConcurrentStoreRotateErrors)
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        auto result = UnderlyingReader_->Read(options);

        auto epochBeforeRead = TabletSnapshot_->OrderedDynamicStoreRotateEpoch;
        auto epochAfterRead = TabletSnapshot_->TabletRuntimeData->OrderedDynamicStoreRotateEpoch.load();

        if (epochBeforeRead != epochAfterRead) {
            ConcurrentStoreRotateErrors_.Increment(1);
            THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::OrderedDynamicStoreRotateEpochMismatch,
                "Ordered dynamic store rotate epoch mismatch: expected %v, got %v",
                epochBeforeRead,
                epochAfterRead);
        }
        return result;
    }

    TFuture<void> GetReadyEvent() const override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

private:
    const ISchemafulUnversionedReaderPtr UnderlyingReader_;
    const NTabletNode::TTabletSnapshotPtr TabletSnapshot_;

    NProfiling::TCounter ConcurrentStoreRotateErrors_;
};

ISchemafulUnversionedReaderPtr CreateFailingOnRotationReader(
    ISchemafulUnversionedReaderPtr reader,
    NTabletNode::TTabletSnapshotPtr tabletSnapshot)
{
    return New<TFailingOnRotationReader>(std::move(reader), tabletSnapshot);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
