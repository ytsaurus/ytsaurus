#include "ordered_chunk_store.h"
#include "tablet.h"
#include "config.h"

#include <yt/server/tablet_node/tablet_manager.pb.h>

#include <yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/ytlib/table_client/schemaful_chunk_reader.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NTabletNode {

using namespace NYTree;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NApi;
using namespace NDataNode;

using NTabletNode::NProto::TAddStoreDescriptor;
using NChunkClient::NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

struct TOrderedChunkStoreReaderTag
{ };

using TIdMapping = SmallVector<int, TypicalColumnCount>;

class TOrderedChunkStore::TReader
    : public ISchemafulReader
{
public:
    TReader(
        ISchemafulReaderPtr underlyingReader,
        bool enableTabletIndex,
        bool enableRowIndex,
        const TIdMapping& idMapping,
        int tabletIndex,
        i64 lowerRowIndex)
        : UnderlyingReader_(std::move(underlyingReader))
        , TabletIndex_(tabletIndex)
        , EnableTabletIndex_(enableTabletIndex)
        , EnableRowIndex_(enableRowIndex)
        , IdMapping_(idMapping)
        , CurrentRowIndex_(lowerRowIndex)
        , Pool_(TOrderedChunkStoreReaderTag())
    { }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        if (!UnderlyingReader_->Read(rows)) {
            return false;
        }

        Pool_.Clear();
        for (auto& row : *rows) {
            int updatedColumnCount =
                row.GetCount() +
                (EnableTabletIndex_ ? 1 : 0) +
                (EnableRowIndex_ ? 1 : 0);
            auto updatedRow = TMutableUnversionedRow::Allocate(&Pool_, updatedColumnCount);

            auto* updatedValue = updatedRow.Begin();

            if (EnableTabletIndex_) {
                *updatedValue++ = MakeUnversionedInt64Value(TabletIndex_, 0);
            }

            if (EnableRowIndex_) {
                *updatedValue++ = MakeUnversionedInt64Value(CurrentRowIndex_, 1);
            }

            for (const auto& value : row) {
                *updatedValue = value;
                updatedValue->Id = IdMapping_[updatedValue->Id];
                ++updatedValue;
            }

            row = updatedRow;
            ++CurrentRowIndex_;
        }

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

private:
    const ISchemafulReaderPtr UnderlyingReader_;
    const int TabletIndex_;
    const bool EnableTabletIndex_;
    const bool EnableRowIndex_;
    const TIdMapping IdMapping_;

    i64 CurrentRowIndex_;

    TChunkedMemoryPool Pool_;

};

////////////////////////////////////////////////////////////////////////////////

TOrderedChunkStore::TOrderedChunkStore(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet,
    IBlockCachePtr blockCache,
    TChunkRegistryPtr chunkRegistry,
    TChunkBlockManagerPtr chunkBlockManager,
    INativeClientPtr client,
    const TNodeDescriptor& localDescriptor)
    : TStoreBase(config, id, tablet)
    , TChunkStoreBase(
        config,
        id,
        tablet,
        blockCache,
        chunkRegistry,
        chunkBlockManager,
        client,
        localDescriptor)
    , TOrderedStoreBase(config, id, tablet)
{
    LOG_DEBUG("Ordered chunk store created");
}

TOrderedChunkStore::~TOrderedChunkStore()
{
    LOG_DEBUG("Ordered chunk store destroyed");
}

void TOrderedChunkStore::Initialize(const TAddStoreDescriptor* descriptor)
{
    TChunkStoreBase::Initialize(descriptor);
    if (descriptor) {
        YCHECK(descriptor->has_starting_row_index());
        SetStartingRowIndex(descriptor->starting_row_index());
    }
}

TOrderedChunkStorePtr TOrderedChunkStore::AsOrderedChunk()
{
    return this;
}

EStoreType TOrderedChunkStore::GetType() const
{
    return EStoreType::OrderedChunk;
}

ISchemafulReaderPtr TOrderedChunkStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    int tabletIndex,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    const TColumnFilter& columnFilter,
    const TWorkloadDescriptor& workloadDescriptor,
    const TReadSessionId& sessionId)
{
    auto blockCache = GetBlockCache();
    auto chunkReader = GetChunkReader();

    auto config = CloneYsonSerializable(ReaderConfig_);
    config->WorkloadDescriptor = workloadDescriptor;

    TReadLimit lowerLimit;
    lowerRowIndex = std::min(std::max(lowerRowIndex, StartingRowIndex_), StartingRowIndex_ + GetRowCount());
    lowerLimit.SetRowIndex(lowerRowIndex - StartingRowIndex_);

    TReadLimit upperLimit;
    upperRowIndex = std::min(std::max(upperRowIndex, StartingRowIndex_), StartingRowIndex_ + GetRowCount());
    upperLimit.SetRowIndex(upperRowIndex - StartingRowIndex_);

    TReadRange readRange(lowerLimit, upperLimit);

    auto querySchema = tabletSnapshot->QuerySchema.Filter(columnFilter);
    auto readSchema = querySchema.ToValues();

    bool enableTabletIndex = columnFilter.Contains(0);
    bool enableRowIndex = columnFilter.Contains(1);

    TIdMapping idMapping;
    for (const auto& readColumn : readSchema.Columns()) {
        idMapping.push_back(querySchema.GetColumnIndex(readColumn.Name()));
    }

    auto underlyingReader = CreateSchemafulChunkReader(
        config,
        std::move(chunkReader),
        std::move(blockCache),
        sessionId,
        readSchema,
        TKeyColumns(),
        GetChunkMeta(),
        {readRange});

    return New<TReader>(
        std::move(underlyingReader),
        enableTabletIndex,
        enableRowIndex,
        idMapping,
        tabletIndex,
        lowerRowIndex);
}

TKeyComparer TOrderedChunkStore::GetKeyComparer()
{
    return TKeyComparer();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
