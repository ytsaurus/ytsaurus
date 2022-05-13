#include "tablet_context_mock.h"
#include "sorted_store_manager_ut_helpers.h"

#include <yt/yt/server/node/tablet_node/store_manager_detail.h>
#include <yt/yt/server/node/tablet_node/structured_logger.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/versioned_block_writer.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTabletNode {
namespace {

using namespace NCypressClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NObjectClient;

using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TLookupTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        Tablet_ = std::make_unique<TTablet>(
            NullTabletId,
            TTableSettings::CreateNew(),
            /*mountRevision*/ 0,
            /*tableId*/ NullObjectId,
            "lookup_ut",
            &TabletContext_,
            /*schemaId*/ NullObjectId,
            Schema_,
            MinKey(),
            MaxKey(),
            EAtomicity::Full,
            ECommitOrdering::Weak,
            TTableReplicaId(),
            /*retainedTimestamp*/ NullTimestamp,
            /*cumulativeDataWeight*/ 0);

        Tablet_->SetStructuredLogger(CreateMockPerTabletStructuredLogger(Tablet_.get()));

        StoreManager_ = New<TSortedStoreManager>(
            New<TTabletManagerConfig>(),
            Tablet_.get(),
            &TabletContext_);

        Tablet_->SetStoreManager(StoreManager_);
    }

    void AddChunkStore(bool eden, const std::vector<TVersionedOwningRow>& rows)
    {
        NTabletNode::NProto::TAddStoreDescriptor descriptor;
        descriptor.set_store_type(ToProto<int>(EStoreType::SortedChunk));
        auto storeId = MakeId(
            EObjectType::Chunk,
            InvalidCellTag,
            AddStoreDescriptors_.size(),
            /*hash*/ 0);
        ToProto(descriptor.mutable_store_id(), storeId);

        auto chunkData = ConstructChunkData(rows);

        ToProto(descriptor.mutable_chunk_meta(), static_cast<NChunkClient::NProto::TChunkMeta>(*chunkData.Meta));

        AddStoreDescriptors_.push_back(std::move(descriptor));
        if (eden) {
            EdenStoreIds_.push_back(storeId);
        }

        TabletContext_.GetBackendChunkReadersHolder()->RegisterBackendChunkReader(storeId, chunkData);
    }

    void AddDynamicStore()
    {
        TabletContext_.CreateStore(
            Tablet_.get(),
            EStoreType::SortedDynamic,
            MakeId(
                EObjectType::SortedDynamicTabletStore,
                InvalidCellTag,
                ++DynamicStoreCount_,
                /*hash*/ 0),
            /*descriptor*/ nullptr);
    }

    void InitializeTablet()
    {
        std::vector<const NTabletNode::NProto::TAddStoreDescriptor*> addStoreDescriptors;
        for (const auto& descriptor : AddStoreDescriptors_) {
            addStoreDescriptors.push_back(&descriptor);
        }

        NProto::TMountHint mountHint;
        ToProto(mountHint.mutable_eden_store_ids(), EdenStoreIds_);

        StoreManager_->StartEpoch(nullptr);
        StoreManager_->Mount(
            addStoreDescriptors,
            /*hunkChunkDescriptors*/ {},
            /*createDynamicStore*/ true,
            mountHint);
    }

    TUnversionedOwningRow LookupRow(
        TLegacyOwningKey key,
        TTimestamp timestamp = SyncLastCommittedTimestamp)
    {
        // TODO(akozhikhov): Pass chunk read options?
        return LookupRowImpl(
            Tablet_.get(),
            key,
            timestamp,
            /*columnIndexes*/ {},
            /*tabletSnapshot*/ nullptr);
    }

    TUnversionedOwningRow BuildRow(TString rowString)
    {
        return YsonToSchemafulRow(rowString, *Schema_, /*treatMissingAsNull*/ true);
    }

private:
    const NTableClient::TTableSchemaPtr Schema_ = New<TTableSchema>(std::vector{
        TColumnSchema(TColumnSchema("k", EValueType::Int64).SetSortOrder(NTableClient::ESortOrder::Ascending)),
        TColumnSchema(TColumnSchema("v", EValueType::Int64)),
    });

    TTabletContextMock TabletContext_;

    std::unique_ptr<TTablet> Tablet_;
    IStoreManagerPtr StoreManager_;

    int DynamicStoreCount_ = 0;
    std::vector<NTabletNode::NProto::TAddStoreDescriptor> AddStoreDescriptors_;
    std::vector<TChunkId> EdenStoreIds_;


    TChunkData ConstructChunkData(const std::vector<TVersionedOwningRow>& rows)
    {
        YT_VERIFY(!rows.empty());

        NChunkClient::NProto::TChunkMeta chunkMeta;
        chunkMeta.set_type(ToProto<int>(EChunkType::Table));
        chunkMeta.set_format(ToProto<int>(EChunkFormat::TableVersionedSimple));

        TTimestamp minTimestamp;
        TTimestamp maxTimestamp;
        i64 dataWeight = 0;
        for (const auto& row : rows) {
            for (auto* it = row.BeginWriteTimestamps(); it != row.EndWriteTimestamps(); ++it) {
                minTimestamp = std::min(minTimestamp, *it);
                maxTimestamp = std::max(maxTimestamp, *it);
            }
            for (auto* it = row.BeginDeleteTimestamps(); it != row.EndDeleteTimestamps(); ++it) {
                minTimestamp = std::min(minTimestamp, *it);
                maxTimestamp = std::max(maxTimestamp, *it);
            }
            dataWeight += GetDataWeight(row);
        }

        NChunkClient::NProto::TMiscExt miscExt;
        miscExt.set_min_timestamp(minTimestamp);
        miscExt.set_max_timestamp(maxTimestamp);
        miscExt.set_sorted(true);
        miscExt.set_row_count(rows.size());
        miscExt.set_data_weight(dataWeight);
        miscExt.set_compressed_data_size(dataWeight);
        miscExt.set_uncompressed_data_size(dataWeight);
        SetProtoExtension(chunkMeta.mutable_extensions(), miscExt);

        TBoundaryKeysExt boundaryKeysExt;
        ToProto(
            boundaryKeysExt.mutable_min(),
            TLegacyOwningKey(rows.begin()->BeginKeys(), rows.begin()->EndKeys()));
        ToProto(
            boundaryKeysExt.mutable_max(),
            TLegacyOwningKey(rows.back().BeginKeys(), rows.back().EndKeys()));
        SetProtoExtension(chunkMeta.mutable_extensions(), boundaryKeysExt);

        SetProtoExtension(chunkMeta.mutable_extensions(), ToProto<TTableSchemaExt>(Schema_));

        TDataBlockMetaExt dataBlockMetaExt;

        auto blockWriter = TSimpleVersionedBlockWriter(Schema_);
        for (const auto& row : rows) {
            blockWriter.WriteRow(row);
        }
        auto block = blockWriter.FlushBlock();

        block.Meta.set_chunk_row_count(rows.size());
        block.Meta.set_block_index(0);
        ToProto(block.Meta.mutable_last_key(), rows.back().BeginKeys(), rows.back().EndKeys());

        dataBlockMetaExt.add_data_blocks()->Swap(&block.Meta);

        SetProtoExtension(chunkMeta.mutable_extensions(), dataBlockMetaExt);

        auto codec = NCompression::GetCodec(NCompression::ECodec::None);
        auto blockData = codec->Compress(block.Data);

        return TChunkData{
            .Blocks = {blockData},
            .Meta = New<TRefCountedChunkMeta>(chunkMeta)
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TLookupTest, Simple)
{
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 1",
                {},
                {TTimestamp(100)}),
        });

    InitializeTablet();

    EXPECT_EQ(
        ToString(LookupRow(YsonToKey("0"))),
        ToString(BuildRow("k=0;v=1")));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode
