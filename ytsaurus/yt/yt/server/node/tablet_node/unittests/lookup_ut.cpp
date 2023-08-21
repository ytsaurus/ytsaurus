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

const NTableClient::TTableSchemaPtr Schema1 = New<TTableSchema>(std::vector{
    TColumnSchema(TColumnSchema("k", EValueType::Int64).SetSortOrder(NTableClient::ESortOrder::Ascending)),
    TColumnSchema(TColumnSchema("v", EValueType::Int64)),
});

const NTableClient::TTableSchemaPtr Schema2 = New<TTableSchema>(std::vector{
    TColumnSchema(TColumnSchema("k", EValueType::Int64).SetSortOrder(NTableClient::ESortOrder::Ascending)),
    TColumnSchema(TColumnSchema("k2", EValueType::Int64).SetSortOrder(NTableClient::ESortOrder::Ascending)),
    TColumnSchema(TColumnSchema("v2", EValueType::Int64)),
    TColumnSchema(TColumnSchema("v", EValueType::Int64)),
});

////////////////////////////////////////////////////////////////////////////////

class TLookupTest
    : public TSortedStoreManagerTestBase
{
public:
    void SetUp() override
    {
        // NB: Tablet setup is postponed until stores creation.
        TableSchema_ = Schema1;
    }

    TTableSchemaPtr GetSchema() const override
    {
        YT_VERIFY(TableSchema_);
        return TableSchema_;
    }

    void SetTableSchema(const TTableSchemaPtr& schema)
    {
        TableSchema_ = schema;
    }

    void AddChunkStore(
        bool eden,
        const std::vector<TVersionedOwningRow>& rows,
        const TTableSchemaPtr& chunkSchema = nullptr)
    {
        NTabletNode::NProto::TAddStoreDescriptor descriptor;
        descriptor.set_store_type(ToProto<int>(EStoreType::SortedChunk));
        auto storeId = MakeId(
            EObjectType::Chunk,
            InvalidCellTag,
            AddStoreDescriptors_.size(),
            /*hash*/ 0);
        ToProto(descriptor.mutable_store_id(), storeId);

        auto chunkData = ConstructChunkData(
            rows,
            chunkSchema ? chunkSchema : TableSchema_);

        ToProto(descriptor.mutable_chunk_meta(), static_cast<NChunkClient::NProto::TChunkMeta>(*chunkData.Meta));

        OnNewChunkStore(std::move(descriptor), eden);

        TabletContext_.GetBackendChunkReadersHolder()->RegisterBackendChunkReader(storeId, chunkData);
    }

    void OnChunkStoresAdded()
    {
        TSortedStoreManagerTestBase::SetUp();
    }

    TUnversionedOwningRow BuildRow(TString rowString)
    {
        return YsonToSchemafulRow(rowString, *TableSchema_, /*treatMissingAsNull*/ false);
    }

    std::vector<TUnversionedOwningRow> DoLookupRows(
        const std::vector<TUnversionedRow>& keys,
        TTimestamp timestamp = SyncLastCommittedTimestamp,
        std::optional<TTimestamp> retentionTimestamp = std::nullopt,
        const std::vector<int>& columnIndexes = {},
        TTabletSnapshotPtr tabletSnapshot = nullptr,
        NChunkClient::TClientChunkReadOptions chunkReadOptions = TClientChunkReadOptions())
    {
        TReadTimestampRange timestampRange {
            .Timestamp = timestamp,
        };
        if (retentionTimestamp) {
            timestampRange.RetentionTimestamp = *retentionTimestamp;
        }

        return BIND(&LookupRowsImpl,
            Tablet_.get(),
            keys,
            timestampRange,
            columnIndexes,
            tabletSnapshot,
            chunkReadOptions)
            .AsyncVia(LookupQueue_->GetInvoker())
            .Run()
            .Get()
            .ValueOrThrow();
    }

    TVersionedOwningRow DoVersionedLookupRow(const TUnversionedOwningRow& key)
    {
        return BIND(&VersionedLookupRowImpl,
            Tablet_.get(),
            key,
            /*minDataVersions*/ 100,
            AsyncLastCommittedTimestamp,
            ChunkReadOptions_)
            .AsyncVia(LookupQueue_->GetInvoker())
            .Run()
            .Get()
            .ValueOrThrow();
    }

    void ValidateLookup(
        const std::vector<TUnversionedRow>& keys,
        const std::vector<TUnversionedOwningRow>& expected)
    {
        EXPECT_EQ(expected, DoLookupRows(keys));
    }

private:
    const TActionQueuePtr LookupQueue_ = New<TActionQueue>("LookupTest");

    TTableSchemaPtr TableSchema_;


    TChunkData ConstructChunkData(
        const std::vector<TVersionedOwningRow>& rows,
        const TTableSchemaPtr& chunkSchema)
    {
        YT_VERIFY(!rows.empty());

        NChunkClient::NProto::TChunkMeta chunkMeta;
        chunkMeta.set_type(ToProto<int>(EChunkType::Table));
        chunkMeta.set_format(ToProto<int>(EChunkFormat::TableVersionedSimple));

        TTimestamp minTimestamp;
        TTimestamp maxTimestamp;
        i64 dataWeight = 0;
        for (const auto& row : rows) {
            for (auto timestamp : row.WriteTimestamps()) {
                minTimestamp = std::min(minTimestamp, timestamp);
                maxTimestamp = std::max(maxTimestamp, timestamp);
            }
            for (auto timestamp : row.DeleteTimestamps()) {
                minTimestamp = std::min(minTimestamp, timestamp);
                maxTimestamp = std::max(maxTimestamp, timestamp);
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
            TLegacyOwningKey(rows.begin()->Keys()));
        ToProto(
            boundaryKeysExt.mutable_max(),
            TLegacyOwningKey(rows.back().Keys()));
        SetProtoExtension(chunkMeta.mutable_extensions(), boundaryKeysExt);

        SetProtoExtension(chunkMeta.mutable_extensions(), ToProto<TTableSchemaExt>(chunkSchema));

        TDataBlockMetaExt dataBlockMetaExt;

        auto blockWriter = TSimpleVersionedBlockWriter(chunkSchema);
        for (const auto& row : rows) {
            blockWriter.WriteRow(row);
        }
        auto block = blockWriter.FlushBlock();

        block.Meta.set_chunk_row_count(rows.size());
        block.Meta.set_block_index(0);
        ToProto(block.Meta.mutable_last_key(), rows.back().Keys());

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
                "<id=1;ts=100> 0"),
        });

    OnChunkStoresAdded();

    ValidateLookup(
        {YsonToKey("0")},
        {BuildRow("k=0;v=0")});
}

TEST_F(TLookupTest, Empty)
{
    OnChunkStoresAdded();

    EXPECT_FALSE(DoLookupRows({YsonToKey("0")})[0]);
}

TEST_F(TLookupTest, OverlappingChunks)
{
    AddChunkStore(
        /*eden*/ true,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
            YsonToVersionedRow(
                "<id=0> 1",
                "<id=1;ts=100> 1"),
            YsonToVersionedRow(
                "<id=0> 2",
                "<id=1;ts=100> 2"),
        });
    AddChunkStore(
        /*eden*/ true,
        {
            YsonToVersionedRow(
                "<id=0> 2",
                "<id=1;ts=300> 3"),
            YsonToVersionedRow(
                "<id=0> 3",
                "<id=1;ts=100> 4"),
        });
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 1",
                "<id=1;ts=200> 5"),
            YsonToVersionedRow(
                "<id=0> 2",
                "<id=1;ts=200> 6"),
            YsonToVersionedRow(
                "<id=0> 4",
                "<id=1;ts=100> 7"),
        });

    OnChunkStoresAdded();

    ValidateLookup(
        {YsonToKey("0"), YsonToKey("2"), YsonToKey("3")},
        {BuildRow("k=0;v=0"), BuildRow("k=2;v=3"), BuildRow("k=3;v=4")});
    ValidateLookup(
        {YsonToKey("1"), YsonToKey("4")},
        {BuildRow("k=1;v=5"), BuildRow("k=4;v=7")});
}

TEST_F(TLookupTest, RetentionTimestamp)
{
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        });

    OnChunkStoresAdded();

    EXPECT_FALSE(
        DoLookupRows(
            {YsonToKey("0")},
            /*timestamp*/ SyncLastCommittedTimestamp,
            /*retentionTimestamp*/ TTimestamp(200))[0]);
}

TEST_F(TLookupTest, ColumnFilter)
{
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        });

    OnChunkStoresAdded();

    EXPECT_EQ(
        DoLookupRows(
            {YsonToKey("0")},
            SyncLastCommittedTimestamp,
            std::nullopt,
            {0}),
        std::vector<TUnversionedOwningRow>({BuildRow("k=0")}));
}

TEST_F(TLookupTest, MissingRow)
{
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
            YsonToVersionedRow(
                "<id=0> 2",
                "<id=1;ts=100> 2"),
        });

    OnChunkStoresAdded();

    auto lookupResult = DoLookupRows({YsonToKey("0"), YsonToKey("1"), YsonToKey("2")});
    EXPECT_EQ(BuildRow("k=0;v=0"), lookupResult[0]);
    EXPECT_FALSE(lookupResult[1]);
    EXPECT_EQ(BuildRow("k=2;v=2"), lookupResult[2]);
}

TEST_F(TLookupTest, DeletedRow)
{
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        });
    AddChunkStore(
        /*eden*/ true,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "",
                {TTimestamp(200)}),
            YsonToVersionedRow(
                "<id=0> 1",
                "",
                {TTimestamp(200)}),
        });

    OnChunkStoresAdded();

    EXPECT_FALSE(DoLookupRows({YsonToKey("0")})[0]);
    EXPECT_FALSE(DoLookupRows({YsonToKey("1")})[0]);
}

TEST_F(TLookupTest, ExplicitTimestamp)
{
    AddChunkStore(
        /*eden*/ true,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
            YsonToVersionedRow(
                "<id=0> 1",
                "<id=1;ts=100> 1",
                {TTimestamp(200)}),
        });
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=200> 1"),
        });

    OnChunkStoresAdded();

    EXPECT_EQ(
        DoLookupRows(
            {YsonToKey("0"), YsonToKey("1")},
            TTimestamp(150)),
        std::vector<TUnversionedOwningRow>({BuildRow("k=0;v=0"), BuildRow("k=1;v=1")}));
}

TEST_F(TLookupTest, SchemaAlter1)
{
    SetTableSchema(Schema2);

    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        },
        Schema1);

    OnChunkStoresAdded();

    ValidateLookup(
        {YsonToKey("0;#")},
        {BuildRow("k=0;k2=#;v=0;v2=#")});
}

TEST_F(TLookupTest, SchemaAlter2)
{
    SetTableSchema(Schema2);

    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        },
        Schema1);
    AddChunkStore(
        /*eden*/ true,
        {
            YsonToVersionedRow(
                "<id=0> 0; <id=1> #",
                "<id=2;ts=200> 1"),
        });

    OnChunkStoresAdded();

    ValidateLookup(
        {YsonToKey("0;#")},
        {BuildRow("k=0;k2=#;v=0;v2=1")});
}

TEST_F(TLookupTest, DynamicStores)
{
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
            YsonToVersionedRow(
                "<id=0> 1",
                "<id=1;ts=100> 1"),
            YsonToVersionedRow(
                "<id=0> 2",
                "<id=1;ts=100> 2"),
        });

    OnChunkStoresAdded();

    WriteRow(BuildRow("k=1;v=2"));
    WriteRow(BuildRow("k=2;v=3"));

    RotateStores();

    WriteRow(BuildRow("k=2;v=4"));

    ValidateLookup(
        {YsonToKey("0"), YsonToKey("1"), YsonToKey("2")},
        {BuildRow("k=0;v=0"), BuildRow("k=1;v=2"), BuildRow("k=2;v=4")});
}

TEST_F(TLookupTest, TwoPartitions)
{
    AddChunkStore(
        /*eden*/ true,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=100> 0"),
        });
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 0",
                "<id=1;ts=200> 1"),
        });
    AddChunkStore(
        /*eden*/ false,
        {
            YsonToVersionedRow(
                "<id=0> 1",
                "<id=1;ts=100> 2"),
        });

    TableSettings_.MountConfig->MinPartitionDataSize = 1;

    OnChunkStoresAdded();

    EXPECT_EQ(2, std::ssize(Tablet_->PartitionList()));

    ValidateLookup(
        {YsonToKey("0"), YsonToKey("1")},
        {BuildRow("k=0;v=1"), BuildRow("k=1;v=2")});
}

TEST_F(TLookupTest, VersionedLookup)
{
    auto row = YsonToVersionedRow(
        "<id=0> 0",
        "<id=1;ts=100> 0; <id=1;ts=200> 1");

    AddChunkStore(/*eden*/ false, {row});

    OnChunkStoresAdded();

    EXPECT_EQ(
        ToString(DoVersionedLookupRow(BuildRow("k=0"))),
        ToString(row));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode
