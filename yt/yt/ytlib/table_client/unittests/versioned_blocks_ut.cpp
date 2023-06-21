#include <yt/yt/ytlib/table_client/chunk_index_builder.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/versioned_block_reader.h>
#include <yt/yt/ytlib/table_client/versioned_block_writer.h>

#include <yt/yt/ytlib/table_chunk_format/slim_versioned_block_reader.h>
#include <yt/yt/ytlib/table_chunk_format/slim_versioned_block_writer.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/compression/codec.h>

namespace NYT::NTableClient {
namespace {

using namespace NTableChunkFormat;
using namespace NTransactionClient;
using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

class TMockSimpleBlockFormatAdapter
{
public:
    using TBlockReader = TSimpleVersionedBlockReader;

    explicit TMockSimpleBlockFormatAdapter(TTableSchemaPtr schema)
        : Schema_(std::move(schema))
    { }

    std::unique_ptr<TSimpleVersionedBlockWriter> CreateBlockWriter()
    {
        return std::make_unique<TSimpleVersionedBlockWriter>(Schema_);
    }

private:
    const TTableSchemaPtr Schema_;
};

class TMockIndexedBlockFormatAdapter
{
public:
    using TBlockReader = TIndexedVersionedBlockReader;

    explicit TMockIndexedBlockFormatAdapter(TTableSchemaPtr schema)
        : Schema_(std::move(schema))
        , BlockFormatDetail_(Schema_)
    {
        ChunkIndexBuilder_ = CreateChunkIndexBuilder(
            New<TChunkIndexesWriterConfig>(),
            BlockFormatDetail_,
            /*logger*/ {});
    }

    std::unique_ptr<TIndexedVersionedBlockWriter> CreateBlockWriter() const
    {
        return std::make_unique<TIndexedVersionedBlockWriter>(
            Schema_,
            /*blockIndex*/ 0,
            BlockFormatDetail_,
            ChunkIndexBuilder_);
    }

    const IChunkIndexBuilderPtr& GetChunkIndexBuilder() const
    {
        return ChunkIndexBuilder_;
    }

private:
    const TTableSchemaPtr Schema_;
    const TIndexedVersionedBlockFormatDetail BlockFormatDetail_;

    IChunkIndexBuilderPtr ChunkIndexBuilder_;
};

struct TMockSlimBlockFormatAdapter
{
    using TBlockReader = TSlimVersionedBlockReader;

    explicit TMockSlimBlockFormatAdapter(TTableSchemaPtr schema)
        : Schema_(std::move(schema))
    { }

    std::unique_ptr<TSlimVersionedBlockWriter> CreateBlockWriter()
    {
        return std::make_unique<TSlimVersionedBlockWriter>(
            New<TSlimVersionedWriterConfig>(),
            Schema_);
    }

private:
    const TTableSchemaPtr Schema_;
};

////////////////////////////////////////////////////////////////////////////////

static const auto SimpleSchema = New<TTableSchema>(std::vector{
    TColumnSchema("k1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k3", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("v1", EValueType::Int64),
    TColumnSchema("v2", EValueType::Boolean),
    TColumnSchema("v3", EValueType::Int64)
});

static const auto SchemaWithGroups = New<TTableSchema>(std::vector{
    TColumnSchema("k1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k3", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("v1", EValueType::Int64).SetGroup("a"),
    TColumnSchema("v2", EValueType::Boolean),
    TColumnSchema("v3", EValueType::Int64).SetGroup("a")
});

////////////////////////////////////////////////////////////////////////////////

template <typename TMockBlockFormatAdapter, bool UseSchemaWithGroups>
class TVersionedBlocksTestOneRowBase
    : public ::testing::Test
    , public TMockBlockFormatAdapter
{
protected:
    TVersionedBlocksTestOneRowBase()
        : TMockBlockFormatAdapter(UseSchemaWithGroups ? SchemaWithGroups : SimpleSchema)
        , Schema(UseSchemaWithGroups ? SchemaWithGroups : SimpleSchema)
    { }

    const TTableSchemaPtr Schema;

    TKeyComparer KeyComparer;

    TSharedRef Data;
    NProto::TDataBlockMeta Meta;

    TChunkedMemoryPool MemoryPool;

    TMutableVersionedRow Row;


    void SetUp() override
    {
        auto blockWriter = TMockBlockFormatAdapter::CreateBlockWriter();

        Row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 5, 3, 1);
        Row.Keys()[0] = MakeUnversionedStringValue("a", 0);
        Row.Keys()[1] = MakeUnversionedInt64Value(1, 1);
        Row.Keys()[2] = MakeUnversionedDoubleValue(1.5, 2);

        // v1
        Row.Values()[0] = MakeVersionedInt64Value(8, 11, 3);
        Row.Values()[1] = MakeVersionedInt64Value(7, 3, 3);
        // v2
        Row.Values()[2] = MakeVersionedBooleanValue(true, 5, 4);
        Row.Values()[3] = MakeVersionedBooleanValue(false, 3, 4);
        // v3
        Row.Values()[4] = MakeVersionedSentinelValue(EValueType::Null, 5, 5);

        Row.WriteTimestamps()[2] = 3;
        Row.WriteTimestamps()[1] = 5;
        Row.WriteTimestamps()[0] = 11;

        Row.DeleteTimestamps()[0] = 9;

        blockWriter->WriteRow(Row);

        auto block = blockWriter->FlushBlock();
        Data = MergeRefsToRef<TDefaultBlobTag>(block.Data);
        Meta = block.Meta;
    }

    virtual void DoCheck(
        const std::vector<TVersionedRow>& rows,
        int keyColumnCount,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        TTimestamp timestamp,
        bool produceAllVersions)
    {
        typename TMockBlockFormatAdapter::TBlockReader reader(
            Data,
            Meta,
            /*blockFormatVersion*/ 1,
            Schema,
            keyColumnCount,
            schemaIdMapping,
            KeyComparer,
            timestamp,
            produceAllVersions);
        reader.SkipToRowIndex(0);

        int i = 0;
        do {
            EXPECT_LT(i, std::ssize(rows));
            auto row = reader.GetRow(&MemoryPool);
            ExpectSchemafulRowsEqual(rows[i++], row);
        } while (reader.NextRow());
        EXPECT_EQ(i, std::ssize(rows));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <bool UseSchemaWithGroups>
class TIndexedVersionedBlocksTestOneRowBase
    : public TVersionedBlocksTestOneRowBase<TMockIndexedBlockFormatAdapter, UseSchemaWithGroups>
{
public:
    void DoCheck(
        const std::vector<TVersionedRow>& rows,
        int keyColumnCount,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        TTimestamp timestamp,
        bool produceAllVersions) override
    {
        YT_VERIFY(rows.size() == 1);

        // First check block reader.

        TVersionedBlocksTestOneRowBase<TMockIndexedBlockFormatAdapter, UseSchemaWithGroups>::DoCheck(
            rows,
            keyColumnCount,
            schemaIdMapping,
            timestamp,
            produceAllVersions);

        // Now check row reader.

        const auto& chunkIndexBuilder = this->GetChunkIndexBuilder();
        NProto::TSystemBlockMetaExt systemBlockMeta;
        auto chunkIndex = chunkIndexBuilder->BuildIndex(rows.back().Keys(), &systemBlockMeta)[0];

        TIndexedVersionedBlockFormatDetail blockFormatDetail(this->Schema);
        auto hashTableChunkIndexBlockMeta = systemBlockMeta
            .system_blocks(0)
            .GetExtension(NProto::THashTableChunkIndexSystemBlockMeta::hash_table_chunk_index_system_block_meta_ext);
        THashTableChunkIndexFormatDetail indexFormatDetail(
            hashTableChunkIndexBlockMeta.seed(),
            hashTableChunkIndexBlockMeta.slot_count(),
            blockFormatDetail.GetGroupCount(),
            /*groupReorderingEnabled*/ false);

        EXPECT_EQ(1, indexFormatDetail.GetSectorCount());
        EXPECT_EQ(THashTableChunkIndexFormatDetail::SectorSize, indexFormatDetail.GetChunkIndexByteSize());
        EXPECT_LT(
            static_cast<int>(sizeof(THashTableChunkIndexFormatDetail::TSerializableFingerprint)),
            indexFormatDetail.GetEntryByteSize());

        auto firstEntry = chunkIndex.Slice(0, indexFormatDetail.GetEntryByteSize()).Begin();
        THashTableChunkIndexFormatDetail::TSerializableFingerprint firstFingerprint;
        ReadPod(firstEntry, firstFingerprint);
        auto firstEntryPresent = indexFormatDetail.IsEntryPresent(firstFingerprint);

        auto secondEntry = chunkIndex.Slice(indexFormatDetail.GetEntryByteSize(), 2 * indexFormatDetail.GetEntryByteSize()).Begin();
        THashTableChunkIndexFormatDetail::TSerializableFingerprint secondFingerprint;
        ReadPod(secondEntry, secondFingerprint);
        auto secondEntryPresent = indexFormatDetail.IsEntryPresent(secondFingerprint);

        EXPECT_TRUE(firstEntryPresent ^ secondEntryPresent);

        struct TChunkIndexEntry
        {
            int BlockIndex;
            i64 BlockOffset;
            i64 Length;
            std::vector<int> GroupOffsets;
            std::vector<int> GroupIndexes;
        };

        auto getEntry = [&] (const char* entry) {
            TChunkIndexEntry chunkIndexEntry;
            ReadPod(entry, chunkIndexEntry.BlockIndex);
            ReadPod(entry, chunkIndexEntry.BlockOffset);
            ReadPod(entry, chunkIndexEntry.Length);
            if (blockFormatDetail.GetGroupCount() > 1) {
                for (int i = 0; i < blockFormatDetail.GetGroupCount(); ++i) {
                    ReadPod(entry, chunkIndexEntry.GroupOffsets.emplace_back());
                }
                if (/*groupReorderingEnabled*/ false) {
                    ReadPod(entry, chunkIndexEntry.GroupIndexes.emplace_back());
                }
            }
            return chunkIndexEntry;
        };

        auto chunkIndexEntry = getEntry((firstEntryPresent ? firstEntry : secondEntry));

        EXPECT_EQ(0, chunkIndexEntry.BlockIndex);
        EXPECT_EQ(0, chunkIndexEntry.BlockOffset);
        EXPECT_LE(chunkIndexEntry.Length, std::ssize(this->Data));

        auto actualRow = TIndexedVersionedRowReader(
            keyColumnCount,
            schemaIdMapping,
            timestamp,
            produceAllVersions,
            this->Schema,
            /*groupIndexesToRead*/ std::vector<int>{})
            .ProcessAndGetRow(
                {this->Data.Slice(0, chunkIndexEntry.Length)},
                chunkIndexEntry.GroupOffsets.data(),
                chunkIndexEntry.GroupIndexes.data(),
                &this->MemoryPool);
        ExpectSchemafulRowsEqual(rows[0], actualRow);

        if (blockFormatDetail.GetGroupCount() > 1) {
            auto groupIndexesToRead = blockFormatDetail.GetGroupIndexesToRead(schemaIdMapping);

            TCompactVector<TSharedRef, IndexedRowTypicalGroupCount> rowData;
            rowData.push_back(this->Data.Slice(0, chunkIndexEntry.GroupOffsets[0]));
            for (auto groupIndex : groupIndexesToRead) {
                rowData.push_back(this->Data.Slice(
                    chunkIndexEntry.GroupOffsets[groupIndex],
                    groupIndex + 1 == std::ssize(chunkIndexEntry.GroupOffsets)
                    ? chunkIndexEntry.Length - sizeof(TChecksum)
                    : chunkIndexEntry.GroupOffsets[groupIndex + 1]));
            }

            auto actualRow = TIndexedVersionedRowReader(
                keyColumnCount,
                schemaIdMapping,
                timestamp,
                produceAllVersions,
                this->Schema,
                groupIndexesToRead)
                .ProcessAndGetRow(
                    rowData,
                    chunkIndexEntry.GroupOffsets.data(),
                    chunkIndexEntry.GroupIndexes.data(),
                    &this->MemoryPool);
            ExpectSchemafulRowsEqual(rows[0], actualRow);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

using TSimpleVersionedBlocksTestOneRowImpl = TVersionedBlocksTestOneRowBase<TMockSimpleBlockFormatAdapter, false>;
using TIndexedVersionedBlocksTestOneRowImpl = TIndexedVersionedBlocksTestOneRowBase<false>;
using TIndexedWithGroupsVersionedBlocksTestOneRowImpl = TIndexedVersionedBlocksTestOneRowBase<true>;
using TSlimVersionedBlocksTestOneRowImpl = TVersionedBlocksTestOneRowBase<TMockSlimBlockFormatAdapter, false>;

using TVersionedBlockTestOneRowImpls = ::testing::Types<
    TSimpleVersionedBlocksTestOneRowImpl,
    TIndexedVersionedBlocksTestOneRowImpl,
    TIndexedWithGroupsVersionedBlocksTestOneRowImpl,
    TSlimVersionedBlocksTestOneRowImpl
>;

template <typename TTestImpl>
class TVersionedBlocksTestOneRow
    : public TTestImpl
{
public:
    TVersionedBlocksTestOneRow()
        : TTestImpl()
    { }
};

TYPED_TEST_SUITE(TVersionedBlocksTestOneRow, TVersionedBlockTestOneRowImpls);

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TVersionedBlocksTestOneRow, ReadByTimestamp1)
{
    auto row = TMutableVersionedRow::Allocate(&this->MemoryPool, 5, 3, 1, 0);
    row.Keys()[0] = MakeUnversionedStringValue("a", 0);
    row.Keys()[1] = MakeUnversionedInt64Value(1, 1);
    row.Keys()[2] = MakeUnversionedDoubleValue(1.5, 2);
    row.Keys()[3] = MakeUnversionedSentinelValue(EValueType::Null, 3);
    row.Keys()[4] = MakeUnversionedSentinelValue(EValueType::Null, 4);
    row.Values()[0] = MakeVersionedSentinelValue(EValueType::Null, 5, 5);
    row.Values()[1] = MakeVersionedInt64Value(7, 3, 6);
    row.Values()[2] = MakeVersionedBooleanValue(true, 5, 7);
    row.WriteTimestamps()[0] = 5;

    std::vector<TVersionedRow> rows;
    rows.push_back(row);

    // Reorder value columns in reading schema.
    std::vector<TColumnIdMapping> schemaIdMapping = {{5, 5}, {3, 6}, {4, 7}};

    this->DoCheck(
        rows,
        this->Schema->GetKeyColumnCount() + 2, // Two padding key columns.
        schemaIdMapping,
        /*timestamp*/ 7,
        /*produceAllVersions*/ false);
}

TYPED_TEST(TVersionedBlocksTestOneRow, ReadByTimestamp2)
{
    auto row = TMutableVersionedRow::Allocate(&this->MemoryPool, 3, 0, 0, 1);
    row.Keys()[0] = MakeUnversionedStringValue("a", 0);
    row.Keys()[1] = MakeUnversionedInt64Value(1, 1);
    row.Keys()[2] = MakeUnversionedDoubleValue(1.5, 2);
    row.DeleteTimestamps()[0] = 9;

    std::vector<TVersionedRow> rows;
    rows.push_back(row);

    std::vector<TColumnIdMapping> schemaIdMapping = {{4, 5}};

    this->DoCheck(
        rows,
        this->Schema->GetKeyColumnCount(),
        schemaIdMapping,
        /*timestamp*/ 9,
        /*produceAllVersions*/ false);
}

TYPED_TEST(TVersionedBlocksTestOneRow, ReadLastCommitted)
{
    auto row = TMutableVersionedRow::Allocate(&this->MemoryPool, 3, 0, 1, 1);
    row.Keys()[0] = MakeUnversionedStringValue("a", 0);
    row.Keys()[1] = MakeUnversionedInt64Value(1, 1);
    row.Keys()[2] = MakeUnversionedDoubleValue(1.5, 2);
    row.WriteTimestamps()[0] = 11;
    row.DeleteTimestamps()[0] = 9;

    std::vector<TVersionedRow> rows;
    rows.push_back(row);

    std::vector<TColumnIdMapping> schemaIdMapping = {{4, 3}};

    this->DoCheck(
        rows,
        this->Schema->GetKeyColumnCount(),
        schemaIdMapping,
        SyncLastCommittedTimestamp,
        /*produceAllVersions*/ false);
}

TYPED_TEST(TVersionedBlocksTestOneRow, ReadAllCommitted)
{
    auto row = TMutableVersionedRow::Allocate(&this->MemoryPool, 3, 1, 3, 1);
    row.Keys()[0] = MakeUnversionedStringValue("a", 0);
    row.Keys()[1] = MakeUnversionedInt64Value(1, 1);
    row.Keys()[2] = MakeUnversionedDoubleValue(1.5, 2);

    // v2
    row.Values()[0] = MakeVersionedSentinelValue(EValueType::Null, 5, 3);

    row.WriteTimestamps()[2] = 3;
    row.WriteTimestamps()[1] = 5;
    row.WriteTimestamps()[0] = 11;

    row.DeleteTimestamps()[0] = 9;

    std::vector<TVersionedRow> rows;
    rows.push_back(row);

    // Read only last non-key column.
    std::vector<TColumnIdMapping> schemaIdMapping = {{5, 3}};

    this->DoCheck(
        rows,
        this->Schema->GetKeyColumnCount(),
        schemaIdMapping,
        AllCommittedTimestamp,
        /*produceAllVersions*/ true);
}

////////////////////////////////////////////////////////////////////////////////

using TIndexedVersionedBlockTestOneRowImpls = ::testing::Types<
    TIndexedVersionedBlocksTestOneRowImpl,
    TIndexedWithGroupsVersionedBlocksTestOneRowImpl
>;

template <typename TTestImpl>
class TIndexedVersionedBlocksTestOneRow
    : public TTestImpl
{
public:
    TIndexedVersionedBlocksTestOneRow()
        : TTestImpl()
    { }
};

TYPED_TEST_SUITE(TIndexedVersionedBlocksTestOneRow, TIndexedVersionedBlockTestOneRowImpls);

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TIndexedVersionedBlocksTestOneRow, IndexedBlockIsSectorAligned)
{
    EXPECT_LT(0, std::ssize(this->Data));
    EXPECT_EQ(0, AlignUpSpace<i64>(this->Data.Size(), THashTableChunkIndexFormatDetail::SectorSize));
}

TYPED_TEST(TIndexedVersionedBlocksTestOneRow, HashTableChunkIndexMeta)
{
    const auto& chunkIndexBuilder = this->GetChunkIndexBuilder();
    NProto::TSystemBlockMetaExt systemBlockMeta;
    auto chunkIndex = chunkIndexBuilder->BuildIndex(this->Row.Keys(), &systemBlockMeta);

    EXPECT_EQ(1, systemBlockMeta.system_blocks_size());

    auto systemBlock = systemBlockMeta.system_blocks(0);
    auto chunkIndexBlockMetaExt = systemBlock.GetExtension(
        NProto::THashTableChunkIndexSystemBlockMeta::hash_table_chunk_index_system_block_meta_ext);
    EXPECT_EQ(2, chunkIndexBlockMetaExt.slot_count());
    auto lastKey = ::NYT::FromProto<TLegacyOwningKey>(chunkIndexBlockMetaExt.last_key());
    EXPECT_EQ(
        0,
        TComparator({ESortOrder::Ascending, ESortOrder::Ascending, ESortOrder::Ascending}).CompareKeys(
            TKey::FromRow(lastKey),
            TKey(this->Row.Keys())));

    EXPECT_EQ(1, std::ssize(chunkIndex));
    EXPECT_LT(0, std::ssize(chunkIndex[0]));
    EXPECT_EQ(0, AlignUpSpace<i64>(chunkIndex[0].Size(), THashTableChunkIndexFormatDetail::SectorSize));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
