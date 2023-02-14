#include <yt/yt/ytlib/table_client/chunk_index_builder.h>
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

struct TMockSimpleBlockFormatAdapter
{
    using TBlockReader = TSimpleVersionedBlockReader;

    static std::unique_ptr<TSimpleVersionedBlockWriter> CreateBlockWriter(const TTableSchemaPtr& schema)
    {
        return std::make_unique<TSimpleVersionedBlockWriter>(schema);
    }
};

class TMockChunkIndexBuilder
    : public IChunkIndexBuilder
{
public:
    void ProcessRow(TChunkIndexEntry /*entry*/) override
    {
        return;
    }

    std::vector<TSharedRef> BuildIndex(NProto::TSystemBlockMetaExt* /*systemBlockMetaExt*/) override
    {
        return {};
    }
};

struct TMockIndexedBlockFormatAdapter
{
    using TBlockReader = TIndexedVersionedBlockReader;

    static std::unique_ptr<TIndexedVersionedBlockWriter> CreateBlockWriter(const TTableSchemaPtr& schema)
    {
        static std::optional<TIndexedVersionedBlockFormatDetail> blockFormatDetail;
        blockFormatDetail.emplace(schema);

        return std::make_unique<TIndexedVersionedBlockWriter>(
            schema,
            /*blockIndex*/ 0,
            *blockFormatDetail,
            New<TMockChunkIndexBuilder>());
    }
};

struct TMockSlimBlockFormatAdapter
{
    using TBlockReader = TSlimVersionedBlockReader;

    static std::unique_ptr<TSlimVersionedBlockWriter> CreateBlockWriter(const TTableSchemaPtr& schema)
    {
        return std::make_unique<TSlimVersionedBlockWriter>(
            New<TSlimVersionedWriterConfig>(),
            schema);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMockBlockFormatAdapter>
class TVersionedBlocksTestOneRowBase
    : public ::testing::Test
{
protected:
    TVersionedBlocksTestOneRowBase(TTableSchemaPtr schema)
        : Schema(std::move(schema))
    { }

    const TTableSchemaPtr Schema;

    TKeyComparer KeyComparer;

    TSharedRef Data;
    NProto::TDataBlockMeta Meta;

    TChunkedMemoryPool MemoryPool;


    void SetUp() override
    {
        auto blockWriter = TMockBlockFormatAdapter::CreateBlockWriter(Schema);

        auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 5, 3, 1);
        row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
        row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
        row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);

        // v1
        row.BeginValues()[0] = MakeVersionedInt64Value(8, 11, 3);
        row.BeginValues()[1] = MakeVersionedInt64Value(7, 3, 3);
        // v2
        row.BeginValues()[2] = MakeVersionedBooleanValue(true, 5, 4);
        row.BeginValues()[3] = MakeVersionedBooleanValue(false, 3, 4);
        // v3
        row.BeginValues()[4] = MakeVersionedSentinelValue(EValueType::Null, 5, 5);

        row.BeginWriteTimestamps()[2] = 3;
        row.BeginWriteTimestamps()[1] = 5;
        row.BeginWriteTimestamps()[0] = 11;

        row.BeginDeleteTimestamps()[0] = 9;

        blockWriter->WriteRow(row);

        auto block = blockWriter->FlushBlock();
        Data = MergeRefsToRef<TDefaultBlobTag>(block.Data);
        Meta = block.Meta;
    }

    void DoCheck(
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

static const auto SimpleSchema = New<TTableSchema>(std::vector{
    TColumnSchema("k1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k3", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("v1", EValueType::Int64),
    TColumnSchema("v2", EValueType::Boolean),
    TColumnSchema("v3", EValueType::Int64)
});

template <typename TMockBlockFormatAdapter>
class TVersionedBlocksTestOneRow
    : public TVersionedBlocksTestOneRowBase<TMockBlockFormatAdapter>
{
public:
    TVersionedBlocksTestOneRow()
        : TVersionedBlocksTestOneRowBase<TMockBlockFormatAdapter>(SimpleSchema)
    { }
};

using TVersionedBlockFormatAdapters = ::testing::Types<
    TMockSimpleBlockFormatAdapter,
    TMockIndexedBlockFormatAdapter,
    TMockSlimBlockFormatAdapter
>;

TYPED_TEST_SUITE(TVersionedBlocksTestOneRow, TVersionedBlockFormatAdapters);

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TVersionedBlocksTestOneRow, ReadByTimestamp1)
{
    auto row = TMutableVersionedRow::Allocate(&this->MemoryPool, 5, 3, 1, 0);
    row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
    row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
    row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);
    row.BeginKeys()[3] = MakeUnversionedSentinelValue(EValueType::Null, 3);
    row.BeginKeys()[4] = MakeUnversionedSentinelValue(EValueType::Null, 4);
    row.BeginValues()[0] = MakeVersionedSentinelValue(EValueType::Null, 5, 5);
    row.BeginValues()[1] = MakeVersionedInt64Value(7, 3, 6);
    row.BeginValues()[2] = MakeVersionedBooleanValue(true, 5, 7);
    row.BeginWriteTimestamps()[0] = 5;

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
    row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
    row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
    row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);
    row.BeginDeleteTimestamps()[0] = 9;

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
    row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
    row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
    row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);
    row.BeginWriteTimestamps()[0] = 11;
    row.BeginDeleteTimestamps()[0] = 9;

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
    row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
    row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
    row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);

    // v2
    row.BeginValues()[0] = MakeVersionedSentinelValue(EValueType::Null, 5, 3);

    row.BeginWriteTimestamps()[2] = 3;
    row.BeginWriteTimestamps()[1] = 5;
    row.BeginWriteTimestamps()[0] = 11;

    row.BeginDeleteTimestamps()[0] = 9;

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

static const auto SchemaWithGroups = New<TTableSchema>(std::vector{
    TColumnSchema("k1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k3", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("v1", EValueType::Int64).SetGroup("a"),
    TColumnSchema("v2", EValueType::Boolean),
    TColumnSchema("v3", EValueType::Int64).SetGroup("a")
});

class TIndexedVersionedBlocksTestOneRow
    : public TVersionedBlocksTestOneRowBase<TMockIndexedBlockFormatAdapter>
{
public:
    TIndexedVersionedBlocksTestOneRow()
        : TVersionedBlocksTestOneRowBase<TMockIndexedBlockFormatAdapter>(SchemaWithGroups)
    { }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TIndexedVersionedBlocksTestOneRow, IndexedBlockIsSectorAligned)
{
    EXPECT_TRUE(this->Data.Size() > 0);
    EXPECT_TRUE(AlignUpSpace<i64>(this->Data.Size(), THashTableChunkIndexFormatDetail::SectorSize) == 0);
}

TEST_F(TIndexedVersionedBlocksTestOneRow, IndexedBlockWithGroups)
{
    auto row = TMutableVersionedRow::Allocate(&this->MemoryPool, 5, 3, 1, 0);
    row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
    row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
    row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);
    row.BeginKeys()[3] = MakeUnversionedSentinelValue(EValueType::Null, 3);
    row.BeginKeys()[4] = MakeUnversionedSentinelValue(EValueType::Null, 4);
    row.BeginValues()[0] = MakeVersionedSentinelValue(EValueType::Null, 5, 5);
    row.BeginValues()[1] = MakeVersionedInt64Value(7, 3, 6);
    row.BeginValues()[2] = MakeVersionedBooleanValue(true, 5, 7);
    row.BeginWriteTimestamps()[0] = 5;

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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
