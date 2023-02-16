#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_index_builder.h>
#include <yt/yt/ytlib/table_client/chunk_index_read_controller.h>
#include <yt/yt/ytlib/table_client/versioned_block_writer.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/checksum.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTableClient {
namespace {

using namespace NChunkClient;
using namespace NChunkClient::NProto;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const TTableSchemaPtr SimpleSchema = New<TTableSchema>(std::vector{
    TColumnSchema("k", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("v", EValueType::Int64)
});

static const TTableSchemaPtr WidenedSimpleSchema = New<TTableSchema>(std::vector{
    TColumnSchema("k", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("v2", EValueType::Int64),
    TColumnSchema("v", EValueType::Int64)
});

static const TTableSchemaPtr SchemaWithGroups = New<TTableSchema>(std::vector{
    TColumnSchema("k1", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k2", EValueType::String).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("v1", EValueType::Int64).SetGroup("group1"),
    TColumnSchema("v2", EValueType::Int64).SetGroup("group2"),
    TColumnSchema("v3", EValueType::Double),
    TColumnSchema("v4", EValueType::String).SetGroup("group1")
});

static const TTableSchemaPtr WidenedSchemaWithGroups = New<TTableSchema>(std::vector{
    TColumnSchema("k1", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k2", EValueType::String).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k3", EValueType::String).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("v3", EValueType::Double).SetGroup("group2"),
    TColumnSchema("v5", EValueType::Int64),
    TColumnSchema("v2", EValueType::Int64).SetGroup("group1"),
    TColumnSchema("v4", EValueType::String).SetGroup("group2"),
    TColumnSchema("v1", EValueType::Int64).SetGroup("group1"),
    TColumnSchema("v6", EValueType::Int64).SetGroup("group2")
});

////////////////////////////////////////////////////////////////////////////////

class TTestHashTableChunkIndexReadController
    : public ::testing::Test
{
public:
    IChunkIndexReadControllerPtr InitializeController(
        std::vector<TVersionedOwningRow> rows,
        std::vector<int> rowSlots,
        int slotCount,
        const TTableSchemaPtr& writeSchema,
        const TTableSchemaPtr& readSchema,
        const std::vector<TUnversionedOwningRow>& owningKeys,
        std::vector<int> keySlots,
        const TColumnFilter& columnFilter = {},
        TTimestamp timestamp = AsyncLastCommittedTimestamp,
        bool produceAllVersions = false,
        std::optional<int> fingerprintDomainSize = std::nullopt,
        std::optional<int> maxBlockSize = std::nullopt)
    {
        Blocks_.clear();

        YT_VERIFY(rows.size() == rowSlots.size());
        YT_VERIFY(owningKeys.size() == keySlots.size());

        TIndexedVersionedBlockFormatDetail blockFormatDetail(writeSchema);

        auto builderConfig = New<TChunkIndexesWriterConfig>();
        if (maxBlockSize) {
            builderConfig->HashTable->MaxBlockSize = *maxBlockSize;
        }
        YT_VERIFY(slotCount >= std::ssize(rows));
        builderConfig->HashTable->LoadFactor = static_cast<double>(std::ssize(rows)) / slotCount;
        auto chunkIndexBuilder = CreateChunkIndexBuilder(
            builderConfig,
            blockFormatDetail);

        auto blockWriter = TIndexedVersionedBlockWriter(
            writeSchema,
            /*blockIndex*/ 0,
            blockFormatDetail,
            chunkIndexBuilder);

        for (int rowIndex = 0; rowIndex < std::ssize(rows); ++rowIndex) {
            blockWriter.WriteRow(rows[rowIndex]);
        }

        auto block = blockWriter.FlushBlock();

        TChunkMeta protoMeta;
        protoMeta.set_type(ToProto<int>(EChunkType::Table));
        protoMeta.set_format(ToProto<int>(EChunkFormat::TableVersionedIndexed));

        SetProtoExtension(protoMeta.mutable_extensions(), TMiscExt());
        SetProtoExtension(protoMeta.mutable_extensions(), ToProto<NProto::TTableSchemaExt>(writeSchema));

        NProto::TDataBlockMetaExt dataBlockMetaExt;

        block.Meta.set_chunk_row_count(1);
        block.Meta.set_block_index(0);
        ToProto(block.Meta.mutable_last_key(), rows.back().Keys());

        dataBlockMetaExt.add_data_blocks()->Swap(&block.Meta);

        SetProtoExtension(protoMeta.mutable_extensions(), dataBlockMetaExt);

        Blocks_.push_back(
            NCompression::GetCodec(NCompression::ECodec::None)->Compress(block.Data));

        NProto::TSystemBlockMetaExt systemBlockMetaExt;

        for (auto block : chunkIndexBuilder->BuildIndex(&systemBlockMetaExt, rowSlots)) {
            Blocks_.push_back(std::move(block));
        }

        SetProtoExtension(protoMeta.mutable_extensions(), systemBlockMetaExt);

        auto versionedChunkMeta = TCachedVersionedChunkMeta::Create(
            /*preparedColumnarMeta*/ false,
            /*memoryTracker*/ nullptr,
            New<TRefCountedChunkMeta>(std::move(protoMeta)));

        std::vector<TLegacyKey> keys;
        for (auto& key : owningKeys) {
            keys.push_back(key);
        }

        return CreateChunkIndexReadController(
            ChunkId_,
            columnFilter,
            versionedChunkMeta,
            MakeSharedRange(keys),
            /*keyComparer*/ TKeyComparer(),
            readSchema,
            timestamp,
            produceAllVersions,
            TChunkIndexReadControllerTestingOptions{
                .KeySlotIndexes = keySlots,
                .FingerprintDomainSize = fingerprintDomainSize
            });
    }

    std::vector<TSharedRef> GetFragments(
        const std::vector<IChunkFragmentReader::TChunkFragmentRequest>& requests) const
    {
        std::vector<TSharedRef> fragments;
        fragments.reserve(requests.size());
        for (const auto& request : requests) {
            EXPECT_EQ(ChunkId_, request.ChunkId);

            EXPECT_EQ(NErasure::ECodec::None, request.ErasureCodec);
            EXPECT_EQ(std::nullopt, request.BlockSize);

            EXPECT_LT(request.BlockIndex, std::ssize(Blocks_));
            EXPECT_LT(0, request.Length);
            EXPECT_LE(request.BlockOffset + request.Length, std::ssize(Blocks_[request.BlockIndex]));

            fragments.push_back(
                Blocks_[request.BlockIndex].Slice(
                    request.BlockOffset,
                    request.BlockOffset + request.Length));
        }

        return fragments;
    }

    const std::vector<TSharedRef>& GetBlocks() const
    {
        return Blocks_;
    }

private:
    const TChunkId ChunkId_ = TGuid::Create();

    std::vector<TSharedRef> Blocks_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTestHashTableChunkIndexReadController, MissingKey1)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 1",
            "<id=1;ts=100> 1"),
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("0") };

    auto controller = InitializeController(
        rows,
        /*rowSlots*/ {0},
        /*slotCount*/ 1,
        SimpleSchema,
        SimpleSchema,
        keys,
        /*keySlots*/ {0});

    auto requests = controller->GetFragmentRequests();
    EXPECT_EQ(1, std::ssize(requests));

    EXPECT_EQ(THashTableChunkIndexFormatDetail::SectorSize, requests[0].Length);
    EXPECT_EQ(1, requests[0].BlockIndex);
    EXPECT_EQ(0, requests[0].BlockOffset);

    controller->HandleFragmentsResponse(GetFragments(requests));
    EXPECT_TRUE(controller->IsFinished());

    const auto& result = controller->GetResult();
    EXPECT_EQ(1, std::ssize(result));
    ExpectSchemafulRowsEqual(TVersionedRow(), result[0]);
}

TEST_F(TTestHashTableChunkIndexReadController, MissingKey2)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 0",
            "<id=1;ts=100> 0")
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("1") };

    auto controller = InitializeController(
        rows,
        /*rowSlots*/ {0},
        /*slotCount*/ 1,
        SimpleSchema,
        SimpleSchema,
        keys,
        /*keySlots*/ {0});

    EXPECT_TRUE(controller->IsFinished());
    const auto& result = controller->GetResult();
    EXPECT_EQ(1, std::ssize(result));
    ExpectSchemafulRowsEqual(TVersionedRow(), result[0]);
}

TEST_F(TTestHashTableChunkIndexReadController, MissingKey3)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 1",
            "<id=1;ts=100> 1")
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("0") };

    for (int slotIndex = 0; slotIndex < 1; ++slotIndex) {
        auto controller = InitializeController(
            rows,
            /*rowSlots*/ {slotIndex},
            /*slotCount*/ 2,
            SimpleSchema,
            SimpleSchema,
            keys,
            /*keySlots*/ {slotIndex});

        controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
        EXPECT_TRUE(controller->IsFinished());

        const auto& result = controller->GetResult();
        EXPECT_EQ(1, std::ssize(result));
        ExpectSchemafulRowsEqual(TVersionedRow(), result[0]);
    }
}

TEST_F(TTestHashTableChunkIndexReadController, MissingKey4)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 1",
            "<id=1;ts=100> 1"),
        YsonToVersionedRow(
            "<id=0> 2",
            "<id=1;ts=100> 2")
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("0") };

    for (int slotIndex = 0; slotIndex < 1; ++slotIndex) {
        auto controller = InitializeController(
            rows,
            /*rowSlots*/ {slotIndex, 1 - slotIndex},
            /*slotCount*/ 2,
            SimpleSchema,
            SimpleSchema,
            keys,
            /*keySlots*/ {slotIndex});

        controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
        EXPECT_TRUE(controller->IsFinished());

        const auto& result = controller->GetResult();
        EXPECT_EQ(1, std::ssize(result));
        ExpectSchemafulRowsEqual(TVersionedRow(), result[0]);
    }
}

TEST_F(TTestHashTableChunkIndexReadController, SimpleLookup)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 0",
            "<id=1;ts=100> 0")
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("0") };

    auto controller = InitializeController(
        rows,
        /*rowSlots*/ {0},
        /*slotCount*/ 1,
        SimpleSchema,
        SimpleSchema,
        keys,
        /*keySlots*/ {0});

    controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
    EXPECT_FALSE(controller->IsFinished());
    controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));

    EXPECT_TRUE(controller->IsFinished());
    auto result = controller->GetResult();
    EXPECT_EQ(1, std::ssize(rows));
    EXPECT_EQ(1, std::ssize(result));
    ExpectSchemafulRowsEqual(rows[0], result[0]);
}

TEST_F(TTestHashTableChunkIndexReadController, SlotClash)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 0",
            "<id=1;ts=100> 0"),
        YsonToVersionedRow(
            "<id=0> 1",
            "<id=1;ts=100> 1"),
    };

    std::vector<TUnversionedOwningRow> keys = {
        YsonToKey("-2"), YsonToKey("-1"), YsonToKey("0"), YsonToKey("1"), YsonToKey("2"), YsonToKey("3")
    };

    for (int slotIndex = 0; slotIndex < 1; ++slotIndex) {
        auto controller = InitializeController(
            rows,
            /*rowSlots*/ {slotIndex, slotIndex},
            /*slotCount*/ 2,
            SimpleSchema,
            SimpleSchema,
            keys,
            /*keySlots*/ {slotIndex, 1 - slotIndex, slotIndex, slotIndex, slotIndex, 1 - slotIndex});

        auto requests = controller->GetFragmentRequests();
        EXPECT_EQ(1, std::ssize(requests));
        controller->HandleFragmentsResponse(GetFragments(requests));

        requests = controller->GetFragmentRequests();
        EXPECT_EQ(2, std::ssize(requests));
        controller->HandleFragmentsResponse(GetFragments(requests));

        EXPECT_TRUE(controller->IsFinished());
        auto result = controller->GetResult();
        EXPECT_EQ(6, std::ssize(result));
        ExpectSchemafulRowsEqual(TVersionedRow(), result[0]);
        ExpectSchemafulRowsEqual(TVersionedRow(), result[1]);
        ExpectSchemafulRowsEqual(rows[0], result[2]);
        ExpectSchemafulRowsEqual(rows[1], result[3]);
        ExpectSchemafulRowsEqual(TVersionedRow(), result[4]);
        ExpectSchemafulRowsEqual(TVersionedRow(), result[5]);
    }
}

TEST_F(TTestHashTableChunkIndexReadController, ColumnFilter)
{
    TString keyColumns = "<id=0> 0; <id=1> k2";
    std::vector<TString> valueColumns = {
        "<id=2;ts=100> 1",
        "<id=3;ts=100> 2",
        "<id=4;ts=100> 3.3",
        "<id=5;ts=100> v4"
    };

    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            keyColumns,
            valueColumns[0] + ";" + valueColumns[1] + ";" + valueColumns[2] + ";" + valueColumns[3])
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("0; #"), YsonToKey("0; k2"),  YsonToKey("1; #")};

    for (int columnBitmap = 0; columnBitmap < (1 << SchemaWithGroups->GetValueColumnCount()); ++columnBitmap) {
        TStringBuilder builder;
        TDelimitedStringBuilderWrapper delimitedBuilder(&builder, TStringBuf("; "));

        std::vector<int> columnIndexes;
        for (int columnIndex = 0; columnIndex < SchemaWithGroups->GetValueColumnCount(); ++columnIndex) {
            if (columnBitmap & (1 << (columnIndex))) {
                delimitedBuilder->AppendString(valueColumns[columnIndex]);
                columnIndexes.push_back(columnIndex + SchemaWithGroups->GetKeyColumnCount());
            }
        }

        auto controller = InitializeController(
            rows,
            /*rowSlots*/ {0},
            /*slotCount*/ 1,
            SchemaWithGroups,
            SchemaWithGroups,
            keys,
            /*keySlots*/ {0, 0, 0},
            TColumnFilter(columnIndexes));

        auto requests = controller->GetFragmentRequests();
        EXPECT_EQ(1, std::ssize(requests));
        controller->HandleFragmentsResponse(GetFragments(requests));

        requests = controller->GetFragmentRequests();
        controller->HandleFragmentsResponse(GetFragments(requests));

        EXPECT_TRUE(controller->IsFinished());
        auto result = controller->GetResult();
        EXPECT_EQ(3, std::ssize(result));
        ExpectSchemafulRowsEqual(TVersionedRow(), result[0]);
        ExpectSchemafulRowsEqual(
            YsonToVersionedRow(
                keyColumns,
                builder.Flush(),
                /*deleteTimestamps*/ {},
                /*extraWriteTimestamps*/ {100}),
            result[1]);
        ExpectSchemafulRowsEqual(TVersionedRow(), result[2]);
    }
}

TEST_F(TTestHashTableChunkIndexReadController, LookupByTimestamp1)
{
    std::vector<TTimestamp> timestamps = {100, 200};
    std::vector<TString> values = {
        "<id=1;ts=100> 0",
        "<id=1;ts=200> 1"
    };

    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 0",
            values[0] + ";" + values[1])
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("0") };

    for (int valueIndex = 0; valueIndex < 1; ++valueIndex) {
        auto controller = InitializeController(
            rows,
            /*rowSlots*/ {0},
            /*slotCount*/ 1,
            SimpleSchema,
            SimpleSchema,
            keys,
            /*keySlots*/ {0},
            /*columnFilter*/ {},
            timestamps[valueIndex]);

        controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
        controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));

        auto result = controller->GetResult();
        EXPECT_EQ(1, std::ssize(result));

        ExpectSchemafulRowsEqual(
            YsonToVersionedRow(
                "<id=0> 0",
                values[valueIndex]),
            result[0]);
    }
}

TEST_F(TTestHashTableChunkIndexReadController, VersionedLookup)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 0",
            "<id=1;ts=100> 0; <id=1;ts=200> 1")
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("0") };

    auto controller = InitializeController(
        rows,
        /*rowSlots*/ {0},
        /*slotCount*/ 1,
        SimpleSchema,
        SimpleSchema,
        keys,
        /*keySlots*/ {0},
        /*columnFilter*/ {},
        /*timestamp*/ AsyncLastCommittedTimestamp,
        /*produceAllVersions*/ true);

    controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
    controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));

    auto result = controller->GetResult();
    EXPECT_EQ(1, std::ssize(result));

    ExpectSchemafulRowsEqual(rows[0], result[0]);
}

TEST_F(TTestHashTableChunkIndexReadController, WidenedSchema1)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 0",
            "<id=1;ts=100> 0")
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("-1; #"), YsonToKey("0; #"), YsonToKey("0; 0") };

    auto controller = InitializeController(
        rows,
        /*rowSlots*/ {0},
        /*slotCount*/ 2,
        SimpleSchema,
        WidenedSimpleSchema,
        keys,
        /*keySlots*/ {0, 0, 0});

    controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
    controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));

    auto result = controller->GetResult();
    EXPECT_EQ(3, std::ssize(result));
    ExpectSchemafulRowsEqual(TVersionedRow(), result[0]);
    ExpectSchemafulRowsEqual(
        YsonToVersionedRow(
            "<id=0> 0; <id=1> #",
            "<id=3;ts=100> 0"),
        result[1]);
    ExpectSchemafulRowsEqual(TVersionedRow(), result[2]);
}

TEST_F(TTestHashTableChunkIndexReadController, WidenedSchema2)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 0; <id=1> #",
            "<id=2;ts=100> 0; <id=5;ts=100> v5"),
        YsonToVersionedRow(
            "<id=0> 1; <id=1> k2",
            "<id=3;ts=200> 1; <id=4; ts=100> 1.5; <id=4; ts=200> 2.5")
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("0; #; #"), YsonToKey("0; 0; 0"), YsonToKey("1; k2; #") };

    auto controller = InitializeController(
        rows,
        /*rowSlots*/ {1, 1},
        /*slotCount*/ 3,
        SchemaWithGroups,
        WidenedSchemaWithGroups,
        keys,
        /*keySlots*/ {1, 2, 1},
        TColumnFilter({3, 5, 7, 8}));

    controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
    controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));

    auto result = controller->GetResult();
    EXPECT_EQ(3, std::ssize(result));
    ExpectSchemafulRowsEqual(
        YsonToVersionedRow(
            "<id=0> 0; <id=1> #; <id=2> #",
            "<id=7;ts=100> 0;"),
        result[0]);
    ExpectSchemafulRowsEqual(TVersionedRow(), result[1]);
    ExpectSchemafulRowsEqual(
        YsonToVersionedRow(
            "<id=0> 1; <id=1> k2; <id=2> #",
            "<id=3;ts=200> 2.5; <id=5;ts=200> 1"),
        result[2]);
}

TEST_F(TTestHashTableChunkIndexReadController, MultipleSectors)
{
    int rowCount = 300;

    // NB: Such slot count will require multiple sectors for chunk index.
    for (auto slotCount : std::vector<int>{300, 400}) {
        std::vector<TVersionedOwningRow> rows;
        std::vector<int> rowSlots;
        for (int index = 0; index < rowCount; ++index) {
            rows.push_back(YsonToVersionedRow(
                "<id=0> " + ToString(index),
                "<id=1;ts=100> " + ToString(index)));
            rowSlots.push_back((index & 1) ? 130 : 170);
        }

        std::vector<TUnversionedOwningRow> keys = {
            YsonToKey("99"), YsonToKey("100"), YsonToKey("199"), YsonToKey("200"),
            YsonToKey("-2"), YsonToKey("-1")
        };

        auto controller = InitializeController(
            rows,
            rowSlots,
            slotCount,
            SimpleSchema,
            SimpleSchema,
            keys,
            {130, 170, 130, 170, 130, 170});

        auto expectedSectorCount = slotCount == 300 ? 2 : 3;
        EXPECT_EQ(THashTableChunkIndexFormatDetail::SectorSize * expectedSectorCount, std::ssize(GetBlocks()[1]));

        controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
        controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
        if (slotCount == 400) {
            controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
        }

        auto result = controller->GetResult();
        EXPECT_EQ(6, std::ssize(result));

        ExpectSchemafulRowsEqual(rows[99], result[0]);
        ExpectSchemafulRowsEqual(rows[100], result[1]);
        ExpectSchemafulRowsEqual(rows[199], result[2]);
        ExpectSchemafulRowsEqual(rows[200], result[3]);
        ExpectSchemafulRowsEqual(TVersionedRow(), result[4]);
        ExpectSchemafulRowsEqual(TVersionedRow(), result[5]);
    }
}

TEST_F(TTestHashTableChunkIndexReadController, FingerprintClash)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 0",
            "<id=1;ts=100> 0"),
        YsonToVersionedRow(
            "<id=0> 1",
            "<id=1;ts=100> 1")
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("-1"), YsonToKey("0"), YsonToKey("1") };

    auto controller = InitializeController(
        rows,
        {1, 1},
        /*slotCount*/ 2,
        SimpleSchema,
        SimpleSchema,
        keys,
        {1, 1, 1},
        /*columnFilter*/ {},
        /*timestamp*/ AsyncLastCommittedTimestamp,
        /*produceAllVersions*/ false,
        /*fingerprintDomainSize*/ 2);

    auto requests = controller->GetFragmentRequests();
    EXPECT_EQ(1, std::ssize(requests));
    controller->HandleFragmentsResponse(GetFragments(requests));

    requests = controller->GetFragmentRequests();
    EXPECT_EQ(3, std::ssize(requests));
    controller->HandleFragmentsResponse(GetFragments(requests));

    requests = controller->GetFragmentRequests();
    EXPECT_EQ(1, std::ssize(requests));
    controller->HandleFragmentsResponse(GetFragments(requests));

    requests = controller->GetFragmentRequests();
    EXPECT_EQ(2, std::ssize(requests));
    controller->HandleFragmentsResponse(GetFragments(requests));

    EXPECT_TRUE(controller->IsFinished());
    auto result = controller->GetResult();
    EXPECT_EQ(3, std::ssize(result));
    ExpectSchemafulRowsEqual(TVersionedRow(), result[0]);
    ExpectSchemafulRowsEqual(rows[0], result[1]);
    ExpectSchemafulRowsEqual(rows[1], result[2]);
}

TEST_F(TTestHashTableChunkIndexReadController, LookupByTimestamp2)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 0",
            "<id=1;ts=200> 0"),
        YsonToVersionedRow(
            "<id=0> 1",
            "<id=1;ts=100> 1")
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("0"), YsonToKey("1") };

    auto controller = InitializeController(
        rows,
        /*rowSlots*/ {0, 0},
        /*slotCount*/ 2,
        SimpleSchema,
        SimpleSchema,
        keys,
        /*keySlots*/ {0, 0},
        /*columnFilter*/ {},
        /*timestamp*/ 100,
        /*produceAllVersions*/ false,
        /*fingerprintDomainSize*/ 2);

    controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
    controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
    controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));
    controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests()));

    auto result = controller->GetResult();
    EXPECT_EQ(2, std::ssize(result));

    ExpectSchemafulRowsEqual(TVersionedRow(), result[0]);
    ExpectSchemafulRowsEqual(rows[1], result[1]);
}

TEST_F(TTestHashTableChunkIndexReadController, MultipleBlocks)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 1",
            "<id=1;ts=100> 1"),
        YsonToVersionedRow(
            "<id=0> 3",
            "<id=1;ts=100> 3")
    };

    auto getController = [&] (const std::vector<TUnversionedOwningRow>& keys) {
        std::vector<int> keySlots(keys.size(), 0);

        auto controller = InitializeController(
            rows,
            {0, 0},
            /*slotCount*/ 300,
            SimpleSchema,
            SimpleSchema,
            keys,
            keySlots,
            /*columnFilter*/ {},
            /*timestamp*/ AsyncLastCommittedTimestamp,
            /*produceAllVersions*/ false,
            /*fingerprintDomainSize*/ std::nullopt,
            /*maxBlockSize*/ THashTableChunkIndexFormatDetail::SectorSize);

        EXPECT_EQ(3, std::ssize(GetBlocks()));
        EXPECT_EQ(THashTableChunkIndexFormatDetail::SectorSize, std::ssize(GetBlocks()[1]));
        EXPECT_EQ(THashTableChunkIndexFormatDetail::SectorSize, std::ssize(GetBlocks()[2]));

        return controller;
    };

    {
        std::vector<TUnversionedOwningRow> keys = { YsonToKey("0"), YsonToKey("1") };
        auto controller = getController(keys);

        auto requests = controller->GetFragmentRequests();
        EXPECT_EQ(1, std::ssize(requests));
        EXPECT_EQ(1, requests[0].BlockIndex);
        controller->HandleFragmentsResponse(GetFragments(requests));

        requests = controller->GetFragmentRequests();
        EXPECT_EQ(1, std::ssize(requests));
        EXPECT_EQ(0, requests[0].BlockIndex);
        controller->HandleFragmentsResponse(GetFragments(requests));

        auto result = controller->GetResult();
        ExpectSchemafulRowsEqual(TVersionedRow(), result[0]);
        ExpectSchemafulRowsEqual(rows[0], result[1]);
    }

    {
        std::vector<TUnversionedOwningRow> keys = { YsonToKey("2"), YsonToKey("3") };
        auto controller = getController(keys);

        auto requests = controller->GetFragmentRequests();
        EXPECT_EQ(1, std::ssize(requests));
        EXPECT_EQ(2, requests[0].BlockIndex);
        controller->HandleFragmentsResponse(GetFragments(requests));

        requests = controller->GetFragmentRequests();
        EXPECT_EQ(1, std::ssize(requests));
        EXPECT_EQ(0, requests[0].BlockIndex);
        controller->HandleFragmentsResponse(GetFragments(requests));

        auto result = controller->GetResult();
        ExpectSchemafulRowsEqual(TVersionedRow(), result[0]);
        ExpectSchemafulRowsEqual(rows[1], result[1]);
    }

    {
        std::vector<TUnversionedOwningRow> keys = { YsonToKey("1"), YsonToKey("3") };
        auto controller = getController(keys);

        auto requests = controller->GetFragmentRequests();
        EXPECT_EQ(2, std::ssize(requests));
        EXPECT_EQ(1, requests[0].BlockIndex);
        EXPECT_EQ(2, requests[1].BlockIndex);
        controller->HandleFragmentsResponse(GetFragments(requests));

        requests = controller->GetFragmentRequests();
        EXPECT_EQ(2, std::ssize(requests));
        EXPECT_EQ(0, requests[0].BlockIndex);
        EXPECT_EQ(0, requests[1].BlockIndex);
        controller->HandleFragmentsResponse(GetFragments(requests));

        auto result = controller->GetResult();
        ExpectSchemafulRowsEqual(rows[0], result[0]);
        ExpectSchemafulRowsEqual(rows[1], result[1]);
    }

    {
        std::vector<TUnversionedOwningRow> keys = { YsonToKey("0"), YsonToKey("2") };
        auto controller = getController(keys);

        auto requests = controller->GetFragmentRequests();
        EXPECT_EQ(2, std::ssize(requests));
        EXPECT_EQ(1, requests[0].BlockIndex);
        EXPECT_EQ(2, requests[1].BlockIndex);
        controller->HandleFragmentsResponse(GetFragments(requests));

        auto result = controller->GetResult();
        ExpectSchemafulRowsEqual(TVersionedRow(), result[0]);
        ExpectSchemafulRowsEqual(TVersionedRow(), result[1]);
    }
}

TEST_F(TTestHashTableChunkIndexReadController, ChecksumMismatch)
{
    std::vector<TVersionedOwningRow> rows = {
        YsonToVersionedRow(
            "<id=0> 1",
            "<id=1;ts=100> 1")
    };

    std::vector<TUnversionedOwningRow> keys = { YsonToKey("0") };

    auto controller = InitializeController(
        rows,
        /*rowSlots*/ {0},
        /*slotCount*/ 1,
        SimpleSchema,
        SimpleSchema,
        keys,
        /*keySlots*/ {0});

    auto* blockBegin = const_cast<char*>(GetBlocks()[1].Begin());
    blockBegin[0] ^= 1;

    EXPECT_THROW(
        controller->HandleFragmentsResponse(GetFragments(controller->GetFragmentRequests())),
        std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
