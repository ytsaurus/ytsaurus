#include <yt/yt/ytlib/push_based_shuffle_client/record_format.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

namespace NYT::NPushBasedShuffleClient {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleRecordFormat, EmptyFlushReturnsNullopt)
{
    TShuffleRecordBuilder builder(/*mapperId*/ 42, /*startRowId*/ 100);
    EXPECT_FALSE(builder.FlushRecord().has_value());
    // Idempotent: still empty after one failed flush.
    EXPECT_FALSE(builder.FlushRecord().has_value());
}

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleRecordFormat, MultiFlushAdvancesNextRowId)
{
    TShuffleRecordBuilder builder(/*mapperId*/ 7, /*startRowId*/ 1000);

    // First batch: 3 rows.
    TUnversionedRowBuilder rowBuilder;
    for (int i = 0; i < 3; ++i) {
        rowBuilder.Reset();
        rowBuilder.AddValue(MakeUnversionedInt64Value(i, /*id*/ 0));
        builder.AddRow(rowBuilder.GetRow());
    }
    auto a = builder.FlushRecord();
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->Header.MapperId, 7);
    EXPECT_EQ(a->Header.StartRow, 1000);
    EXPECT_EQ(a->Header.RowCount, 3);

    // Second batch: 5 rows.
    for (int i = 0; i < 5; ++i) {
        rowBuilder.Reset();
        rowBuilder.AddValue(MakeUnversionedInt64Value(i + 100, /*id*/ 0));
        builder.AddRow(rowBuilder.GetRow());
    }
    auto b = builder.FlushRecord();
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(b->Header.MapperId, 7);
    EXPECT_EQ(b->Header.StartRow, 1003);   // 1000 + 3
    EXPECT_EQ(b->Header.RowCount, 5);
}

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleRecordFormat, RoundTripMixedTypesLz4)
{
    constexpr i32 MapperId = 999;
    constexpr i64 StartRowId = 50;
    constexpr auto Codec = NCompression::ECodec::Lz4;

    // Build two rows with varied types.
    TShuffleRecordBuilder builder(MapperId, StartRowId);

    TUnversionedRowBuilder rb;
    rb.AddValue(MakeUnversionedInt64Value(-7, /*id*/ 0));
    rb.AddValue(MakeUnversionedStringValue("hello", /*id*/ 1));
    rb.AddValue(MakeUnversionedBooleanValue(true, /*id*/ 2));
    builder.AddRow(rb.GetRow());

    rb.Reset();
    rb.AddValue(MakeUnversionedUint64Value(42u, /*id*/ 0));
    rb.AddValue(MakeUnversionedDoubleValue(3.14, /*id*/ 1));
    rb.AddValue(MakeUnversionedNullValue(/*id*/ 2));
    builder.AddRow(rb.GetRow());

    auto built = builder.FlushRecord();
    ASSERT_TRUE(built.has_value());

    // Wire form via the symmetric Compress/Decompress pair.
    auto wire = CompressShuffleRecord(*built, Codec);

    // Cheap header peek should match without decompression.
    auto peeked = ReadShuffleRecordHeader(wire);
    EXPECT_EQ(peeked.MapperId, MapperId);
    EXPECT_EQ(peeked.StartRow, StartRowId);
    EXPECT_EQ(peeked.RowCount, 2);

    auto decompressed = DecompressShuffleRecord(wire, Codec);
    EXPECT_EQ(decompressed.Header.MapperId, MapperId);
    EXPECT_EQ(decompressed.Header.StartRow, StartRowId);
    EXPECT_EQ(decompressed.Header.RowCount, 2);

    TChunkedMemoryPool pool;
    auto parsed = ParseShuffleRecord(std::move(decompressed), &pool);

    ASSERT_EQ(std::ssize(parsed.Rows), 2);

    // Row 0: int64=-7, string="hello", bool=true.
    {
        auto row = parsed.Rows[0];
        ASSERT_EQ(row.GetCount(), 3u);
        EXPECT_EQ(row[0].Id, 0u);
        EXPECT_EQ(row[0].Type, EValueType::Int64);
        EXPECT_EQ(row[0].Data.Int64, -7);
        EXPECT_EQ(row[1].Id, 1u);
        EXPECT_EQ(row[1].Type, EValueType::String);
        EXPECT_EQ(TStringBuf(row[1].Data.String, row[1].Length), "hello");
        // Zero-copy contract: string bytes live inside UncompressedPayload.
        EXPECT_GE(row[1].Data.String, parsed.UncompressedPayload.Begin());
        EXPECT_LE(row[1].Data.String + row[1].Length, parsed.UncompressedPayload.End());
        EXPECT_EQ(row[2].Id, 2u);
        EXPECT_EQ(row[2].Type, EValueType::Boolean);
        EXPECT_EQ(row[2].Data.Boolean, true);
    }

    // Row 1: uint64=42, double=3.14, null.
    {
        auto row = parsed.Rows[1];
        ASSERT_EQ(row.GetCount(), 3u);
        EXPECT_EQ(row[0].Type, EValueType::Uint64);
        EXPECT_EQ(row[0].Data.Uint64, 42u);
        EXPECT_EQ(row[1].Type, EValueType::Double);
        EXPECT_DOUBLE_EQ(row[1].Data.Double, 3.14);
        EXPECT_EQ(row[2].Type, EValueType::Null);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleRecordFormat, CompositeNormalizesToAny)
{
    constexpr auto Codec = NCompression::ECodec::None;

    TShuffleRecordBuilder builder(/*mapperId*/ 1, /*startRowId*/ 0);

    TUnversionedRowBuilder rb;
    rb.AddValue(MakeUnversionedCompositeValue("[1;2;3]", /*id*/ 0));
    builder.AddRow(rb.GetRow());

    auto built = builder.FlushRecord();
    ASSERT_TRUE(built.has_value());

    auto wire = CompressShuffleRecord(*built, Codec);
    auto decompressed = DecompressShuffleRecord(wire, Codec);
    TChunkedMemoryPool pool;
    auto parsed = ParseShuffleRecord(std::move(decompressed), &pool);

    ASSERT_EQ(std::ssize(parsed.Rows), 1);
    ASSERT_EQ(parsed.Rows[0].GetCount(), 1u);

    // THorizontalBlockWriter::WriteRow rewrites Composite -> Any on write.
    // Parsed output must reflect that.
    EXPECT_EQ(parsed.Rows[0][0].Type, EValueType::Any);
    EXPECT_EQ(TStringBuf(parsed.Rows[0][0].Data.String, parsed.Rows[0][0].Length), "[1;2;3]");
}

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleRecordFormat, ReadHeaderRejectsShortRecord)
{
    std::vector<TSharedRef> tooShort = {
        TSharedRef::FromString("fifteen bytes!!"),  // 15 bytes < sizeof(TRecordHeader) == 16
    };
    EXPECT_THROW_WITH_SUBSTRING(
        ReadShuffleRecordHeader(tooShort),
        "too short");
}

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleRecordFormat, ReadHeaderHandlesSplitHeader)
{
    // Header bytes split across two refs to exercise the multi-ref walk.
    TRecordHeader header{.RowCount = 7, .MapperId = 42, .StartRow = 1000};
    auto bytes = TSharedRef::FromString(std::string(
        reinterpret_cast<const char*>(&header), sizeof(TRecordHeader)));
    std::vector<TSharedRef> wire = {
        bytes.Slice(0u, 5u),                       // first 5 bytes
        bytes.Slice(5u, sizeof(TRecordHeader)),    // remaining 11 bytes
    };
    auto peeked = ReadShuffleRecordHeader(wire);
    EXPECT_EQ(peeked.RowCount, 7);
    EXPECT_EQ(peeked.MapperId, 42);
    EXPECT_EQ(peeked.StartRow, 1000);
}

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleRecordFormat, ParseHandlesMultiRefPayload)
{
    // Build a normal record, decompress it, then split the resulting single-ref
    // UncompressedPayload across two slices to exercise the parser's coalesce
    // branch.
    constexpr auto Codec = NCompression::ECodec::None;

    TShuffleRecordBuilder builder(/*mapperId*/ 1, /*startRowId*/ 0);
    TUnversionedRowBuilder rb;
    rb.AddValue(MakeUnversionedInt64Value(11, /*id*/ 0));
    builder.AddRow(rb.GetRow());
    rb.Reset();
    rb.AddValue(MakeUnversionedInt64Value(22, /*id*/ 0));
    builder.AddRow(rb.GetRow());

    auto built = builder.FlushRecord();
    ASSERT_TRUE(built.has_value());
    auto wire = CompressShuffleRecord(*built, Codec);
    auto decompressed = DecompressShuffleRecord(wire, Codec);
    ASSERT_EQ(decompressed.UncompressedPayload.size(), 1u);

    auto single = std::move(decompressed.UncompressedPayload.front());
    ASSERT_GT(single.Size(), 1u);
    i64 mid = static_cast<i64>(single.Size()) / 2;
    decompressed.UncompressedPayload = {
        single.Slice(0, mid),
        single.Slice(mid, single.Size()),
    };

    TChunkedMemoryPool pool;
    auto parsed = ParseShuffleRecord(std::move(decompressed), &pool);
    ASSERT_EQ(std::ssize(parsed.Rows), 2);
    EXPECT_EQ(parsed.Rows[0][0].Data.Int64, 11);
    EXPECT_EQ(parsed.Rows[1][0].Data.Int64, 22);
}

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleRecordFormat, ParseRejectsNegativeRowCount)
{
    TShuffleRecord record{
        .Header = TRecordHeader{
            .RowCount = -1,
            .MapperId = 0,
            .StartRow = 0,
        },
        .UncompressedPayload = {TSharedRef::FromString("")},
    };

    TChunkedMemoryPool pool;
    EXPECT_THROW_WITH_SUBSTRING(
        ParseShuffleRecord(std::move(record), &pool),
        "negative row count");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NPushBasedShuffleClient
