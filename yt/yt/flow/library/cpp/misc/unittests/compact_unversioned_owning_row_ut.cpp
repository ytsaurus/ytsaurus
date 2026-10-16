#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/compact_unversioned_owning_row.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSharedRef TEST_STRING = TSharedRef::FromString(std::string("Hello world!"));

TUnversionedOwningRow MakeTestRow()
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(42, 0));
    builder.AddValue(MakeUnversionedNullValue(1));
    builder.AddValue(MakeUnversionedStringValue(TEST_STRING.ToStringBuf(), 2));
    builder.AddValue(MakeUnversionedNullValue(3));
    return builder.FinishRow();
}

std::string GetDiff(TUnversionedRow actual, TUnversionedRow expected)
{
    if (TDefaultUnversionedRowEqual()(actual, expected)) {
        return "";
    }
    return NYT::Format("Has diff. Actual: %v, expected: %v", actual, expected);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCompactUnversionedOwningRowTest, SizeIsEightBytes)
{
    static_assert(sizeof(TCompactUnversionedOwningRow) == 8);
}

TEST(TCompactUnversionedOwningRowTest, DefaultConstructor)
{
    TCompactUnversionedOwningRow row;
    EXPECT_FALSE(row);
    EXPECT_FALSE(row.Get());
    EXPECT_EQ(row.GetCount(), 0);
    EXPECT_EQ(row.GetSpaceUsed(), 0u);
}

TEST(TCompactUnversionedOwningRowTest, ConstructFromRange)
{
    auto original = MakeTestRow();
    TCompactUnversionedOwningRow row(original.Elements());

    EXPECT_TRUE(row);
    EXPECT_TRUE(row.Get());
    EXPECT_EQ(row.GetCount(), original.GetCount());
    EXPECT_EQ(GetDiff(row.Get(), original), "");
}

TEST(TCompactUnversionedOwningRowTest, ConstructFromUnversionedRow)
{
    auto original = MakeTestRow();
    TCompactUnversionedOwningRow row(original.Get());

    EXPECT_TRUE(row);
    EXPECT_EQ(row.GetCount(), original.GetCount());
    EXPECT_EQ(GetDiff(row.Get(), original), "");
}

TEST(TCompactUnversionedOwningRowTest, ConstructFromNullRow)
{
    TCompactUnversionedOwningRow row{TUnversionedRow()};
    EXPECT_FALSE(row);
    EXPECT_EQ(row.GetCount(), 0);
}

TEST(TCompactUnversionedOwningRowTest, CopyIsShared)
{
    auto original = MakeTestRow();
    TCompactUnversionedOwningRow row1(original.Elements());
    TCompactUnversionedOwningRow row2 = row1; // NOLINT(performance-unnecessary-copy-initialization)

    // Both point to the same data.
    EXPECT_EQ(row1.Begin(), row2.Begin());
    EXPECT_EQ(GetDiff(row1.Get(), row2.Get()), "");
}

TEST(TCompactUnversionedOwningRowTest, MoveLeavesSrcEmpty)
{
    auto original = MakeTestRow();
    TCompactUnversionedOwningRow row1(original.Elements());
    TCompactUnversionedOwningRow row2 = std::move(row1);

    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_FALSE(row1);
    EXPECT_TRUE(row2);
    EXPECT_EQ(GetDiff(row2.Get(), original), "");
}

TEST(TCompactUnversionedOwningRowTest, Iteration)
{
    auto original = MakeTestRow();
    TCompactUnversionedOwningRow row(original.Elements());

    int count = 0;
    for (const auto& value : row) {
        EXPECT_EQ(value, original[count]);
        ++count;
    }
    EXPECT_EQ(count, row.GetCount());
}

TEST(TCompactUnversionedOwningRowTest, IndexOperator)
{
    auto original = MakeTestRow();
    TCompactUnversionedOwningRow row(original.Elements());

    for (int i = 0; i < row.GetCount(); ++i) {
        EXPECT_EQ(row[i], original[i]);
    }
}

TEST(TCompactUnversionedOwningRowTest, FirstNElements)
{
    auto original = MakeTestRow();
    TCompactUnversionedOwningRow row(original.Elements());

    auto range = row.FirstNElements(2);
    EXPECT_EQ(std::ssize(range), 2);
    EXPECT_EQ(range[0], original[0]);
    EXPECT_EQ(range[1], original[1]);
}

TEST(TCompactUnversionedOwningRowTest, StringDataIsCopied)
{
    auto original = MakeTestRow();
    TCompactUnversionedOwningRow row(original.Elements());

    // String data must be copied into the row's own allocation.
    EXPECT_NE(
        static_cast<const void*>(row[2].Data.String),
        static_cast<const void*>(original[2].Data.String));

    // But the content must be equal.
    EXPECT_EQ(
        TStringBuf(row[2].Data.String, row[2].Length),
        TStringBuf(original[2].Data.String, original[2].Length));
}

TEST(TCompactUnversionedOwningRowTest, GetSpaceUsed)
{
    auto original = MakeTestRow();
    TCompactUnversionedOwningRow row(original.Elements());

    // Space used should be positive and reasonable.
    EXPECT_GT(row.GetSpaceUsed(), 0u);
    // Should include at least the row data and string data.
    EXPECT_GE(row.GetSpaceUsed(), GetUnversionedRowByteSize(row.GetCount()) + TEST_STRING.Size());
}

TEST(TCompactUnversionedOwningRowTest, RowWithNoStrings)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(1, 0));
    builder.AddValue(MakeUnversionedInt64Value(2, 1));
    builder.AddValue(MakeUnversionedNullValue(2));
    auto original = builder.FinishRow();

    TCompactUnversionedOwningRow row(original.Elements());
    EXPECT_TRUE(row);
    EXPECT_EQ(row.GetCount(), 3);
    EXPECT_EQ(GetDiff(row.Get(), original), "");
}

TEST(TCompactUnversionedOwningRowTest, EmptyRow)
{
    TUnversionedOwningRowBuilder builder;
    auto original = builder.FinishRow();

    TCompactUnversionedOwningRow row(original.Elements());
    EXPECT_TRUE(row);
    EXPECT_EQ(row.GetCount(), 0);
}

TEST(TCompactUnversionedOwningRowTest, MultipleStrings)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedStringValue("foo", 0));
    builder.AddValue(MakeUnversionedInt64Value(42, 1));
    builder.AddValue(MakeUnversionedStringValue("bar baz", 2));
    builder.AddValue(MakeUnversionedStringValue("qux", 3));
    auto original = builder.FinishRow();

    TCompactUnversionedOwningRow row(original.Elements());
    EXPECT_EQ(GetDiff(row.Get(), original), "");

    // All strings must be copied.
    for (int i = 0; i < row.GetCount(); ++i) {
        if (IsStringLikeType(row[i].Type)) {
            EXPECT_NE(
                static_cast<const void*>(row[i].Data.String),
                static_cast<const void*>(original[i].Data.String));
        }
    }
}

TEST(TCompactUnversionedOwningRowTest, MakeCompactUnversionedOwningRowFromValues)
{
    auto expected = MakeUnversionedOwningRow(42, "test", std::nullopt);
    auto row = MakeCompactUnversionedOwningRow(
        MakeUnversionedInt64Value(42, 0),
        MakeUnversionedStringValue("test", 1),
        MakeUnversionedNullValue(2));

    EXPECT_EQ(GetDiff(row.Get(), expected), "");
}

TEST(TCompactUnversionedOwningRowTest, MakeCompactUnversionedOwningRowFromInlineTypes)
{
    auto expected = MakeUnversionedOwningRow(42, 3.14);
    auto row = MakeCompactUnversionedOwningRow(42, 3.14);

    EXPECT_EQ(GetDiff(row.Get(), expected), "");
}

TEST(TCompactUnversionedOwningRowTest, MakeCompactUnversionedOwningRowEmpty)
{
    auto expected = MakeUnversionedOwningRow();
    auto row = MakeCompactUnversionedOwningRow();

    EXPECT_EQ(GetDiff(row.Get(), expected), "");
}

TEST(TCompactUnversionedOwningRowTest, FunctorConstructorNoStrings)
{
    TCompactUnversionedOwningRow row(3, 0, [] (TMutableUnversionedRow r) {
        r[0] = MakeUnversionedInt64Value(42, 0);
        r[1] = MakeUnversionedNullValue(1);
        r[2] = MakeUnversionedInt64Value(100, 2);
    });

    EXPECT_TRUE(row);
    EXPECT_EQ(row.GetCount(), 3);
    EXPECT_EQ(row[0].Data.Int64, 42);
    EXPECT_EQ(row[1].Type, EValueType::Null);
    EXPECT_EQ(row[2].Data.Int64, 100);
}

TEST(TCompactUnversionedOwningRowTest, FunctorConstructorWithString)
{
    TStringBuf str = "hello";
    TCompactUnversionedOwningRow row(2, str.size(), [&] (TMutableUnversionedRow r) {
        r[0] = MakeUnversionedStringValue(str, 0);
        r[1] = MakeUnversionedInt64Value(7, 1);
    });

    EXPECT_TRUE(row);
    EXPECT_EQ(row.GetCount(), 2);
    EXPECT_EQ(row[1].Data.Int64, 7);

    // String content must match.
    EXPECT_EQ(TStringBuf(row[0].Data.String, row[0].Length), str);
    // String data must be copied into the row's own allocation.
    EXPECT_NE(static_cast<const void*>(row[0].Data.String), static_cast<const void*>(str.data()));
}

TEST(TCompactUnversionedOwningRowTest, FunctorConstructorMultipleStrings)
{
    TStringBuf str0 = "foo";
    TStringBuf str2 = "bar baz";
    TStringBuf str3 = "qux";
    size_t stringDataSize = str0.size() + str2.size() + str3.size();

    TCompactUnversionedOwningRow row(4, stringDataSize, [&] (TMutableUnversionedRow r) {
        r[0] = MakeUnversionedStringValue(str0, 0);
        r[1] = MakeUnversionedInt64Value(42, 1);
        r[2] = MakeUnversionedStringValue(str2, 2);
        r[3] = MakeUnversionedStringValue(str3, 3);
    });

    EXPECT_TRUE(row);
    EXPECT_EQ(row.GetCount(), 4);

    EXPECT_EQ(TStringBuf(row[0].Data.String, row[0].Length), str0);
    EXPECT_EQ(row[1].Data.Int64, 42);
    EXPECT_EQ(TStringBuf(row[2].Data.String, row[2].Length), str2);
    EXPECT_EQ(TStringBuf(row[3].Data.String, row[3].Length), str3);

    // All string pointers must be inside the row's own allocation.
    EXPECT_NE(static_cast<const void*>(row[0].Data.String), static_cast<const void*>(str0.data()));
    EXPECT_NE(static_cast<const void*>(row[2].Data.String), static_cast<const void*>(str2.data()));
    EXPECT_NE(static_cast<const void*>(row[3].Data.String), static_cast<const void*>(str3.data()));
}

TEST(TCompactUnversionedOwningRowTest, FunctorConstructorMatchesRangeConstructor)
{
    // Build the same row via range constructor and functor constructor, then compare.
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(1, 0));
    builder.AddValue(MakeUnversionedStringValue("abc", 1));
    builder.AddValue(MakeUnversionedNullValue(2));
    auto original = builder.FinishRow();

    TCompactUnversionedOwningRow rangeRow(original.Elements());

    TStringBuf str = "abc";
    TCompactUnversionedOwningRow functorRow(3, str.size(), [&] (TMutableUnversionedRow r) {
        r[0] = MakeUnversionedInt64Value(1, 0);
        r[1] = MakeUnversionedStringValue(str, 1);
        r[2] = MakeUnversionedNullValue(2);
    });

    EXPECT_EQ(GetDiff(functorRow.Get(), rangeRow.Get()), "");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCompactUnversionedOwningRowWireTest, RoundTrip)
{
    auto original = MakeTestRow();
    TCompactUnversionedOwningRow compactRow(original.Elements());

    std::vector<char> buffer(GetWireByteSize(compactRow));
    char* end = SerializeToBuffer(buffer.data(), compactRow);

    TCompactUnversionedOwningRow parsed;
    const char* cursor = DeserializeFromBuffer(buffer.data(), end, &parsed);

    EXPECT_EQ(cursor, end);
    EXPECT_EQ(GetDiff(parsed.Get(), original), "");
}

TEST(TCompactUnversionedOwningRowWireTest, ByteEqualityWithProto)
{
    auto original = MakeTestRow();
    TCompactUnversionedOwningRow compactRow(original.Elements());

    TProtobufString proto;
    ToProto(&proto, compactRow);

    std::vector<char> buffer(GetWireByteSize(compactRow));
    char* end = SerializeToBuffer(buffer.data(), compactRow);

    EXPECT_EQ(TStringBuf(buffer.data(), end - buffer.data()), TStringBuf(proto));
}

TEST(TCompactUnversionedOwningRowWireTest, EmptyRow)
{
    TCompactUnversionedOwningRow compactRow;

    std::vector<char> buffer(GetWireByteSize(compactRow));
    char* end = SerializeToBuffer(buffer.data(), compactRow);

    TCompactUnversionedOwningRow parsed;
    const char* cursor = DeserializeFromBuffer(buffer.data(), end, &parsed);

    EXPECT_EQ(cursor, end);
    EXPECT_EQ(parsed.GetCount(), 0);
}

TEST(TCompactUnversionedOwningRowWireTest, SurvivesBufferRelease)
{
    auto original = MakeTestRow();

    TCompactUnversionedOwningRow parsed;
    {
        TCompactUnversionedOwningRow compactRow(original.Elements());
        std::vector<char> buffer(GetWireByteSize(compactRow));
        char* end = SerializeToBuffer(buffer.data(), compactRow);
        DeserializeFromBuffer(buffer.data(), end, &parsed);
        // buffer is released at the end of this scope.
    }

    EXPECT_EQ(GetDiff(parsed.Get(), original), "");
    EXPECT_EQ(TStringBuf(parsed[2].Data.String, parsed[2].Length), TEST_STRING.ToStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
