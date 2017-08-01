#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>

#include <cstring>

namespace NYT {
namespace {

using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

struct TWireProtocolTestTag
{ };

class TWireProtocolTest
    : public ::testing::Test
{
public:
    static std::vector<EValueType> MakeValueTypes()
    {
        return {
            EValueType::Null, EValueType::Int64, EValueType::Uint64, EValueType::Double, EValueType::Boolean,
            EValueType::String, EValueType::Any
        };
    }

    static TUnversionedValue MakeValueSample(ui16 id, EValueType type, bool aggregate)
    {
        TUnversionedValue value{};
        value.Id = id;
        value.Type = type;
        value.Aggregate = aggregate;
        value.Length = 0;
        switch (value.Type) {
            case EValueType::Int64:
                value.Data.Uint64 = 0x0123456789ABCDEFULL;
                break;
            case EValueType::Uint64:
                value.Data.Uint64 = 0xFEDCBA9876543210ULL;
                break;
            case EValueType::Double:
                value.Data.Double = 3.141592653589793;
                break;
            case EValueType::String:
                value.Length = 1;
                value.Data.String = "s";
                break;
            case EValueType::Any:
                value.Length = 2;
                value.Data.String = "{}";
                break;
            default:
                break;
        }
        return value;
    }

    static TUnversionedOwningRow MakeUnversionedRowSample()
    {
        TUnversionedOwningRowBuilder builder;
        ui16 id = 0;
        for (auto type : MakeValueTypes()) {
            for (auto aggregate : {true, false}) {
                builder.AddValue(MakeValueSample(id, type, aggregate));
            }
        }
        return builder.FinishRow();
    }

    static TUnversionedOwningRow MakeSchemafulRowSample()
    {
        TUnversionedOwningRowBuilder builder;
        ui16 id = 0;
        for (auto type : MakeValueTypes()) {
            // This one does not support aggregate flags.
            builder.AddValue(MakeValueSample(id, type, false));
        }
        return builder.FinishRow();
    }

    static std::vector<ui32> ExtractSchemaData(TUnversionedRow row, EValueType nullType)
    {
        std::vector<ui32> result;
        for (ui32 index = 0; index < row.GetCount(); ++index) {
            ui32 tmp = static_cast<ui32>(row[index].Id);
            if (row[index].Type == EValueType::Null) {
                tmp |= static_cast<ui32>(nullType) << 16;
            } else {
                tmp |= static_cast<ui32>(row[index].Type) << 16;
            }
            result.push_back(tmp);
        }
        return result;
    }

    // We cannot use CompareRows, because it does not check aggregate bits and does not compare anys.
    void CheckEquals(TUnversionedRow lhs, TUnversionedRow rhs)
    {
        ASSERT_EQ(lhs.GetCount(), rhs.GetCount()) << "rows have different length";

        for (ui32 i = 0; i < lhs.GetCount(); ++i) {
            const auto& lhsValue = lhs[i];
            const auto& rhsValue = rhs[i];

            SCOPED_TRACE(Format("#%v: LHS = %v ; RHS = %v", i, lhsValue, rhsValue));
            EXPECT_EQ(lhsValue.Id, rhsValue.Id);
            EXPECT_EQ(lhsValue.Type, rhsValue.Type);
            EXPECT_EQ(lhsValue.Aggregate, rhsValue.Aggregate);
            // Double-check types here to avoid accessing dangling pointers.
            if (IsStringLikeType(lhsValue.Type) && IsStringLikeType(rhsValue.Type)) {
                EXPECT_EQ(lhsValue.Length, rhsValue.Length);
                EXPECT_EQ(0, memcmp(lhsValue.Data.String, rhsValue.Data.String, lhsValue.Length));
            } else if (lhsValue.Type != EValueType::Null || rhsValue.Type != EValueType::Null){
                EXPECT_EQ(lhsValue.Data.Uint64, rhsValue.Data.Uint64);
            }
        }
    }

    void CheckEquals(const std::vector<unsigned char>& canonical, const TSharedRef& actual)
    {
        ASSERT_EQ(canonical.size(), actual.Size());

        for (size_t i = 0; i < canonical.size(); ++i) {
            auto canonicalByte = static_cast<unsigned char>(canonical[i]);
            auto actualByte = static_cast<unsigned char>(actual[i]);
            if (canonicalByte != 0xcf) { // 0xcf marks garbage due to alignment.
                EXPECT_EQ(canonicalByte, actualByte);
            }
        }
    }

    void DumpSharedRef(const TSharedRef& blob)
    {
        Cerr << "=== BEGIN DUMP SHARED REF ===" << Endl;
        Cerr << "{";
        for (size_t i = 0; i < blob.Size(); ++i) {
            if (i % 16 == 0) {
                Cerr << "\n";
            }
            Cerr << Format("0x%02x, ", static_cast<unsigned char>(blob.Begin()[i]));
        }
        Cerr << Endl << "}" << Endl;
        Cerr << "=== END DUMP SHARED REF ===" << Endl;
    }
};

TEST_F(TWireProtocolTest, UnversionedRow)
{
    auto originalRow = MakeUnversionedRowSample();

    TWireProtocolWriter writer;
    writer.WriteUnversionedRow(originalRow);
    auto blob = MergeRefsToRef<TWireProtocolTestTag>(writer.Finish());
    // DumpSharedRef(blob);

    TWireProtocolReader reader(blob);
    auto reconstructedRow = reader.ReadUnversionedRow(true);
    CheckEquals(originalRow, reconstructedRow);

    // This is a canonical dump. Do not break it.
    const std::vector<unsigned char> canonicalBlob({
        // value count
        0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // one value per row
        0x00, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
        0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
        0x00, 0x00, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
        0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
        0x00, 0x00, 0x05, 0x01, 0x00, 0x00, 0x00, 0x00, 0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40,
        0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40,
        0x00, 0x00, 0x06, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x10, 0x01, 0x01, 0x00, 0x00, 0x00, 0x73, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
        0x00, 0x00, 0x10, 0x00, 0x01, 0x00, 0x00, 0x00, 0x73, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
        0x00, 0x00, 0x11, 0x01, 0x02, 0x00, 0x00, 0x00, 0x7b, 0x7d, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
        0x00, 0x00, 0x11, 0x00, 0x02, 0x00, 0x00, 0x00, 0x7b, 0x7d, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
    });

    CheckEquals(canonicalBlob, blob);
}

TEST_F(TWireProtocolTest, SchemafulRow)
{
    auto originalRow = MakeSchemafulRowSample();

    TWireProtocolWriter writer;
    writer.WriteSchemafulRow(originalRow);
    auto blob = MergeRefsToRef<TWireProtocolTestTag>(writer.Finish());
    // DumpSharedRef(blob);

    TWireProtocolReader reader(blob);
    auto reconstructedRow = reader.ReadSchemafulRow(ExtractSchemaData(originalRow, EValueType::Int64), true);
    CheckEquals(originalRow, reconstructedRow);

    // This is a canonical dump. Do not break it.
    const std::vector<unsigned char> canonicalBlob({
        // value count
        0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // null bitmap
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // one value per row
        0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
        0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
        0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x73, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7b, 0x7d, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
    });

    CheckEquals(canonicalBlob, blob);
}

// Test that schemaful reader/writer properly treats null bitmap.
TEST_F(TWireProtocolTest, Regression1)
{
    const std::vector<unsigned char> blob({
        // value count = 4
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // null bitmap = 1 << 3
        0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // id = 0, type = int64, data = 1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // id = 1, type = int64, data = 1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // id = 2, type = string, data = "2"
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x32, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
        // id = 3, type = string, data = (null)
    });

    const std::vector<ui32> blobSchemaData({
        0x00030000, // id = 0, type = int64
        0x00030001, // id = 1, type = int64
        0x00100002, // id = 2, type = int64
        0x00100003, // id = 3, type = int64
    });

    TWireProtocolReader reader(TSharedRef::MakeCopy<TWireProtocolTestTag>(
        TRef(blob.data(), blob.size())));
    auto row = reader.ReadSchemafulRow(blobSchemaData, false);
    EXPECT_TRUE(reader.GetCurrent() == reader.GetEnd());

    ASSERT_EQ(row.GetCount(), 4);

    EXPECT_EQ(row[0].Id, 0);
    EXPECT_EQ(row[0].Type, EValueType::Int64);
    EXPECT_EQ(row[0].Aggregate, false);
    EXPECT_EQ(row[0].Data.Int64, 1);

    EXPECT_EQ(row[1].Id, 1);
    EXPECT_EQ(row[1].Type, EValueType::Int64);
    EXPECT_EQ(row[1].Aggregate, false);
    EXPECT_EQ(row[1].Data.Int64, 1);

    EXPECT_EQ(row[2].Id, 2);
    EXPECT_EQ(row[2].Type, EValueType::String);
    EXPECT_EQ(row[2].Aggregate, false);
    EXPECT_EQ(row[2].Length, 1);
    EXPECT_EQ(row[2].Data.String[0], '2');

    EXPECT_EQ(row[3].Id, 3);
    EXPECT_EQ(row[3].Type, EValueType::Null);
    EXPECT_EQ(row[3].Aggregate, false);
}

// Test sentinel values (min & max) serializability.
TEST_F(TWireProtocolTest, Regression2)
{
    const std::vector<unsigned char> blob({
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value count = 3
        0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, // id = 0, type = null
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id = 1, type = min
        0x02, 0x00, 0xef, 0x00, 0x00, 0x00, 0x00, 0x00, // id = 2, type = max
    });

    TWireProtocolReader reader(TSharedRef::MakeCopy<TWireProtocolTestTag>(
        TRef(blob.data(), blob.size())));
    auto row = reader.ReadUnversionedRow(true);
    EXPECT_TRUE(reader.GetCurrent() == reader.GetEnd());

    ASSERT_EQ(row.GetCount(), 3);

    EXPECT_EQ(row[0].Id, 0);
    EXPECT_EQ(row[0].Type, EValueType::Null);
    EXPECT_EQ(row[0].Aggregate, false);

    EXPECT_EQ(row[1].Id, 1);
    EXPECT_EQ(row[1].Type, EValueType::Min);
    EXPECT_EQ(row[1].Aggregate, false);

    EXPECT_EQ(row[2].Id, 2);
    EXPECT_EQ(row[2].Type, EValueType::Max);
    EXPECT_EQ(row[2].Aggregate, false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
