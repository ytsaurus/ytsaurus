#include <yt/core/test_framework/framework.h>

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/unversioned_value.h>


namespace NYT::NTableClient {
namespace {

/* NB: This test intention is to provide a sanity check for stability
 * of FarmHash and FarmFingerprint functions
 * for TUnversionedRow and TUnversionedValue.
 */

/////////////////////////////////////////////////////////////////////////////

class TFarmHashTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<
        std::tuple<TUnversionedValue, TUnversionedValue, ui64, ui64, ui64>>
{ };

TEST_P(TFarmHashTest, TFarmHashUnversionedValueTest)
{
    const auto& params = GetParam();

    TUnversionedValue values[2] = {std::get<0>(params), std::get<1>(params)};
    auto expected1 = std::get<2>(params);
    auto expected2 = std::get<3>(params);
    auto expected3 = std::get<4>(params);

    static_assert(std::is_same<ui64, decltype(GetHash(values[0]))>::value);
    EXPECT_EQ(expected1, GetHash(values[0]));
    EXPECT_EQ(expected2, GetHash(values[1]));

    static_assert(std::is_same<ui64, decltype(GetFarmFingerprint(values[0]))>::value);
    EXPECT_EQ(expected1, GetFarmFingerprint(values[0]));
    EXPECT_EQ(expected2, GetFarmFingerprint(values[1]));

    static_assert(std::is_same<ui64, decltype(GetFarmFingerprint(values, values + 2))>::value);
    EXPECT_EQ(expected3, GetFarmFingerprint(values, values + 2));
}

TEST_P(TFarmHashTest, TFarmHashUnversionedRowTest)
{
    const auto& params = GetParam();

    char buf[sizeof(TUnversionedRowHeader) + 2 * sizeof(TUnversionedValue)];
    *reinterpret_cast<TUnversionedRowHeader*>(buf) = TUnversionedRowHeader{2, 2};
    *reinterpret_cast<TUnversionedValue*>(buf + sizeof(TUnversionedRowHeader)) = std::get<0>(params);
    *reinterpret_cast<TUnversionedValue*>(buf + sizeof(TUnversionedRowHeader) + sizeof(TUnversionedValue)) = std::get<1>(params);
    TUnversionedRow row(reinterpret_cast<TUnversionedRowHeader*>(buf));

    auto expected = std::get<4>(params);

    static_assert(std::is_same<ui64, decltype(GetHash(row))>::value);
    EXPECT_EQ(expected, GetHash(row));

    static_assert(std::is_same<ui64, decltype(GetFarmFingerprint(row))>::value);
    EXPECT_EQ(expected, GetFarmFingerprint(row));
}

INSTANTIATE_TEST_SUITE_P(
    TFarmHashTest,
    TFarmHashTest,
    ::testing::Values(
        std::make_tuple(
            TUnversionedValue{0, EValueType::Int64, false, 0, TUnversionedValueData{.Int64=12345678}},
            TUnversionedValue{1, EValueType::Uint64, true, 0, TUnversionedValueData{.Uint64=42}},
            18329046069279503950ULL,
            17355217915646310598ULL,
            16453323425893019626ULL),
        std::make_tuple(
            TUnversionedValue{1, EValueType::Uint64, true, 0, TUnversionedValueData{.Uint64=12345678}},
            TUnversionedValue{2, EValueType::Boolean, true, 0, TUnversionedValueData{.Boolean=true}},
            18329046069279503950ULL,
            10105606910506535461ULL,
            10502610411105654667ULL),
        std::make_tuple(
            TUnversionedValue{2, EValueType::Double, true, 0, TUnversionedValueData{.Double=42.}},
            TUnversionedValue{3, EValueType::String, false, 1, TUnversionedValueData{.String="0"}},
            6259286942292166412ULL,
            15198969275252572735ULL,
            12125805494429148155ULL),
        std::make_tuple(
            TUnversionedValue{3, EValueType::Boolean, true, 0, TUnversionedValueData{.Boolean=false}},
            TUnversionedValue{4, EValueType::String, false, 0, TUnversionedValueData{.String=""}},
            0ULL,
            11160318154034397263ULL,
            10248854568006048452ULL),
        std::make_tuple(
            TUnversionedValue{4, EValueType::String, false, 3, TUnversionedValueData{.String="abc"}},
            TUnversionedValue{5, EValueType::Int64, false, 3, TUnversionedValueData{.Int64=-1000000}},
            2640714258260161385ULL,
            13952380479379003069ULL,
            9998489714118868374ULL)));

/////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
