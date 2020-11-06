#include <yt/core/test_framework/framework.h>

#include <yt/client/table_client/key_bound.h>
#include <yt/client/table_client/comparator.h>
#include <yt/client/table_client/helpers.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyBoundTest, Simple)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedDoubleValue(3.14, 0));
    builder.AddValue(MakeUnversionedInt64Value(-42, 1));
    builder.AddValue(MakeUnversionedUint64Value(27, 2));
    TString str = "Foo";
    builder.AddValue(MakeUnversionedStringValue(str, 3));

    auto owningRow = builder.FinishRow();
    // Builder captures string, so this address is different from str.data().
    auto* strPtr = owningRow[3].Data.String;

    auto row = owningRow;
    auto rowBeginPtr = row.Begin();
    {
        auto keyBound = TKeyBound::FromRow(row, /* isInclusive */ false, /* isUpper */ false);
        EXPECT_EQ(row, keyBound.Prefix);
        EXPECT_EQ(rowBeginPtr, keyBound.Prefix.Begin());
    }
    {
        // Steal row.
        auto stolenKeyBound = TKeyBound::FromRow(std::move(row), /* isInclusive */ false, /* isUpper */ false);
        EXPECT_EQ(owningRow, stolenKeyBound.Prefix);
        EXPECT_EQ(rowBeginPtr, stolenKeyBound.Prefix.Begin());
    }
    {
        auto owningKeyBound = TOwningKeyBound::FromRow(owningRow, /* isInclusive */ false, /* isUpper */ false);
        EXPECT_EQ(owningRow, owningKeyBound.Prefix);
    }
    {
        // Steal owningRow.
        auto stolenOwningKeyBound = TOwningKeyBound::FromRow(std::move(owningRow), /* isInclusive */ false, /* isUpper */ false);
        EXPECT_EQ(EValueType::String, stolenOwningKeyBound.Prefix[3].Type);
        EXPECT_EQ(strPtr, stolenOwningKeyBound.Prefix[3].Data.String);
    }
}

TUnversionedOwningRow MakeRow(const std::vector<TUnversionedValue>& values)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& value : values) {
        builder.AddValue(value);
    }
    return builder.FinishRow();
}

TOwningKeyBound MakeKeyBound(const std::vector<TUnversionedValue>& values, bool isInclusive, bool isUpper)
{
    return TOwningKeyBound::FromRow(MakeRow(values), isInclusive, isUpper);
}

TEST(TKeyBoundTest, KeyBoundToLegacyRow)
{
    auto intValue = MakeUnversionedInt64Value(42);
    auto maxValue = MakeUnversionedSentinelValue(EValueType::Max);

    std::vector<TOwningKeyBound> keyBounds = {
        MakeKeyBound({intValue}, /* isInclusive */ false, /* isUpper */ false),
        MakeKeyBound({intValue}, /* isInclusive */ false, /* isUpper */ true),
        MakeKeyBound({intValue}, /* isInclusive */ true, /* isUpper */ false),
        MakeKeyBound({intValue}, /* isInclusive */ true, /* isUpper */ true),
    };

    auto expectedLegacyRows = {
        MakeRow({intValue, maxValue}),
        MakeRow({intValue}),
        MakeRow({intValue}),
        MakeRow({intValue, maxValue}),
    };

    for (const auto& [keyBound, legacyRow] : Zip(keyBounds, expectedLegacyRows)) {
        EXPECT_EQ(KeyBoundToLegacyRow(keyBound), legacyRow);
    }
}

TEST(TKeyBoundTest, KeyBoundFromLegacyRow)
{
    auto intValue1 = MakeUnversionedInt64Value(42);
    auto intValue2 = MakeUnversionedInt64Value(-7);
    auto intValue3 = MakeUnversionedInt64Value(0);
    auto maxValue = MakeUnversionedSentinelValue(EValueType::Max);
    auto minValue = MakeUnversionedSentinelValue(EValueType::Min);
    const int KeyLength = 2;

    // Refer to comment in KeyBoundFromLegacyRow for detailed explanation of possible cases.

    // (A)
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, intValue2, intValue3}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1, intValue2}, /* isInclusive */ false, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, intValue2, intValue3}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1, intValue2}, /* isInclusive */ true, /* isUpper */ true));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, intValue2, maxValue}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1, intValue2}, /* isInclusive */ false, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, intValue2, maxValue}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1, intValue2}, /* isInclusive */ true, /* isUpper */ true));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, intValue2, minValue}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1, intValue2}, /* isInclusive */ false, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, intValue2, minValue}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1, intValue2}, /* isInclusive */ true, /* isUpper */ true));

    // (B)
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, intValue2}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1, intValue2}, /* isInclusive */ true, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, intValue2}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1, intValue2}, /* isInclusive */ false, /* isUpper */ true));

    // (C)
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, minValue}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ true, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, minValue}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ false, /* isUpper */ true));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ true, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ false, /* isUpper */ true));

    // (C), arbitrary garbage after first sentinel does not change outcome.
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, minValue, minValue}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ true, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, minValue, minValue}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ false, /* isUpper */ true));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, minValue, maxValue}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ true, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, minValue, maxValue}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ false, /* isUpper */ true));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, minValue, intValue2}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ true, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, minValue, intValue2}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ false, /* isUpper */ true));

    // (D)
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, maxValue}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ false, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, maxValue}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ true, /* isUpper */ true));

    // (D), arbitrary garbage after first sentinel does not change outcome.
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, maxValue, minValue}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ false, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, maxValue, minValue}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ true, /* isUpper */ true));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, maxValue, maxValue}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ false, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, maxValue, maxValue}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ true, /* isUpper */ true));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, maxValue, intValue2}), /* isUpper */ false, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ false, /* isUpper */ false));
    EXPECT_EQ(
        KeyBoundFromLegacyRow(MakeRow({intValue1, maxValue, intValue2}), /* isUpper */ true, KeyLength),
        MakeKeyBound({intValue1}, /* isInclusive */ true, /* isUpper */ true));
}

TEST(TKeyBoundTest, StressNewAndLegacyEquivalence)
{
    auto intValue1 = MakeUnversionedInt64Value(42);
    auto intValue2 = MakeUnversionedInt64Value(-7);
    auto strValue1 = MakeUnversionedStringValue("foo");
    auto strValue2 = MakeUnversionedStringValue("bar");
    auto nullValue = MakeUnversionedNullValue();
    auto maxValue = MakeUnversionedSentinelValue(EValueType::Max);
    auto minValue = MakeUnversionedSentinelValue(EValueType::Min);
    const int KeyLength = 3;
    const TComparator Comparator(std::vector<ESortOrder>(KeyLength, ESortOrder::Ascending));

    std::vector<TUnversionedValue> allValues = {
        intValue1,
        intValue2,
        strValue1,
        strValue2,
        nullValue,
        maxValue,
        minValue,
    };

    std::vector<TUnversionedValue> noSentinelValues = {
        intValue1,
        intValue2,
        strValue1,
        strValue2,
        nullValue,
    };

    std::vector<TOwningKey> allKeys;
    for (const auto& value0 : noSentinelValues) {
        for (const auto& value1 : noSentinelValues) {
            for (const auto& value2 : noSentinelValues) {
                allKeys.emplace_back(TOwningKey::FromRow(MakeRow({value0, value1, value2})));
            }
        }
    }

    auto validateTestPreservation = [&] (const TKeyBound& keyBound, const TUnversionedRow& legacyRow) {
        bool isUpper = keyBound.IsUpper;
        for (const auto& key : allKeys) {
            auto legacyTest = isUpper ? key < legacyRow : key >= legacyRow;
            auto newTest = Comparator.TestKey(AsNonOwningKey(key), keyBound);

            if (legacyTest != newTest) {
                Cerr
                    << "Legacy row: " << ToString(legacyRow) << Endl
                    << "Key bound: " << ToString(keyBound) << Endl
                    << "Key: " << ToString(key.AsRow()) << Endl
                    << "LegacyTest: " << legacyTest << Endl
                    << "NewTest: " << newTest << Endl;
                // Somehow ASSERTs do not step execution in our gtest :(
                THROW_ERROR_EXCEPTION("Failure");
            }
        }
    };

    // Legacy -> New.
    // Check that all possible legacy bounds of length up to 5 produce
    // same test result as corresponding key bounds over all keys of length 3.

    std::vector<TUnversionedValue> currentValues;
    auto validateCurrentLegacyRow = [&] {
        for (bool isUpper : {false, true}) {
            auto legacyRow = MakeRow(currentValues);
            auto keyBound = KeyBoundFromLegacyRow(legacyRow, isUpper, KeyLength);
            validateTestPreservation(keyBound, legacyRow);
        }
    };

    // Test all possible legacy bounds of length up to 5.
    validateCurrentLegacyRow();
    for (const auto& value0 : allValues) {
        currentValues.push_back(value0);
        validateCurrentLegacyRow();
        for (const auto& value1 : allValues) {
            currentValues.push_back(value1);
            validateCurrentLegacyRow();
            for (const auto& value2 : allValues) {
                currentValues.push_back(value2);
                validateCurrentLegacyRow();
                for (const auto& value3 : allValues) {
                    currentValues.push_back(value3);
                    validateCurrentLegacyRow();
                    for (const auto& value4 : allValues) {
                        currentValues.push_back(value4);
                        validateCurrentLegacyRow();
                        currentValues.pop_back();
                    }
                    currentValues.pop_back();
                }
                currentValues.pop_back();
            }
            currentValues.pop_back();
        }
        currentValues.pop_back();
    }

    // New -> Legacy.
    // Check that all possible key bounds of length up to 3 produce
    // same test result as corresponding legacy bounds over all keys of length 3.

    auto validateCurrentKeyBound = [&] {
        for (bool isUpper : {false, true}) {
            for (bool isInclusive : {false, true}) {
                auto keyBound = MakeKeyBound(currentValues, isInclusive, isUpper);
                auto legacyRow = KeyBoundToLegacyRow(keyBound);
                validateTestPreservation(keyBound, legacyRow);
            }
        }
    };

    validateCurrentKeyBound();
    for (const auto& value0 : noSentinelValues) {
        currentValues.push_back(value0);
        validateCurrentKeyBound();
        for (const auto& value1 : noSentinelValues) {
            currentValues.push_back(value1);
            validateCurrentKeyBound();
            for (const auto& value2 : noSentinelValues) {
                currentValues.push_back(value2);
                validateCurrentKeyBound();
                currentValues.pop_back();
            }
            currentValues.pop_back();
        }
        currentValues.pop_back();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
