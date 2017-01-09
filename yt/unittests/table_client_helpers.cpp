#include "table_client_helpers.h"

#include <yt/ytlib/table_client/versioned_reader.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

bool AreUnversionedValuesEqual(const TUnversionedValue& expected, const TUnversionedValue& actual)
{
    if (expected.Type != actual.Type) {
        return false;
    }

    if (expected.Id != actual.Id) {
        return false;
    }

    if (expected.Type != EValueType::Any) {
        return expected == actual;
    } else if (expected.Length != actual.Length) {
        return false;
    } else {
        return ::memcmp(expected.Data.String, actual.Data.String, actual.Length) == 0;
    }
}

void ExpectSchemafulRowsEqual(TUnversionedRow expected, TUnversionedRow actual)
{
    #define ADD_DIAGNOSTIC "expected: " << ToString(expected)    \
                           << ", "                               \
                           << "actual: " << ToString(actual)

    if (!expected) {
        EXPECT_FALSE(actual) << ADD_DIAGNOSTIC;
        return;
    }

    EXPECT_EQ(expected.GetCount(), actual.GetCount()) << ADD_DIAGNOSTIC;
    for (int valueIndex = 0; valueIndex < expected.GetCount(); ++valueIndex) {
        EXPECT_TRUE(AreUnversionedValuesEqual(expected[valueIndex], actual[valueIndex])) << ADD_DIAGNOSTIC;
    }

    #undef ADD_DIAGNOSTIC
}

void ExpectSchemalessRowsEqual(TUnversionedRow expected, TUnversionedRow actual, int keyColumnCount)
{
#define ADD_DIAGNOSTIC "expected: " << ToString(expected)    \
                       << ", "                               \
                       << "actual: " << ToString(actual)

    if (!expected) {
        EXPECT_FALSE(actual) << ADD_DIAGNOSTIC;
        return;
    }

    ASSERT_TRUE(actual) << ADD_DIAGNOSTIC;
    EXPECT_EQ(expected.GetCount(), actual.GetCount()) << ADD_DIAGNOSTIC;
    for (int valueIndex = 0; valueIndex < keyColumnCount; ++valueIndex) {
        EXPECT_TRUE(AreUnversionedValuesEqual(expected[valueIndex], actual[valueIndex])) << ADD_DIAGNOSTIC;
    }

    for (int valueIndex = keyColumnCount; valueIndex < expected.GetCount(); ++valueIndex) {
        // Find value with the same id. Since this in schemaless read, value positions can be different.
        bool found = false;
        for (int index = keyColumnCount; index < expected.GetCount(); ++index) {
            if (expected[valueIndex].Id == actual[index].Id) {
                EXPECT_TRUE(AreUnversionedValuesEqual(expected[valueIndex], actual[index])) << ADD_DIAGNOSTIC;
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << ADD_DIAGNOSTIC << "Value index " << valueIndex;
    }

#undef ADD_DIAGNOSTIC
}

void ExpectSchemafulRowsEqual(TVersionedRow expected, TVersionedRow actual)
{
    if (!expected) {
        EXPECT_FALSE(actual);
        YCHECK(!actual);
        return;
    }

    EXPECT_EQ(0, CompareRows(expected.BeginKeys(), expected.EndKeys(), actual.BeginKeys(), actual.EndKeys()));

    EXPECT_EQ(expected.GetWriteTimestampCount(), actual.GetWriteTimestampCount());
    for (int i = 0; i < expected.GetWriteTimestampCount(); ++i) {
        EXPECT_EQ(expected.BeginWriteTimestamps()[i], actual.BeginWriteTimestamps()[i]);
    }

    EXPECT_EQ(expected.GetDeleteTimestampCount(), actual.GetDeleteTimestampCount());
    for (int i = 0; i < expected.GetDeleteTimestampCount(); ++i) {
        EXPECT_EQ(expected.BeginDeleteTimestamps()[i], actual.BeginDeleteTimestamps()[i]);
    }

    EXPECT_EQ(expected.GetValueCount(), actual.GetValueCount());
    for (int i = 0; i < expected.GetValueCount(); ++i) {
        EXPECT_TRUE(AreUnversionedValuesEqual(
            expected.BeginValues()[i],
            actual.BeginValues()[i]));
        EXPECT_EQ(expected.BeginValues()[i].Timestamp, actual.BeginValues()[i].Timestamp);
    }
}

void CheckResult(std::vector<TVersionedRow>* expected, IVersionedReaderPtr reader)
{
    expected->erase(
        std::remove_if(
            expected->begin(),
            expected->end(),
            [] (TVersionedRow row) {
                return !row;
            }),
        expected->end());

    auto it = expected->begin();
    std::vector<TVersionedRow> actual;
    actual.reserve(1000);

    while (reader->Read(&actual)) {
        if (actual.empty()) {
            EXPECT_TRUE(reader->GetReadyEvent().Get().IsOK());
            continue;
        }

        actual.erase(
            std::remove_if(
                actual.begin(),
                actual.end(),
                [] (TVersionedRow row) {
                    return !row;
                }),
            actual.end());

        std::vector<TVersionedRow> ex(it, it + actual.size());

        CheckSchemafulResult(ex, actual);
        it += actual.size();
    }

    EXPECT_TRUE(it == expected->end());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
