#include "table_client_helpers.h"

#include <yt/client/table_client/versioned_reader.h>
#include <yt/client/api/rpc_proxy/api_service_proxy.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void CheckEqualNoTrace(const TUnversionedValue& expected, const TUnversionedValue& actual)
{
    EXPECT_EQ(expected.Id, actual.Id);
    ASSERT_EQ(expected.Type, actual.Type);
    ASSERT_EQ(expected.Aggregate, expected.Aggregate);

    if (IsStringLikeType(expected.Type)) {
        ASSERT_EQ(expected.Length, actual.Length);
        EXPECT_EQ(0, ::memcmp(expected.Data.String, actual.Data.String, expected.Length));
    } else if (IsValueType(expected.Type)) {
        EXPECT_EQ(expected.Data.Uint64, actual.Data.Uint64);
    }
}

void CheckEqual(const TUnversionedValue& expected, const TUnversionedValue& actual)
{
    SCOPED_TRACE(Format("Expected: %v; Actual: %v", expected, actual));
    CheckEqualNoTrace(expected, actual);
}

void CheckEqual(const TVersionedValue& expected, const TVersionedValue& actual)
{
    SCOPED_TRACE(Format("Expected: %v; Actual: %v", expected, actual));
    CheckEqualNoTrace(
        static_cast<const TUnversionedValue&>(expected),
        static_cast<const TUnversionedValue&>(actual));
    EXPECT_EQ(expected.Timestamp, expected.Timestamp);
}

void ExpectSchemafulRowsEqual(TUnversionedRow expected, TUnversionedRow actual)
{
    SCOPED_TRACE(Format("Expected: %v; Actual: %v", expected, actual));

    ASSERT_EQ(static_cast<bool>(expected), static_cast<bool>(actual));
    if (!expected || !actual) {
        return;
    }
    ASSERT_EQ(expected.GetCount(), actual.GetCount());

    for (int valueIndex = 0; valueIndex < expected.GetCount(); ++valueIndex) {
        SCOPED_TRACE(Format("Value index %v", valueIndex));
        CheckEqual(expected[valueIndex], actual[valueIndex]);
    }
}

void ExpectSchemalessRowsEqual(TUnversionedRow expected, TUnversionedRow actual, int keyColumnCount)
{
    SCOPED_TRACE(Format("Expected: %v; Actual: %v", expected, actual));

    ASSERT_EQ(static_cast<bool>(expected), static_cast<bool>(actual));
    if (!expected || !actual) {
        return;
    }
    ASSERT_EQ(expected.GetCount(), actual.GetCount());

    for (int valueIndex = 0; valueIndex < keyColumnCount; ++valueIndex) {
        SCOPED_TRACE(Format("Value index %v", valueIndex));
        CheckEqual(expected[valueIndex], actual[valueIndex]);
    }

    for (int valueIndex = keyColumnCount; valueIndex < expected.GetCount(); ++valueIndex) {
        SCOPED_TRACE(Format("Value index %v", valueIndex));

        // Find value with the same id. Since this in schemaless read, value positions can be different.
        bool found = false;
        for (int index = keyColumnCount; index < expected.GetCount(); ++index) {
            if (expected[valueIndex].Id == actual[index].Id) {
                CheckEqual(expected[valueIndex], actual[index]);
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    }
}

void ExpectSchemafulRowsEqual(TVersionedRow expected, TVersionedRow actual)
{
    SCOPED_TRACE(Format("Expected: %v; Actual: %v", expected, actual));

    ASSERT_EQ(static_cast<bool>(expected), static_cast<bool>(actual));
    if (!expected || !actual) {
        return;
    }

    ASSERT_EQ(expected.GetWriteTimestampCount(), actual.GetWriteTimestampCount());
    for (int i = 0; i < expected.GetWriteTimestampCount(); ++i) {
        SCOPED_TRACE(Format("Write Timestamp %v", i));
        EXPECT_EQ(expected.BeginWriteTimestamps()[i], actual.BeginWriteTimestamps()[i]);
    }

    ASSERT_EQ(expected.GetDeleteTimestampCount(), actual.GetDeleteTimestampCount());
    for (int i = 0; i < expected.GetDeleteTimestampCount(); ++i) {
        SCOPED_TRACE(Format("Delete Timestamp %v", i));
        EXPECT_EQ(expected.BeginDeleteTimestamps()[i], actual.BeginDeleteTimestamps()[i]);
    }

    ASSERT_EQ(expected.GetKeyCount(), actual.GetKeyCount());
    for (int index = 0; index < expected.GetKeyCount(); ++index) {
        SCOPED_TRACE(Format("Key index %v", index));
        CheckEqual(expected.BeginKeys()[index], actual.BeginKeys()[index]);
    }

    ASSERT_EQ(expected.GetValueCount(), actual.GetValueCount());
    for (int index = 0; index < expected.GetValueCount(); ++index) {
        SCOPED_TRACE(Format("Value index %v", index));
        CheckEqual(expected.BeginValues()[index], actual.BeginValues()[index]);
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

TEST(TEnvelopeTest, CrashExample)
{
    unsigned char badString[] = {
        0x4f, 0x6d, 0x61, 0x78, 0x00, 0x00, 0x01, 0x00,
        0x52, 0xd5, 0xe9, 0xe6, 0x43, 0x55, 0xee, 0xa7,
        0x41, 0xeb, 0xa5, 0x88, 0x2e, 0x02, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
    };

    NApi::NRpcProxy::NProto::TReqPingTransaction ping;
    EXPECT_FALSE(TryDeserializeProtoWithEnvelope(&ping, TRef{badString, sizeof(badString)}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
