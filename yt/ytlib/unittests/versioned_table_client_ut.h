#pragma once

#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/table_client/public.h>
#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/versioned_row.h>
#include <yt/client/table_client/versioned_reader.h>

namespace NYT {
namespace NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TVersionedTableClientTestBase
    : public ::testing::Test
{
protected:
    void ExpectRowsEqual(TUnversionedRow expected, TUnversionedRow actual)
    {
        if (!expected || !actual) {
            EXPECT_EQ(static_cast<bool>(expected), static_cast<bool>(actual))
                << "expected: " << ToString(expected)
                << ", "
                << "actual: " << ToString(actual);
        } else {
            EXPECT_EQ(0, CompareRows(expected.Begin(), expected.End(), actual.Begin(), actual.End()))
                << "expected: " << ToString(expected)
                << ", "
                << "actual: " << ToString(actual);
        }
    }

    void CheckResult(const std::vector<TVersionedRow>& expected, IVersionedReaderPtr reader)
    {
        auto it = expected.begin();
        std::vector<TVersionedRow> actual;
        actual.reserve(1000);

        while (reader->Read(&actual)) {
            if (actual.empty()) {
                EXPECT_TRUE(reader->GetReadyEvent().Get().IsOK());
                continue;
            }

            std::vector<TVersionedRow> ex(it, it + actual.size());

            CheckResult(ex, actual);
            it += actual.size();
        }

        EXPECT_TRUE(it == expected.end());
    }

    void CheckResult(const std::vector<TVersionedRow>& expected, const std::vector<TVersionedRow>& actual)
    {
        EXPECT_EQ(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); ++i) {
            ExpectRowsEqual(expected[i], actual[i]);
        }
    }

    void ExpectRowsEqual(TVersionedRow expected, TVersionedRow actual)
    {
        if (!expected) {
            EXPECT_FALSE(actual);
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
            EXPECT_EQ(CompareRowValues(expected.BeginValues()[i], actual.BeginValues()[i]), 0);
            EXPECT_EQ(expected.BeginValues()[i].Timestamp, actual.BeginValues()[i].Timestamp);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT

