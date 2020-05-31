#pragma once

#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/unversioned_row_batch.h>
#include <yt/client/table_client/versioned_row.h>

#include <iostream>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void ExpectSchemafulRowsEqual(TUnversionedRow expected, TUnversionedRow actual);

void ExpectSchemalessRowsEqual(TUnversionedRow expected, TUnversionedRow actual, int keyColumnCount);

void ExpectSchemafulRowsEqual(TVersionedRow expected, TVersionedRow actual);

void CheckResult(std::vector<TVersionedRow>* expected, IVersionedReaderPtr reader);

template <class TExpectedRow, class TActualRow>
void CheckSchemafulResult(const std::vector<TExpectedRow>& expected, const std::vector<TActualRow>& actual)
{
    ASSERT_EQ(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); ++i) {
        ExpectSchemafulRowsEqual(expected[i], actual[i]);
    }
}

template <class TExpectedRow, class TActualRow>
void CheckSchemalessResult(
    TRange<TExpectedRow> expected,
    TRange<TActualRow> actual,
    int keyColumnCount)
{
    ASSERT_EQ(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); ++i) {
        ExpectSchemalessRowsEqual(expected[i], actual[i], keyColumnCount);
    }
}

template <class TRow, class TReader>
void CheckSchemalessResult(const std::vector<TRow>& expected, TIntrusivePtr<TReader> reader, int keyColumnCount)
{
    size_t offset = 0;
    while (auto batch = reader->Read()) {
        auto actual = batch->MaterializeRows();
        if (actual.empty()) {
            ASSERT_TRUE(reader->GetReadyEvent().Get().IsOK());
            continue;
        }

        CheckSchemalessResult(
            MakeRange(expected).Slice(offset, std::min(expected.size(), offset + actual.size())),
            actual,
            keyColumnCount);
        offset += actual.size();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

