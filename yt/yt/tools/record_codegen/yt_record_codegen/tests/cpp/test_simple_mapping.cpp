#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/tools/record_codegen/yt_record_codegen/tests/records/simple_mapping.record.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NTableClient::NTest {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSimpleMappingTest, CheckConversion)
{
    std::array records{
        NRecords::TSimpleMapping{
            .Key = {
                .KeyA = "foo",
                .KeyB = "bar",
                .KeyC = NYson::TYsonString(TString("[foo;bar]")),
            },
            .ValueA = "baz",
            .ValueB = "qux",
            .ValueC = NYson::TYsonString(TString("{baz=qux}")),
        },
    };

    auto rows = FromRecords(TRange(records));
    auto recordsCopy = ToRecords<NRecords::TSimpleMapping>(
        rows,
        /*idMapping*/ NRecords::TSimpleMapping::TRecordDescriptor::Get()->GetPartialIdMapping());

    ASSERT_EQ(recordsCopy.size(), records.size());
    EXPECT_EQ(recordsCopy[0], records[0]);

    auto optionalRecords = ToRecords<NRecords::TSimpleMappingPartial>(
        rows,
        /*idMapping*/ NRecords::TSimpleMapping::TRecordDescriptor::Get()->GetPartialIdMapping());

    auto optionalRecordsCopy = ToOptionalRecords<NRecords::TSimpleMappingPartial>(
        FromRecords(TRange(optionalRecords)),
        /*idMapping*/ NRecords::TSimpleMapping::TRecordDescriptor::Get()->GetPartialIdMapping());

    ASSERT_EQ(optionalRecordsCopy.size(), optionalRecords.size());
    EXPECT_EQ(optionalRecordsCopy[0], optionalRecords[0]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
}  // namespace NYT::NTableClient::NTest
