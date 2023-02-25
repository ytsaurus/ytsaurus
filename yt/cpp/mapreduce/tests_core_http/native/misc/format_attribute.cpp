#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/tests_core_http/lib/owning_yamr_row.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

TString NormalizeDsv(const TString& value)
{
    TVector<TString> splited = StringSplitter(value).Split('\t').ToList<TString>();
    Sort(splited.begin(), splited.end());
    return JoinRange("\t", splited.begin(), splited.end());
}

////////////////////////////////////////////////////////////////////////////////

class TSwapKvMapper
    : public IMapper<TTableReader<TYaMRRow>, TTableWriter<TYaMRRow>>
{
public:
    void Do(TTableReader<TYaMRRow>* reader, TTableWriter<TYaMRRow>* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->GetRow();
            TYaMRRow res;
            res.Key = row.Value;
            res.SubKey = row.SubKey;
            res.Value = row.Key;
            writer->AddRow(res);
        }
    }
};

REGISTER_MAPPER(TSwapKvMapper);

////////////////////////////////////////////////////////////////////////////////

void CreateYamredDsvInput(const IClientBasePtr& client, const TYPath& workingDir)
{
    auto writer = client->CreateTableWriter<TNode>(TRichYPath(workingDir + "/yamred_dsv_input"));
    writer->AddRow(TNode()("int", "1")("double", "1.0")("string", "one")("stringstring", "oneone"));
    writer->AddRow(TNode()("int", "2")("double", "2.0")("string", "two")("stringstring", "twotwo"));
    writer->Finish();

    TNode format("yamred_dsv");
    format.Attributes()
        ("key_column_names", TNode().Add("stringstring"))
        ("has_subkey", false);
    client->Set(workingDir + "/yamred_dsv_input/@_format", format);
};

void CreateYamrInput(const IClientBasePtr& client, const TYPath& workingDir)
{
    auto writer = client->CreateTableWriter<TNode>(TRichYPath(workingDir + "/yamr_input"));
    writer->AddRow(TNode()("key", "1")("value", "one"));
    writer->AddRow(TNode()("key", "2")("value", "two"));
    writer->Finish();
    client->Set(workingDir + "/yamr_input/@_format", "yamr");
}

Y_UNIT_TEST_SUITE(FormatAttribute)
{
    Y_UNIT_TEST(Read_YamredDsv)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        CreateYamredDsvInput(client, workingDir);

        TVector<TOwningYaMRRow> table;
        auto reader = client->CreateTableReader<TYaMRRow>(workingDir + "/yamred_dsv_input");
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->GetRow();
            table.emplace_back(ToString(row.Key), ToString(row.SubKey), NormalizeDsv(ToString(row.Value)));
        }

        const TVector<TOwningYaMRRow> expectedTable = {
            {"oneone", "", "double=1.0\tint=1\tstring=one"},
            {"twotwo", "", "double=2.0\tint=2\tstring=two"},
        };

        UNIT_ASSERT_VALUES_EQUAL(table, expectedTable);
    }

    Y_UNIT_TEST(Read_Yamr)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        CreateYamrInput(client, workingDir);

        TVector<TOwningYaMRRow> table;
        auto reader = client->CreateTableReader<TYaMRRow>(workingDir + "/yamr_input");
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->GetRow();
            table.emplace_back(ToString(row.Key), ToString(row.SubKey), NormalizeDsv(ToString(row.Value)));
        }

        const TVector<TOwningYaMRRow> expectedTable = {
            {"1", "", "one"},
            {"2", "", "two"},
        };

        UNIT_ASSERT_VALUES_EQUAL(table, expectedTable);
    }


    Y_UNIT_TEST(Operation_YamredDsv)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        CreateYamredDsvInput(client, workingDir);

        client->Map(
            TMapOperationSpec()
            .AddInput<TYaMRRow>(workingDir + "/yamred_dsv_input")
            .AddOutput<TYaMRRow>(workingDir + "/output"),
            new TSwapKvMapper,
            TOperationOptions().UseTableFormats(true));

        TVector<TOwningYaMRRow> table;

        auto reader = client->CreateTableReader<TYaMRRow>(workingDir + "/output");
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->GetRow();
            table.emplace_back(NormalizeDsv(ToString(row.Key)), ToString(row.SubKey), ToString(row.Value));
        }

        const TVector<TOwningYaMRRow> expectedTable = {
            {"double=1.0\tint=1\tstring=one", "", "oneone"},
            {"double=2.0\tint=2\tstring=two", "", "twotwo"},
        };

        UNIT_ASSERT_VALUES_EQUAL(table, expectedTable);
    }

    Y_UNIT_TEST(Operation_Yamr)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        CreateYamrInput(client, workingDir);

        client->Map(
            TMapOperationSpec()
            .AddInput<TYaMRRow>(workingDir + "/yamr_input")
            .AddOutput<TYaMRRow>(workingDir + "/output"),
            new TSwapKvMapper,
            TOperationOptions().UseTableFormats(true));

        TVector<TOwningYaMRRow> table;
        auto reader = client->CreateTableReader<TYaMRRow>(workingDir + "/output");
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->GetRow();
            table.emplace_back(ToString(row.Key), ToString(row.SubKey), NormalizeDsv(ToString(row.Value)));
        }

        const TVector<TOwningYaMRRow> expectedTable = {
            {"one", "", "1"},
            {"two", "", "2"},
        };

        UNIT_ASSERT_VALUES_EQUAL(table, expectedTable);
    }
}

////////////////////////////////////////////////////////////////////////////////
