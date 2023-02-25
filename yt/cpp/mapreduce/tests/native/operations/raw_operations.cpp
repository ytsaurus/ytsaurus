#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/stream/file.h>
#include <util/string/strip.h>

using namespace NYT;
using namespace NYT::NTesting;

class TJsonKvSwapper
    : public IRawJob
{
public:
    void Do(const TRawJobContext& context) override
    {
        TUnbufferedFileInput inf(context.GetInputFile());
        TUnbufferedFileOutput outf(context.GetOutputFileList()[0]);

        TString line;
        while (inf.ReadLine(line)) {
            line = StripInPlace(line);
            if (line.empty()) {
                break;
            }
            NJson::TJsonValue val;
            Y_VERIFY(NJson::ReadJsonTree(line, &val));
            if (val.Has("$attributes")) {
                continue;
            }
            NJson::TJsonValue result;
            for (const auto& item : val.GetMap()) {
                result.InsertValue(item.second.GetString(), item.first);
            }
            NJson::WriteJson(&outf, &result);
        }
    }
};
REGISTER_RAW_JOB(TJsonKvSwapper);

class TJsonValueJoin
    : public IRawJob
{
public:
    void Do(const TRawJobContext& context) override
    {
        TUnbufferedFileInput inf(context.GetInputFile());
        TUnbufferedFileOutput outf(context.GetOutputFileList()[0]);

        bool exhausted = false;
        while (!exhausted) {
            TString key;
            TString value;
            while (true) {
                TString line;
                inf.ReadLine(line);
                line = StripInPlace(line);
                if (line.empty()) {
                    exhausted = true;
                    break;
                }
                NJson::TJsonValue val;
                Y_VERIFY(NJson::ReadJsonTree(line, &val));
                if (val.Has("$attributes")) {
                    if (val.GetMap().at("$attributes").Has("key_switch")) {
                        break;
                    } else {
                        continue;
                    }
                }

                key = val.GetMap().at("key").GetString();
                value += val.GetMap().at("value").GetString() + ";";
            }
            if (!key.empty()) {
                NJson::TJsonValue result;
                result.InsertValue("key", key);
                result.InsertValue("value", value);
                NJson::WriteJson(&outf, &result);
            }
        }
    }
};
REGISTER_RAW_JOB(TJsonValueJoin);

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(RawOperations)
{
    Y_UNIT_TEST(Map)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        {
            writer->AddRow(TNode()("one", "1")("two", "2"));
            writer->AddRow(TNode()("five", "5")("eight", "8"));
            writer->AddRow(TNode()("forty two", "42"));
            writer->Finish();
        }

        client->RawMap(
            TRawMapOperationSpec()
            .AddInput(workingDir + "/input")
            .AddOutput(workingDir + "/output")
            .Format(TFormat(TNode("json"))),
            new TJsonKvSwapper);

        TVector<TNode> actual = ReadTable(client, workingDir + "/output");

        const TVector<TNode> expected = {
            TNode()("1", "one")("2", "two"),
            TNode()("5", "five")("8", "eight"),
            TNode()("42", "forty two"),
        };
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(Reduce)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto writer = client->CreateTableWriter<TNode>(TRichYPath(workingDir + "/input").SortedBy({"key"}));
        {
            writer->AddRow(TNode()("key", "1")("value", "one"));
            writer->AddRow(TNode()("key", "1")("value", "two"));
            writer->AddRow(TNode()("key", "2")("value", "three"));
            writer->AddRow(TNode()("key", "3")("value", "four"));
            writer->AddRow(TNode()("key", "3")("value", "five"));
            writer->AddRow(TNode()("key", "3")("value", "six"));
            writer->Finish();
        }

        client->RawReduce(
            TRawReduceOperationSpec()
            .ReduceBy({"key"})
            .AddInput(workingDir + "/input")
            .AddOutput(workingDir + "/output")
            .Format(TFormat(TNode("json"))),
            new TJsonValueJoin);

        TVector<TNode> actual = ReadTable(client, workingDir + "/output");

        const TVector<TNode> expected = {
            TNode()("key", "1")("value", "one;two;"),
            TNode()("key", "2")("value", "three;"),
            TNode()("key", "3")("value", "four;five;six;"),
        };
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(MapReduce)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        {
            writer->AddRow(TNode()("1", "key")("one", "value"));
            writer->AddRow(TNode()("1", "key")("two", "value"));
            writer->AddRow(TNode()("2", "key")("three", "value"));
            writer->AddRow(TNode()("3", "key")("four", "value"));
            writer->AddRow(TNode()("3", "key")("five", "value"));
            writer->AddRow(TNode()("3", "key")("six", "value"));
            writer->Finish();
        }

        client->RawMapReduce(
            TRawMapReduceOperationSpec()
            .AddInput(workingDir + "/input")
            .AddOutput(workingDir + "/output")
            .SortBy("key")
            .MapperFormat(TFormat(TNode("json")))
            .ReducerFormat(TFormat(TNode("json"))),
            new TJsonKvSwapper,
            nullptr,
            new TJsonValueJoin);

        TVector<TNode> actual = ReadTable(client, workingDir + "/output");

        const TVector<TNode> expected = {
            TNode()("key", "1")("value", "one;two;"),
            TNode()("key", "2")("value", "three;"),
            TNode()("key", "3")("value", "four;five;six;"),
        };
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }
}

////////////////////////////////////////////////////////////////////////////////
