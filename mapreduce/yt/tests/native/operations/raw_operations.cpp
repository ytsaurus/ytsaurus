#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/interface/client.h>

#include <library/unittest/registar.h>
#include <library/json/json_reader.h>
#include <library/json/json_value.h>
#include <library/json/json_writer.h>

#include <util/stream/file.h>
#include <util/string/strip.h>

using namespace NYT;
using namespace NYT::NTesting;

class TJsonKvSwapper
    : public IRawJob
{
public:
    virtual void Do(const TRawJobContext& context) override
    {
        TUnbufferedFileInput inf(context.GetInputFile());
        TUnbufferedFileOutput outf(context.GetOutputFileList()[0]);

        TString line;
        while (inf.ReadLine(line)) {
            line = Strip(line);
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
    virtual void Do(const TRawJobContext& context) override
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
                line = Strip(line);
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
        auto client = CreateTestClient();

        auto writer = client->CreateTableWriter<TNode>("//testing/input");
        {
            writer->AddRow(TNode()("one", "1")("two", "2"));
            writer->AddRow(TNode()("five", "5")("eight", "8"));
            writer->AddRow(TNode()("forty two", "42"));
            writer->Finish();
        }

        client->RawMap(
            TRawMapOperationSpec()
            .AddInput("//testing/input")
            .AddOutput("//testing/output")
            .Format(TFormat(EFormatType::Custom, TNode("json"))),
            new TJsonKvSwapper);

        TVector<TNode> actual = ReadTable(client, "//testing/output");

        const TVector<TNode> expected = {
            TNode()("1", "one")("2", "two"),
            TNode()("5", "five")("8", "eight"),
            TNode()("42", "forty two"),
        };
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(Reduce)
    {
        auto client = CreateTestClient();

        auto writer = client->CreateTableWriter<TNode>(TRichYPath("//testing/input").SortedBy({"key"}));
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
            .AddInput("//testing/input")
            .AddOutput("//testing/output")
            .Format(TFormat(EFormatType::Custom, TNode("json"))),
            new TJsonValueJoin);

        TVector<TNode> actual = ReadTable(client, "//testing/output");

        const TVector<TNode> expected = {
            TNode()("key", "1")("value", "one;two;"),
            TNode()("key", "2")("value", "three;"),
            TNode()("key", "3")("value", "four;five;six;"),
        };
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(MapReduce)
    {
        auto client = CreateTestClient();

        auto writer = client->CreateTableWriter<TNode>("//testing/input");
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
            .AddInput("//testing/input")
            .AddOutput("//testing/output")
            .SortBy("key")
            .MapperFormat(TFormat(EFormatType::Custom, TNode("json")))
            .ReducerFormat(TFormat(EFormatType::Custom, TNode("json"))),
            new TJsonKvSwapper,
            nullptr,
            new TJsonValueJoin);

        TVector<TNode> actual = ReadTable(client, "//testing/output");

        const TVector<TNode> expected = {
            TNode()("key", "1")("value", "one;two;"),
            TNode()("key", "2")("value", "three;"),
            TNode()("key", "3")("value", "four;five;six;"),
        };
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }
}

////////////////////////////////////////////////////////////////////////////////
