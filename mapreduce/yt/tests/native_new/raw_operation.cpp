#include "lib.h"

#include <mapreduce/yt/interface/client.h>

#include <library/unittest/registar.h>
#include <library/json/json_reader.h>
#include <library/json/json_value.h>
#include <library/json/json_writer.h>

#include <util/stream/file.h>
#include <util/string/strip.h>

using namespace NYT;
using namespace NYT::NTesting;

class TJsonKvSwapper : public IRawJob
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


////////////////////////////////////////////////////////////////////////////////

SIMPLE_UNIT_TEST_SUITE(RawOperations)
{
    SIMPLE_UNIT_TEST(RawMapper)
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

        TVector<TNode> actual;
        {
            // TODO: factorize this code
            auto reader = client->CreateTableReader<TNode>("//testing/output");
            for (; reader->IsValid(); reader->Next()) {
                actual.emplace_back(reader->GetRow());
            }
        }

        const TVector<TNode> expected = {
            TNode()("1", "one")("2", "two"),
            TNode()("5", "five")("8", "eight"),
            TNode()("42", "forty two"),
        };
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }
}

////////////////////////////////////////////////////////////////////////////////
