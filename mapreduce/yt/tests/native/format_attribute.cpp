#include <mapreduce/yt/tests/lib/lib.h>
#include <mapreduce/yt/tests/lib/owning_yamr_row.h>

#include <mapreduce/yt/interface/client.h>

#include <util/string/join.h>
#include <util/string/iterator.h>

namespace NYT {
namespace NNativeTest {

using namespace NTest;

////////////////////////////////////////////////////////////////////////////////

TString NormalizeDsv(const TString& value)
{
    yvector<TString> splited = StringSplitter(value).Split('\t').ToList<TString>();
    Sort(splited.begin(), splited.end());
    return JoinRange("\t", splited.begin(), splited.end());
}

////////////////////////////////////////////////////////////////////////////////

class TSwapKvMapper : public IMapper<TTableReader<TYaMRRow>, TTableWriter<TYaMRRow>>
{
    public:
        virtual void Do(TTableReader<TYaMRRow>* reader, TTableWriter<TYaMRRow>* writer) override
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

class TFormatAttribute
    : public NTest::TTest
{
public:
    void SetUp() override
    {
        TTest::SetUp();
        Client_ = CreateClient(ServerName());

        Client()->Remove("home/testing", TRemoveOptions().Force(true).Recursive(true));
        Client()->Create("home/testing", NYT::ENodeType::NT_MAP);

        {
            auto writer = Client()->CreateTableWriter<TNode>(TRichYPath("home/testing/yamred_dsv_input"));
            writer->AddRow(TNode()("int", "1")("double", "1.0")("string", "one")("stringstring", "oneone"));
            writer->AddRow(TNode()("int", "2")("double", "2.0")("string", "two")("stringstring", "twotwo"));
            writer->Finish();

            TNode format("yamred_dsv");
            format.Attributes()
                ("key_column_names", TNode().Add("stringstring"))
                ("has_subkey", false);
            Client()->Set("home/testing/yamred_dsv_input/@_format", format);
        }

        {
            auto writer = Client()->CreateTableWriter<TNode>(TRichYPath("home/testing/yamr_input"));
            writer->AddRow(TNode()("key", "1")("value", "one"));
            writer->AddRow(TNode()("key", "2")("value", "two"));
            writer->Finish();
            Client()->Set("home/testing/yamr_input/@_format", "yamr");
        }
    }

    void TearDown() override
    {
        TTest::TearDown();
    }

    IClientPtr Client() {
        return Client_;
    }

private:
    IClientPtr Client_;
};

YT_TEST(TFormatAttribute, Read_YamredDsv)
{
    yvector<TOwningYaMRRow> table;

    auto reader = Client()->CreateTableReader<TYaMRRow>("home/testing/yamred_dsv_input");
    for (; reader->IsValid(); reader->Next()) {
        auto row = reader->GetRow();
        table.emplace_back(row.Key.ToString(), row.SubKey.ToString(), NormalizeDsv(row.Value.ToString()));
    }

    const yvector<TOwningYaMRRow> expectedTable = {
        {"oneone", "", "double=1.0\tint=1\tstring=one"},
        {"twotwo", "", "double=2.0\tint=2\tstring=two"},
    };

    ASSERT_EQ(table, expectedTable);
}

YT_TEST(TFormatAttribute, Read_Yamr)
{
    yvector<TOwningYaMRRow> table;
    auto reader = Client()->CreateTableReader<TYaMRRow>("home/testing/yamr_input");
    for (; reader->IsValid(); reader->Next()) {
        auto row = reader->GetRow();
        table.emplace_back(row.Key.ToString(), row.SubKey.ToString(), NormalizeDsv(row.Value.ToString()));
    }

    const yvector<TOwningYaMRRow> expectedTable = {
        {"1", "", "one"},
        {"2", "", "two"},
    };

    ASSERT_EQ(table, expectedTable);
}


YT_TEST(TFormatAttribute, Operation_YamredDsv)
{
    Client()->Map(
        TMapOperationSpec()
            .AddInput<TYaMRRow>("home/testing/yamred_dsv_input")
            .AddOutput<TYaMRRow>("home/testing/output"),
        new TSwapKvMapper,
        TOperationOptions().UseTableFormats(true));

    yvector<TOwningYaMRRow> table;

    auto reader = Client()->CreateTableReader<TYaMRRow>("home/testing/output");
    for (; reader->IsValid(); reader->Next()) {
        auto row = reader->GetRow();
        table.emplace_back(NormalizeDsv(row.Key.ToString()), row.SubKey.ToString(), row.Value.ToString());
    }

    const yvector<TOwningYaMRRow> expectedTable = {
        {"double=1.0\tint=1\tstring=one", "", "oneone"},
        {"double=2.0\tint=2\tstring=two", "", "twotwo"},
    };

    ASSERT_EQ(table, expectedTable);
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TFormatAttribute, Operation_Yamr)
{
    Client()->Map(
        TMapOperationSpec()
            .AddInput<TYaMRRow>("home/testing/yamr_input")
            .AddOutput<TYaMRRow>("home/testing/output"),
        new TSwapKvMapper,
        TOperationOptions().UseTableFormats(true));

    yvector<TOwningYaMRRow> table;
    auto reader = Client()->CreateTableReader<TYaMRRow>("home/testing/output");
    for (; reader->IsValid(); reader->Next()) {
        auto row = reader->GetRow();
        table.emplace_back(row.Key.ToString(), row.SubKey.ToString(), NormalizeDsv(row.Value.ToString()));
    }

    const yvector<TOwningYaMRRow> expectedTable = {
        {"one", "", "1"},
        {"two", "", "2"},
    };

    ASSERT_EQ(table, expectedTable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTest
} // namespace NYT
