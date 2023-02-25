#include <yt/cpp/mapreduce/interface/client.h>
#include <util/system/env.h>

using namespace NYT;

class TTestException : public yexception
{ };

int main(int argc, const char** argv) {
    NYT::Initialize(argc, argv);

    auto client = CreateClient(GetEnv("YT_PROXY"));
    bool throwException = ::FromString<bool>(GetEnv("THROW_EXCEPTION"));

    client->Create("//tmp/schemaful_table", NT_TABLE,
        TCreateOptions()
            .Force(true)
            .Attributes(
                TNode()(
                    "schema",
                    TTableSchema().AddColumn("foo", VT_INT64).ToNode())));

    try {
        auto writer = client->CreateTableWriter<TNode>(TRichYPath("//tmp/schemaful_table").Append(true));

        // Cannot write such value, but we don't get error immediately.
        writer->AddRow(TNode()("foo", "bar"));

        if (throwException) {
            ythrow TTestException();
        }
    } catch (const TTestException&) {
        // Do nothing.
    }

    return 0;
}

