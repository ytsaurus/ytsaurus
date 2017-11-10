#include "lib.h"

#include <mapreduce/yt/tests/native_new/all_types.pb.h>
#include <mapreduce/yt/tests/native_new/row.pb.h>

#include <mapreduce/yt/common/config.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/io.h>

#include <mapreduce/yt/http/abortable_http_response.h>

#include <library/unittest/registar.h>

#include <util/random/fast.h>

using namespace NYT;
using namespace NYT::NTesting;

SIMPLE_UNIT_TEST_SUITE(Schema) {
    SIMPLE_UNIT_TEST(Required)
    {
        auto client = CreateTestClient();

        client->Create("//testing/table", NT_TABLE,
            TCreateOptions().Attributes(
                TNode()
                ("schema", TTableSchema().AddColumn(TColumnSchema().Name("value").Type(VT_STRING).Required(true)).ToNode())
            ));

        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath("//testing/table").Append(true));
            writer->AddRow(TNode()("value", "foo"));
            writer->Finish();
        }
        try {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath("//testing/table").Append(true));
            writer->AddRow(TNode()("value", TNode::CreateEntity()));
            writer->Finish();
            UNIT_FAIL("expected to throw");
        } catch (const yexception& ex) {
            if (!TString(ex.what()).Contains("cannot have \"null\" value")) {
                throw;
            }
        }
        try {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath("//testing/table").Append(true));
            writer->AddRow(TNode::CreateMap());
            writer->Finish();
            UNIT_FAIL("expected to throw");
        } catch (const yexception& ex) {
            if (!TString(ex.what()).Contains("cannot have \"null\" value")) {
                throw;
            }
        }
    }

}
