#include "lib.h"

#include <mapreduce/yt/tests/native_new/row.pb.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/http/error.h>

#include <library/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;


SIMPLE_UNIT_TEST_SUITE(TableReader) {
    SIMPLE_UNIT_TEST(Simple)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/table");
            writer->AddRow(TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TNode>("//testing/table");
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    SIMPLE_UNIT_TEST(NonEmptyColumns)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/table");
            writer->AddRow(TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TNode>(TRichYPath("//testing/table").Columns({"key1", "key3"}));
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode()("key1", "value1")("key3", "value3"));
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    SIMPLE_UNIT_TEST(EmptyColumns)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/table");
            writer->AddRow(TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TNode>(TRichYPath("//testing/table").Columns({}));
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode::CreateMap());
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    SIMPLE_UNIT_TEST(Move)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/table");
            writer->AddRow(TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
            writer->AddRow(TNode()("key1", "value4")("key2", "value5")("key3", "value6"));
            writer->Finish();
        }
        auto reader = client->CreateTableReader<TNode>(TRichYPath("//testing/table"));
        UNIT_ASSERT(reader->IsValid());

        UNIT_ASSERT_VALUES_EQUAL(reader->MoveRow(), TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
        UNIT_ASSERT_EXCEPTION(reader->MoveRow(), yexception);
        UNIT_ASSERT_EXCEPTION(reader->GetRow(), yexception);

        reader->Next();

        {
            TNode row;
            reader->MoveRow(&row);
            UNIT_ASSERT_VALUES_EQUAL(row, TNode()("key1", "value4")("key2", "value5")("key3", "value6"));
            UNIT_ASSERT_EXCEPTION(reader->MoveRow(), yexception);
            UNIT_ASSERT_EXCEPTION(reader->MoveRow(&row), yexception);
            UNIT_ASSERT_EXCEPTION(reader->GetRow(), yexception);
        }

        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    SIMPLE_UNIT_TEST(Protobuf)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/table");
            writer->AddRow(TNode()("Host", "http://www.example.com")("Path", "/")("HttpCode", 302));
            writer->AddRow(TNode()("Host", "http://www.example.com")("Path", "/index.php")("HttpCode", 200));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TUrlRow>("//testing/table");
        UNIT_ASSERT(reader->IsValid());
        {
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 302);
        }
        UNIT_ASSERT_NO_EXCEPTION(reader->GetRow());
        {
            TUrlRow row;
            reader->MoveRow(&row);
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 302);
        }
        UNIT_ASSERT_EXCEPTION(reader->GetRow(), yexception);
        {
            TUrlRow row;
            UNIT_ASSERT_EXCEPTION(reader->MoveRow(&row), yexception);
        }
        UNIT_ASSERT(reader->IsValid());

        reader->Next();
        UNIT_ASSERT(reader->IsValid());
        {
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/index.php");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 200);
        }
        UNIT_ASSERT_NO_EXCEPTION(reader->GetRow());
        {
            TUrlRow row;
            reader->MoveRow(&row);
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/index.php");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 200);
        }
        UNIT_ASSERT_EXCEPTION(reader->GetRow(), yexception);
        {
            TUrlRow row;
            UNIT_ASSERT_EXCEPTION(reader->MoveRow(&row), yexception);
        }
        UNIT_ASSERT(reader->IsValid());

        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    SIMPLE_UNIT_TEST(ErrorInTableWriter)
    {
        const TNode DATA = TString(1024, 'a');
        auto client = CreateTestClient();
        client->Create("//testing/table", NT_TABLE, TCreateOptions().Force(true).Attributes(
                TNode()("schema",
                    TNode()
                    .Add(TNode()("name", "value")("type", "string")))
                ));

        auto writer = client->CreateTableWriter<TNode>("//testing/table");
        auto writeTable = [&] {
            for (int i = 0; i != 100000; ++i) {
                writer->AddRow(TNode()("foo", 0)("value", DATA));
            }
            writer->AddRow(TNode()("bar", "qux"));
            for (int i = 0; i != 100000; ++i) {
                writer->AddRow(TNode()("foo", 0)("value", DATA));
            }
            writer->Finish();
        };
        UNIT_ASSERT_EXCEPTION(writeTable(), TErrorResponse);
    }

    SIMPLE_UNIT_TEST(ErrorInFinish)
    {
        auto client = CreateTestClient();
        client->Create("//testing/table", NT_TABLE, TCreateOptions().Force(true).Attributes(
                TNode()("schema",
                    TNode()
                    .Add(TNode()("name", "value")("type", "string")))
                ));

        auto writer = client->CreateTableWriter<TNode>("//testing/table");
        writer->AddRow(TNode()("bar", "qux"));
        UNIT_ASSERT_EXCEPTION(writer->Finish(), TErrorResponse);

        auto writeMore = [&] {
            writer->AddRow(TNode()("value", "a"));
            writer->Finish();
        };

        UNIT_ASSERT_EXCEPTION(writeMore(), TApiUsageError);
    }

    SIMPLE_UNIT_TEST(CantWriteAfterFinish)
    {
        auto client = CreateTestClient();
        auto writer = client->CreateTableWriter<TNode>("//testing/table");
        writer->AddRow(TNode()("value", "foo"));
        writer->Finish();
        UNIT_ASSERT_EXCEPTION(writer->AddRow(TNode()("value", "a")), TApiUsageError);
    }
}
