#include "stdafx.h"
#include "framework.h"

#include <core/yson/writer.h>
#include <core/yson/parser.h>
#include <core/ytree/yson_consumer-mock.h>
#include <core/ytree/yson_stream.h>

#include <util/string/escape.h>

namespace NYT {
namespace NYson {
namespace {

using ::testing::InSequence;
using ::testing::StrictMock;

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TYsonWriterTest: public ::testing::Test
{
public:
    TStringStream Stream;
    StrictMock<TMockYsonConsumer> Mock;

    void Run()
    {
        Stream.Flush();
        ParseYson(TYsonInput(&Stream), &Mock);
    }
};

////////////////////////////////////////////////////////////////////////////////

#define TEST_SCALAR(value, type) \
    { \
        InSequence dummy; \
        EXPECT_CALL(Mock, On##type##Scalar(value)); \
        TYsonWriter writer(&Stream, EYsonFormat::Binary); \
        writer.On##type##Scalar(value); \
        Run(); \
    } \
        \
    { \
        InSequence dummy; \
        EXPECT_CALL(Mock, On##type##Scalar(value)); \
        TYsonWriter writer(&Stream, EYsonFormat::Text); \
        writer.On##type##Scalar(value); \
        Run(); \
    } \


TEST_F(TYsonWriterTest, String)
{
    Stroka value = "YSON";
    TEST_SCALAR(value, String)
}

TEST_F(TYsonWriterTest, Int64)
{
    i64 value = 100500424242ll;
    TEST_SCALAR(value, Int64)
}

TEST_F(TYsonWriterTest, Uint64)
{
    ui64 value = 100500424242llu;
    TEST_SCALAR(value, Uint64)
}

TEST_F(TYsonWriterTest, Boolean)
{
    bool value = true;
    TEST_SCALAR(value, Boolean)
}

TEST_F(TYsonWriterTest, EmptyMap)
{

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    TYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnBeginMap();
    writer.OnEndMap();

    Run();
}

TEST_F(TYsonWriterTest, OneItemMap)
{

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("hello"));
    EXPECT_CALL(Mock, OnStringScalar("world"));
    EXPECT_CALL(Mock, OnEndMap());

    TYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnBeginMap();
    writer.OnKeyedItem("hello");
    writer.OnStringScalar("world");
    writer.OnEndMap();

    Run();
}

TEST_F(TYsonWriterTest, MapWithAttributes)
{
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
    EXPECT_CALL(Mock, OnKeyedItem("acl"));
        EXPECT_CALL(Mock, OnBeginMap());
            EXPECT_CALL(Mock, OnKeyedItem("read"));
            EXPECT_CALL(Mock, OnBeginList());
                EXPECT_CALL(Mock, OnListItem());
                EXPECT_CALL(Mock, OnStringScalar("*"));
            EXPECT_CALL(Mock, OnEndList());

            EXPECT_CALL(Mock, OnKeyedItem("write"));
            EXPECT_CALL(Mock, OnBeginList());
                EXPECT_CALL(Mock, OnListItem());
                EXPECT_CALL(Mock, OnStringScalar("sandello"));
            EXPECT_CALL(Mock, OnEndList());
        EXPECT_CALL(Mock, OnEndMap());

        EXPECT_CALL(Mock, OnKeyedItem("lock_scope"));
        EXPECT_CALL(Mock, OnStringScalar("mytables"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("path"));
        EXPECT_CALL(Mock, OnStringScalar("/home/sandello"));

        EXPECT_CALL(Mock, OnKeyedItem("mode"));
        EXPECT_CALL(Mock, OnInt64Scalar(755));
    EXPECT_CALL(Mock, OnEndMap());

    TYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnBeginAttributes();
        writer.OnKeyedItem("acl");
        writer.OnBeginMap();
            writer.OnKeyedItem("read");
            writer.OnBeginList();
                writer.OnListItem();
                writer.OnStringScalar("*");
            writer.OnEndList();

            writer.OnKeyedItem("write");
            writer.OnBeginList();
                writer.OnListItem();
                writer.OnStringScalar("sandello");
            writer.OnEndList();
        writer.OnEndMap();

        writer.OnKeyedItem("lock_scope");
        writer.OnStringScalar("mytables");
    writer.OnEndAttributes();

    writer.OnBeginMap();
        writer.OnKeyedItem("path");
        writer.OnStringScalar("/home/sandello");

        writer.OnKeyedItem("mode");
        writer.OnInt64Scalar(755);
    writer.OnEndMap();

    Run();
}

TEST_F(TYsonWriterTest, Escaping)
{
    TStringStream outputStream;
    TYsonWriter writer(&outputStream, EYsonFormat::Text);

    Stroka input;
    for (int i = 0; i < 256; ++i) {
        input.push_back(char(i));
    }

    writer.OnStringScalar(input);

    Stroka output =
        "\"\\0\\1\\2\\3\\4\\5\\6\\7\\x08\\t\\n\\x0B\\x0C\\r\\x0E\\x0F"
        "\\x10\\x11\\x12\\x13\\x14\\x15\\x16\\x17\\x18\\x19\\x1A\\x1B"
        "\\x1C\\x1D\\x1E\\x1F !\\\"#$%&'()*+,-./0123456789:;<=>?@ABCD"
        "EFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
        "\\x7F\\x80\\x81\\x82\\x83\\x84\\x85\\x86\\x87\\x88\\x89\\x8A"
        "\\x8B\\x8C\\x8D\\x8E\\x8F\\x90\\x91\\x92\\x93\\x94\\x95\\x96"
        "\\x97\\x98\\x99\\x9A\\x9B\\x9C\\x9D\\x9E\\x9F\\xA0\\xA1\\xA2"
        "\\xA3\\xA4\\xA5\\xA6\\xA7\\xA8\\xA9\\xAA\\xAB\\xAC\\xAD\\xAE"
        "\\xAF\\xB0\\xB1\\xB2\\xB3\\xB4\\xB5\\xB6\\xB7\\xB8\\xB9\\xBA"
        "\\xBB\\xBC\\xBD\\xBE\\xBF\\xC0\\xC1\\xC2\\xC3\\xC4\\xC5\\xC6"
        "\\xC7\\xC8\\xC9\\xCA\\xCB\\xCC\\xCD\\xCE\\xCF\\xD0\\xD1\\xD2"
        "\\xD3\\xD4\\xD5\\xD6\\xD7\\xD8\\xD9\\xDA\\xDB\\xDC\\xDD\\xDE"
        "\\xDF\\xE0\\xE1\\xE2\\xE3\\xE4\\xE5\\xE6\\xE7\\xE8\\xE9\\xEA"
        "\\xEB\\xEC\\xED\\xEE\\xEF\\xF0\\xF1\\xF2\\xF3\\xF4\\xF5\\xF6"
        "\\xF7\\xF8\\xF9\\xFA\\xFB\\xFC\\xFD\\xFE\\xFF\"";

    EXPECT_EQ(outputStream.Str(), output);
}

TEST_F(TYsonWriterTest, ConvertToYson)
{
    TStringStream outputStream;
    TYsonWriter writer(&outputStream, EYsonFormat::Text);

    Stroka input;
    for (int i = 0; i < 256; ++i) {
        input.push_back(char(i));
    }

    writer.OnStringScalar(input);

    Stroka output =
        "\"\\0\\1\\2\\3\\4\\5\\6\\7\\x08\\t\\n\\x0B\\x0C\\r\\x0E\\x0F"
        "\\x10\\x11\\x12\\x13\\x14\\x15\\x16\\x17\\x18\\x19\\x1A\\x1B"
        "\\x1C\\x1D\\x1E\\x1F !\\\"#$%&'()*+,-./0123456789:;<=>?@ABCD"
        "EFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
        "\\x7F\\x80\\x81\\x82\\x83\\x84\\x85\\x86\\x87\\x88\\x89\\x8A"
        "\\x8B\\x8C\\x8D\\x8E\\x8F\\x90\\x91\\x92\\x93\\x94\\x95\\x96"
        "\\x97\\x98\\x99\\x9A\\x9B\\x9C\\x9D\\x9E\\x9F\\xA0\\xA1\\xA2"
        "\\xA3\\xA4\\xA5\\xA6\\xA7\\xA8\\xA9\\xAA\\xAB\\xAC\\xAD\\xAE"
        "\\xAF\\xB0\\xB1\\xB2\\xB3\\xB4\\xB5\\xB6\\xB7\\xB8\\xB9\\xBA"
        "\\xBB\\xBC\\xBD\\xBE\\xBF\\xC0\\xC1\\xC2\\xC3\\xC4\\xC5\\xC6"
        "\\xC7\\xC8\\xC9\\xCA\\xCB\\xCC\\xCD\\xCE\\xCF\\xD0\\xD1\\xD2"
        "\\xD3\\xD4\\xD5\\xD6\\xD7\\xD8\\xD9\\xDA\\xDB\\xDC\\xDD\\xDE"
        "\\xDF\\xE0\\xE1\\xE2\\xE3\\xE4\\xE5\\xE6\\xE7\\xE8\\xE9\\xEA"
        "\\xEB\\xEC\\xED\\xEE\\xEF\\xF0\\xF1\\xF2\\xF3\\xF4\\xF5\\xF6"
        "\\xF7\\xF8\\xF9\\xFA\\xFB\\xFC\\xFD\\xFE\\xFF\"";

    EXPECT_EQ(outputStream.Str(), output);
}

TEST_F(TYsonWriterTest, NoNewLinesInEmptyMap)
{
    TStringStream outputStream;
    TYsonWriter writer(&outputStream, EYsonFormat::Pretty);
    writer.OnBeginMap();
    writer.OnEndMap();

    EXPECT_EQ(outputStream.Str(), "{}\n");
}

TEST_F(TYsonWriterTest, NoNewLinesInEmptyList)
{
    TStringStream outputStream;
    TYsonWriter writer(&outputStream, EYsonFormat::Pretty);
    writer.OnBeginList();
    writer.OnEndList();

    EXPECT_EQ(outputStream.Str(), "[]\n");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonFragmentWriterTest, NewLinesInList)
{
    TStringStream outputStream;

    TYsonWriter writer(&outputStream, EYsonFormat::Text, EYsonType::ListFragment);
    writer.OnListItem();
        writer.OnInt64Scalar(200);
    writer.OnListItem();
        writer.OnBeginMap();
            writer.OnKeyedItem("key");
            writer.OnInt64Scalar(42);
            writer.OnKeyedItem("yek");
            writer.OnInt64Scalar(24);
            writer.OnKeyedItem("list");
            writer.OnBeginList();
            writer.OnEndList();
        writer.OnEndMap();
    writer.OnListItem();
        writer.OnStringScalar("aaa");

    Stroka output =
        "200;\n"
        "{\"key\"=42;\"yek\"=24;\"list\"=[]};\n"
        "\"aaa\";\n";

    EXPECT_EQ(outputStream.Str(), output);
}


TEST(TYsonFragmentWriterTest, NewLinesInMap)
{
    TStringStream outputStream;

    TYsonWriter writer(&outputStream, EYsonFormat::Text, EYsonType::MapFragment);
    writer.OnKeyedItem("a");
        writer.OnInt64Scalar(100);
    writer.OnKeyedItem("b");
        writer.OnBeginList();
            writer.OnListItem();
            writer.OnBeginMap();
                writer.OnKeyedItem("key");
                writer.OnInt64Scalar(42);
                writer.OnKeyedItem("yek");
                writer.OnInt64Scalar(24);
            writer.OnEndMap();
            writer.OnListItem();
            writer.OnInt64Scalar(-1);
        writer.OnEndList();
    writer.OnKeyedItem("c");
        writer.OnStringScalar("word");

    Stroka output =
        "\"a\"=100;\n"
        "\"b\"=[{\"key\"=42;\"yek\"=24};-1];\n"
        "\"c\"=\"word\";\n";

    EXPECT_EQ(outputStream.Str(), output);
}

TEST(TYsonFragmentWriter, NoFirstIndent)
{
    TStringStream outputStream;

    TYsonWriter writer(&outputStream, EYsonFormat::Pretty, EYsonType::MapFragment);
    writer.OnKeyedItem("a1");
        writer.OnBeginMap();
            writer.OnKeyedItem("key");
            writer.OnInt64Scalar(42);
        writer.OnEndMap();
    writer.OnKeyedItem("a2");
        writer.OnInt64Scalar(0);

    Stroka output =
        "\"a1\" = {\n"
        "    \"key\" = 42\n"
        "};\n"
        "\"a2\" = 0;\n";

    EXPECT_EQ(outputStream.Str(), output);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYTree
} // namespace NYT
