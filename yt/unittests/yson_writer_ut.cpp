#include "stdafx.h"

#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/yson_consumer-mock.h>

#include <util/string/escape.h>

#include <contrib/testing/framework.h>

using ::testing::InSequence;
using ::testing::StrictMock;

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonWriterTest: public ::testing::Test
{
public:
    TStringStream Stream;
    StrictMock<TMockYsonConsumer> Mock;

    void Run()
    {
        Stream.Flush();

        ParseYson(&Stream, &Mock);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYsonWriterTest, BinaryString)
{
    Stroka value = "YSON";

    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar(value));

    TYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnStringScalar(value);

    Run();
}

TEST_F(TYsonWriterTest, BinaryInteger)
{
    i64 value = 100500424242ll;

    InSequence dummy;
    EXPECT_CALL(Mock, OnIntegerScalar(value));

    TYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnIntegerScalar(value);

    Run();
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
    EXPECT_CALL(Mock, OnMapItem("hello"));
    EXPECT_CALL(Mock, OnStringScalar("world"));
    EXPECT_CALL(Mock, OnEndMap());

    TYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnBeginMap();
    writer.OnMapItem("hello");
    writer.OnStringScalar("world");
    writer.OnEndMap();

    Run();
}

TEST_F(TYsonWriterTest, MapWithAttributes)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());

    EXPECT_CALL(Mock, OnMapItem("path"));
        EXPECT_CALL(Mock, OnStringScalar("/home/sandello"));

    EXPECT_CALL(Mock, OnMapItem("mode"));
        EXPECT_CALL(Mock, OnIntegerScalar(755));

    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnBeginAttributes());
    EXPECT_CALL(Mock, OnAttributesItem("acl"));
        EXPECT_CALL(Mock, OnBeginMap());

        EXPECT_CALL(Mock, OnMapItem("read"));
        EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("*"));
        EXPECT_CALL(Mock, OnEndList());

        EXPECT_CALL(Mock, OnMapItem("write"));
        EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("sandello"));
        EXPECT_CALL(Mock, OnEndList());

        EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnAttributesItem("lock_scope"));
        EXPECT_CALL(Mock, OnStringScalar("mytables"));

    EXPECT_CALL(Mock, OnEndAttributes());

    TYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnBeginMap();

    writer.OnMapItem("path");
        writer.OnStringScalar("/home/sandello");

    writer.OnMapItem("mode");
        writer.OnIntegerScalar(755);

    writer.OnEndMap();

    writer.OnBeginAttributes();
    writer.OnAttributesItem("acl");
        writer.OnBeginMap();

        writer.OnMapItem("read");
        writer.OnBeginList();
        writer.OnListItem();
        writer.OnStringScalar("*");
        writer.OnEndList();

        writer.OnMapItem("write");
        writer.OnBeginList();
        writer.OnListItem();
        writer.OnStringScalar("sandello");
        writer.OnEndList();

        writer.OnEndMap();

    writer.OnAttributesItem("lock_scope");
        writer.OnStringScalar("mytables");

    writer.OnEndAttributes();

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

TEST_F(TYsonWriterTest, SerializeToYson)
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


////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
