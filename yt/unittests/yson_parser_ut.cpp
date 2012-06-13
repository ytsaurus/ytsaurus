#include "stdafx.h"

#include <ytlib/ytree/yson_consumer-mock.h>

#include <ytlib/ytree/yson_parser.h>

#include <util/stream/mem.h>

#include <contrib/testing/framework.h>

using ::testing::InSequence;
using ::testing::StrictMock;

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonParserTest: public ::testing::Test
{
public:
    Stroka Input;
    StrictMock<TMockYsonConsumer> Mock;
    EYsonType Mode;

    virtual void SetUp()
    {
        Mode = EYsonType::Node;
    }

    void Run()
    {
        Mock.OnRaw(Input, Mode);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYsonParserTest, Integer)
{
    Input = "   100500  ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnIntegerScalar(100500));

    Run();
}

TEST_F(TYsonParserTest, Double)
{
    Input = " 31415926e-7  ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(3.1415926)));

    Run();
}

TEST_F(TYsonParserTest, StringStartingWithLetter)
{
    Input = " Hello_789_World_123   ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar("Hello_789_World_123"));

    Run();
}

TEST_F(TYsonParserTest, StringStartingWithQuote)
{
    Input = "\" abcdeABCDE <1234567> + (10_000) - = 900   \"";

    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar(" abcdeABCDE <1234567> + (10_000) - = 900   "));

    Run();
}

TEST_F(TYsonParserTest, Entity)
{
    Input = " # ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnEntity());

    Run();
}

TEST_F(TYsonParserTest, BinaryInteger)
{
    Input = Stroka(" \x02\x80\x80\x80\x02  ", 1 + 5 + 2); //IntegerMarker + (1 << 21) as VarInt ZigZagEncoded

    InSequence dummy;
    EXPECT_CALL(Mock, OnIntegerScalar(1ull << 21));

    Run();
}

TEST_F(TYsonParserTest, BinaryDouble)
{
    double x = 2.71828;
    Input = Stroka("\x03", 1) + Stroka((char*) &x, sizeof(double)); // DoubleMarker

    InSequence dummy;
    EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(2.71828)));

    Run();
}

TEST_F(TYsonParserTest, BinaryString)
{
    Input = Stroka(" \x01\x08YSON", 1 + 6); // StringMarker + length ( = 4) + String

    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar("YSON"));

    Run();
}

TEST_F(TYsonParserTest, EmptyBinaryString)
{
    Input = Stroka("\x01\x00", 2); // StringMarker + length ( = 0 )

    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar(""));

    Run();
}

TEST_F(TYsonParserTest, EmptyList)
{
    Input = "  [    ]   ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginList());
    EXPECT_CALL(Mock, OnEndList());

    Run();
}

TEST_F(TYsonParserTest, EmptyMap)
{
    Input = "  {    }   ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    Run();
}

TEST_F(TYsonParserTest, OneElementList)
{
    Input = "  [  42  ]   ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginList());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnIntegerScalar(42));
    EXPECT_CALL(Mock, OnEndList());

    Run();
}

TEST_F(TYsonParserTest, OneElementMap)
{
    Input = "  {  hello = world  }   ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("hello"));
    EXPECT_CALL(Mock, OnStringScalar("world"));
    EXPECT_CALL(Mock, OnEndMap());

    Run();
}

TEST_F(TYsonParserTest, OneElementBinaryMap)
{
    Input = Stroka("{\x01\x0Ahello=\x01\x0Aworld}",1 + 7 + 1 + 7 + 1);

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("hello"));
    EXPECT_CALL(Mock, OnStringScalar("world"));
    EXPECT_CALL(Mock, OnEndMap());

    Run();
}



TEST_F(TYsonParserTest, SeveralElementsList)
{
    Input = "  [  42    ; 1e3   ; nosy_111 ; \"nosy is the best format ever!\"; { } ; ]   ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginList());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnIntegerScalar(42));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(1000)));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnStringScalar("nosy_111"));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnStringScalar("nosy is the best format ever!"));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnEndList());

    Run();
}

TEST_F(TYsonParserTest, MapWithAttributes)
{
    Input = "<acl = { read = [ \"*\" ]; write = [ sandello ] } ;  \n";
    Input += "  lock_scope = mytables> \n";
    Input +=  "{ path = \"/home/sandello\" ; mode = 0755 }";

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
        EXPECT_CALL(Mock, OnIntegerScalar(755));
    EXPECT_CALL(Mock, OnEndMap());

    Run();
}

TEST_F(TYsonParserTest, Unescaping)
{
    Input =
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

    Stroka output;
    for (int i = 0; i < 256; ++i) {
        output.push_back(char(i));
    }

    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar(output));

    Run();
}

TEST_F(TYsonParserTest, TrailingSlashes)
{
    Stroka slash = "\\";
    Stroka escapedSlash = slash + slash;
    Stroka quote = "\"";
    Input = quote + escapedSlash + quote;

    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar(slash));

    Run();
}

TEST_F(TYsonParserTest, ListFragment)
{
    Input = "   1 ;2; 3; 4;5  ";
    Mode = EYsonType::ListFragment;

    InSequence dummy;
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnIntegerScalar(1));
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnIntegerScalar(2));
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnIntegerScalar(3));
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnIntegerScalar(4));
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnIntegerScalar(5));

    Run();
}

TEST_F(TYsonParserTest, ListFragmentWithTrailingSemicolon)
{
    Input = "{};[];<>#;";
    Mode = EYsonType::ListFragment;

    InSequence dummy;
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginList());
    EXPECT_CALL(Mock, OnEndList());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginAttributes());
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnEntity());

    Run();
}

TEST_F(TYsonParserTest, OneListFragment)
{
    Input = "   100500  ";
    Mode = EYsonType::ListFragment;

    InSequence dummy;
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnIntegerScalar(100500));

    Run();
}

TEST_F(TYsonParserTest, EmptyListFragment)
{
    Input = "  ";
    Mode = EYsonType::ListFragment;

    InSequence dummy;
    Run();
}

TEST_F(TYsonParserTest, MapFragment)
{
    Input = "  a = 1 ;b=2; c= 3; d =4;e=5  ";
    Mode = EYsonType::MapFragment;

    InSequence dummy;
    EXPECT_CALL(Mock, OnKeyedItem("a"));
    EXPECT_CALL(Mock, OnIntegerScalar(1));
    EXPECT_CALL(Mock, OnKeyedItem("b"));
    EXPECT_CALL(Mock, OnIntegerScalar(2));
    EXPECT_CALL(Mock, OnKeyedItem("c"));
    EXPECT_CALL(Mock, OnIntegerScalar(3));
    EXPECT_CALL(Mock, OnKeyedItem("d"));
    EXPECT_CALL(Mock, OnIntegerScalar(4));
    EXPECT_CALL(Mock, OnKeyedItem("e"));
    EXPECT_CALL(Mock, OnIntegerScalar(5));

    Run();
}

TEST_F(TYsonParserTest, MapFragmentWithTrailingSemicolon)
{
    Input = "map={};list=[];entity=#;";
    Mode = EYsonType::MapFragment;

    InSequence dummy;
    EXPECT_CALL(Mock, OnKeyedItem("map"));
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnKeyedItem("list"));
    EXPECT_CALL(Mock, OnBeginList());
    EXPECT_CALL(Mock, OnEndList());
    EXPECT_CALL(Mock, OnKeyedItem("entity"));
    EXPECT_CALL(Mock, OnEntity());

    Run();
}

TEST_F(TYsonParserTest, OneMapFragment)
{
    Input = "   \"1\" = 100500  ";
    Mode = EYsonType::MapFragment;

    InSequence dummy;
    EXPECT_CALL(Mock, OnKeyedItem("1"));
    EXPECT_CALL(Mock, OnIntegerScalar(100500));

    Run();
}

TEST_F(TYsonParserTest, EmptyMapFragment)
{
    Input = "  ";
    Mode = EYsonType::MapFragment;

    InSequence dummy;
    Run();
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
