#include "stdafx.h"
#include "framework.h"

#include <core/ytree/yson_consumer-mock.h>

#include <core/yson/parser.h>

#include <util/stream/mem.h>

namespace NYT {
namespace NYson {
namespace {

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::HasSubstr;

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TYsonParserTest: public ::testing::Test
{
public:
    StrictMock<TMockYsonConsumer> Mock;

    void Run(const Stroka& input, EYsonType mode = EYsonType::Node, TNullable<i64> memoryLimit = Null)
    {
        TYsonParser parser(&Mock, mode, true, memoryLimit);
        parser.Read(input);
        parser.Finish();
    }

    void Run(const std::vector<Stroka>& input, EYsonType mode = EYsonType::Node, TNullable<i64> memoryLimit = Null)
    {
        TYsonParser parser(&Mock, mode, true, memoryLimit);
        for (const auto& str : input) {
            parser.Read(str);
        }
        parser.Finish();
    }

};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYsonParserTest, Int64)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnInt64Scalar(100500));

    Run("   100500  ");
}

TEST_F(TYsonParserTest, Uint64)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnUint64Scalar(100500));

    Run("   100500u  ");
}

TEST_F(TYsonParserTest, Double)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(3.1415926)));

    Run(" 31415926e-7  ");
}

TEST_F(TYsonParserTest, StringStartingWithLetter)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar("Hello_789_World_123"));

    Run(" Hello_789_World_123   ");
}

TEST_F(TYsonParserTest, StringStartingWithQuote)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar(" abcdeABCDE <1234567> + (10_000) - = 900   "));

    Run("\" abcdeABCDE <1234567> + (10_000) - = 900   \"");
}

TEST_F(TYsonParserTest, Entity)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnEntity());

    Run(" # ");
}

TEST_F(TYsonParserTest, BinaryInt64)
{
    InSequence dummy;
    {
        EXPECT_CALL(Mock, OnInt64Scalar(1ull << 21));

        //Int64Marker + (1 << 21) as VarInt ZigZagEncoded
        Run(Stroka(" \x02\x80\x80\x80\x02  ", 1 + 5 + 2));
    }

    {
        EXPECT_CALL(Mock, OnInt64Scalar(1ull << 21));

        //Uint64Marker + (1 << 21) as VarInt ZigZagEncoded
        std::vector<Stroka> parts = {Stroka("\x02"), Stroka("\x80\x80\x80\x02")};
        Run(parts);
    }
}

TEST_F(TYsonParserTest, BinaryUint64)
{
    InSequence dummy;
    {
        EXPECT_CALL(Mock, OnUint64Scalar(1ull << 22));

        //Int64Marker + (1 << 21) as VarInt ZigZagEncoded
        Run(Stroka(" \x06\x80\x80\x80\x02  ", 1 + 5 + 2));
    }

    {
        EXPECT_CALL(Mock, OnUint64Scalar(1ull << 22));

        //Uint64Marker + (1 << 21) as VarInt ZigZagEncoded
        std::vector<Stroka> parts = {Stroka("\x06"), Stroka("\x80\x80\x80\x02")};
        Run(parts);
    }
}

TEST_F(TYsonParserTest, BinaryDouble)
{
    InSequence dummy;
    double x = 2.71828;

    {
        EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(x)));

        Run(Stroka("\x03", 1) + Stroka((char*) &x, sizeof(double))); // DoubleMarker
    }

    {
        EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(x)));

        std::vector<Stroka> parts = {Stroka("\x03", 1), Stroka((char*) &x, sizeof(double))};
        Run(parts); // DoubleMarker
    }

    {
        EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(x)));

        std::vector<Stroka> parts = {Stroka("\x03", 1), Stroka((char*) &x, 4), Stroka(((char*) &x) + 4, 4)};
        Run(parts); // DoubleMarker
    }
}

TEST_F(TYsonParserTest, BinaryBoolean)
{
    InSequence dummy;

    EXPECT_CALL(Mock, OnBooleanScalar(false));
    Run(Stroka("\x04", 1));

    EXPECT_CALL(Mock, OnBooleanScalar(true));
    Run(Stroka("\x05", 1));
}


TEST_F(TYsonParserTest, InvalidBinaryDouble)
{
    EXPECT_THROW(Run(Stroka("\x03", 1)), std::exception);
}

TEST_F(TYsonParserTest, BinaryString)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar("YSON"));

    Run(Stroka(" \x01\x08YSON", 1 + 6)); // StringMarker + length ( = 4) + String
}

TEST_F(TYsonParserTest, EmptyBinaryString)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar(""));

    Run(Stroka("\x01\x00", 2)); // StringMarker + length ( = 0 )
}

TEST_F(TYsonParserTest, EmptyList)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginList());
    EXPECT_CALL(Mock, OnEndList());

    Run("  [    ]   ");
}

TEST_F(TYsonParserTest, EmptyMap)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    Run("  {    }   ");
}

TEST_F(TYsonParserTest, OneElementList)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginList());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnInt64Scalar(42));
    EXPECT_CALL(Mock, OnEndList());

    Run("  [  42  ]   ");
}

TEST_F(TYsonParserTest, OneElementMap)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("hello"));
    EXPECT_CALL(Mock, OnStringScalar("world"));
    EXPECT_CALL(Mock, OnEndMap());

    Run("  {  hello = world  }   ");
}

TEST_F(TYsonParserTest, OneElementBinaryMap)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("hello"));
    EXPECT_CALL(Mock, OnStringScalar("world"));
    EXPECT_CALL(Mock, OnEndMap());

    Run(Stroka("{\x01\x0Ahello=\x01\x0Aworld}",1 + 7 + 1 + 7 + 1));
}



TEST_F(TYsonParserTest, SeveralElementsList)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginList());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnInt64Scalar(42));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnUint64Scalar(36u));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(1000)));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnStringScalar("nosy_111"));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBooleanScalar(false));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnStringScalar("nosy is the best format ever!"));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnEndList());

    Run("  [  42    ; 36u  ; 1e3   ; nosy_111 ; %false; \"nosy is the best format ever!\"; { } ; ]   ");
}

TEST_F(TYsonParserTest, MapWithAttributes)
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

    Stroka input;
    input = "<acl = { read = [ \"*\" ]; write = [ sandello ] } ;  \n"
            "  lock_scope = mytables> \n"
            "{ path = \"/home/sandello\" ; mode = 0755 }";
    Run(input);
}

TEST_F(TYsonParserTest, Unescaping)
{
    Stroka output;
    for (int i = 0; i < 256; ++i) {
        output.push_back(char(i));
    }

    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar(output));

    Run(
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
        "\\xF7\\xF8\\xF9\\xFA\\xFB\\xFC\\xFD\\xFE\\xFF\"");
}

TEST_F(TYsonParserTest, TrailingSlashes)
{
    Stroka slash = "\\";
    Stroka escapedSlash = slash + slash;
    Stroka quote = "\"";

    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar(slash));

    Run(quote + escapedSlash + quote);
}

TEST_F(TYsonParserTest, ListFragment)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnInt64Scalar(1));
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnInt64Scalar(2));
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnInt64Scalar(3));
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnInt64Scalar(4));
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnInt64Scalar(5));

    Run("   1 ;2; 3; 4;5  ", EYsonType::ListFragment);
}

TEST_F(TYsonParserTest, ListFragmentWithTrailingSemicolon)
{
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

    Run("{};[];<>#;", EYsonType::ListFragment);
}

TEST_F(TYsonParserTest, OneListFragment)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnInt64Scalar(100500));

    Run("   100500  ", EYsonType::ListFragment);
}

TEST_F(TYsonParserTest, EmptyListFragment)
{
    InSequence dummy;
    Run("  ", EYsonType::ListFragment);
}

TEST_F(TYsonParserTest, MapFragment)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnKeyedItem("a"));
    EXPECT_CALL(Mock, OnInt64Scalar(1));
    EXPECT_CALL(Mock, OnKeyedItem("b"));
    EXPECT_CALL(Mock, OnInt64Scalar(2));
    EXPECT_CALL(Mock, OnKeyedItem("c"));
    EXPECT_CALL(Mock, OnInt64Scalar(3));
    EXPECT_CALL(Mock, OnKeyedItem("d"));
    EXPECT_CALL(Mock, OnInt64Scalar(4));
    EXPECT_CALL(Mock, OnKeyedItem("e"));
    EXPECT_CALL(Mock, OnInt64Scalar(5));

    Run("  a = 1 ;b=2; c= 3; d =4;e=5  ", EYsonType::MapFragment);
}

TEST_F(TYsonParserTest, MapFragmentWithTrailingSemicolon)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnKeyedItem("map"));
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnKeyedItem("list"));
    EXPECT_CALL(Mock, OnBeginList());
    EXPECT_CALL(Mock, OnEndList());
    EXPECT_CALL(Mock, OnKeyedItem("entity"));
    EXPECT_CALL(Mock, OnEntity());

    Run("map={};list=[];entity=#;", EYsonType::MapFragment);
}

TEST_F(TYsonParserTest, OneMapFragment)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnKeyedItem("1"));
    EXPECT_CALL(Mock, OnInt64Scalar(100500));

    Run("   \"1\" = 100500  ", EYsonType::MapFragment);
}

TEST_F(TYsonParserTest, EmptyMapFragment)
{
    InSequence dummy;
    Run("  ", EYsonType::MapFragment);
}

TEST_F(TYsonParserTest, MemoryLimit)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    Run("  {    }   ", EYsonType::Node, 1024);
}

TEST_F(TYsonParserTest, MemoryLimitExceeded)
{
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("key"));

    EXPECT_THROW(Run("{key=" + Stroka(10000, 'a') + "}", EYsonType::Node, 1024), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYTree
} // namespace NYT
