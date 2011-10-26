#include "../ytlib/ytree/yson_reader.h"
#include <util/stream/mem.h>

#include <contrib/testing/framework.h>

using ::testing::InSequence;
using ::testing::StrictMock;

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TMockConsumer
    : public NYTree::IYsonConsumer
{
public:
    MOCK_METHOD2(OnStringScalar, void(const Stroka& value, bool hasAttributes));
    MOCK_METHOD2(OnInt64Scalar, void(i64 value, bool hasAttributes));
    MOCK_METHOD2(OnDoubleScalar, void(double value, bool hasAttributes));
    MOCK_METHOD1(OnEntity, void(bool hasAttributes));

    MOCK_METHOD0(OnBeginList, void());
    MOCK_METHOD0(OnListItem, void());
    MOCK_METHOD1(OnEndList, void(bool hasAttributes));

    MOCK_METHOD0(OnBeginMap, void());
    MOCK_METHOD1(OnMapItem, void(const Stroka& name));
    MOCK_METHOD1(OnEndMap, void(bool hasAttributes));

    MOCK_METHOD0(OnBeginAttributes, void());
    MOCK_METHOD1(OnAttributesItem, void(const Stroka& name));
    MOCK_METHOD0(OnEndAttributes, void());
};

////////////////////////////////////////////////////////////////////////////////

class TYsonReaderTest: public ::testing::Test
{
public:
    Stroka Input;
    StrictMock<TMockConsumer> Mock;

    void Run()
    {
        TMemoryInput inputStream(Input.c_str(), Input.length());
        TYsonReader reader(&Mock);
        reader.Read(&inputStream);
    }
};

TEST_F(TYsonReaderTest, Int64)
{
    Input = "   100500  ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnInt64Scalar(100500, false));

    Run();
}

TEST_F(TYsonReaderTest, Double)
{
    Input = " 31415926e-7  ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(3.1415926), false));

    Run();
}

TEST_F(TYsonReaderTest, StringStartingWithLetter)
{
    Input = " Hello_789_World_123   ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar("Hello_789_World_123", false));

    Run();
}

TEST_F(TYsonReaderTest, StringStartingWithQuote)
{
    Input = "\" abcdeABCDE <1234567> + (10_000) - = 900   \"";

    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar(" abcdeABCDE <1234567> + (10_000) - = 900   ", false));

    Run();
}

TEST_F(TYsonReaderTest, EntityWithEmptyAttributes)
{
    Input = "< >";

    InSequence dummy;
    EXPECT_CALL(Mock, OnEntity(true));
    EXPECT_CALL(Mock, OnBeginAttributes());
    EXPECT_CALL(Mock, OnEndAttributes());

    Run();
}

TEST_F(TYsonReaderTest, BinaryInt64)
{
    Input = Stroka(" \x01\x80\x80\x80\x02  ", 1 + 5 + 2); //Int64Marker + (1 << 21) as VarInt ZigZagEncoded

    InSequence dummy;
    EXPECT_CALL(Mock, OnInt64Scalar(1ull << 21, false));

    Run();
}

TEST_F(TYsonReaderTest, BinaryDouble)
{
    double x = 2.71828;
    Input = Stroka("\x02", 1) + Stroka((char*) &x, sizeof(double)); // DoubleMarker

    InSequence dummy;
    EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(2.71828), false));

    Run();
}

TEST_F(TYsonReaderTest, BinaryString)
{
    Input = Stroka(" \x03\x08YSON", 1 + 6); // StringMarker + length ( = 4) + String

    InSequence dummy;
    EXPECT_CALL(Mock, OnStringScalar("YSON", false));

    Run();
}

TEST_F(TYsonReaderTest, EmptyList)
{
    Input = "  [    ]   ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginList());
    EXPECT_CALL(Mock, OnEndList(false));

    Run();
}

TEST_F(TYsonReaderTest, EmptyMap)
{
    Input = "  {    }   ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap(false));

    Run();
}

TEST_F(TYsonReaderTest, OneElementList)
{
    Input = "  [  42  ]   ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginList());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnInt64Scalar(42, false));
    EXPECT_CALL(Mock, OnEndList(false));

    Run();
}

TEST_F(TYsonReaderTest, OneElementMap)
{
    Input = "  {  hello = world  }   ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnMapItem("hello"));
    EXPECT_CALL(Mock, OnStringScalar("world", false));
    EXPECT_CALL(Mock, OnEndMap(false));

    Run();
}


TEST_F(TYsonReaderTest, SeveralElementsList)
{
    Input = "  [  42    ; 1e3   ; nosy_111 ; \"nosy is the best format ever!\"; { } ; ]   ";

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginList());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnInt64Scalar(42, false));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(1000), false));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnStringScalar("nosy_111", false));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnStringScalar("nosy is the best format ever!", false));

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap(false));

    EXPECT_CALL(Mock, OnEndList(false));

    Run();
}

TEST_F(TYsonReaderTest, MapWithAttributes)
{
    Input =  "{ path = \"/home/sandello\" ; mode = 0755 } \n";
    Input += "<acl = { read = [ \"*\" ]; write = [ sandello ] } ;  \n";
    Input += "  lock_scope = mytables>";

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());

    EXPECT_CALL(Mock, OnMapItem("path"));
        EXPECT_CALL(Mock, OnStringScalar("/home/sandello", false));

    EXPECT_CALL(Mock, OnMapItem("mode"));
        EXPECT_CALL(Mock, OnInt64Scalar(755, false));

    EXPECT_CALL(Mock, OnEndMap(true));

    EXPECT_CALL(Mock, OnBeginAttributes());
    EXPECT_CALL(Mock, OnAttributesItem("acl"));
        EXPECT_CALL(Mock, OnBeginMap());

        EXPECT_CALL(Mock, OnMapItem("read"));
        EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("*", false));
        EXPECT_CALL(Mock, OnEndList(false));

        EXPECT_CALL(Mock, OnMapItem("write"));
        EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("sandello", false));
        EXPECT_CALL(Mock, OnEndList(false));

        EXPECT_CALL(Mock, OnEndMap(false));

    EXPECT_CALL(Mock, OnAttributesItem("lock_scope"));
        EXPECT_CALL(Mock, OnStringScalar("mytables", false));

    EXPECT_CALL(Mock, OnEndAttributes());

    Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
