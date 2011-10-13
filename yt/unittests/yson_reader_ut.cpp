#include "../ytlib/ytree/yson_reader.h"
#include <util/stream/mem.h>


#include <contrib/testing/framework.h>

namespace NYT {
namespace NYTree {

using ::testing::InSequence;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

class TMockConsumer
    : public NYTree::IYsonConsumer
{
public:
    MOCK_METHOD1(OnStringScalar, void(const Stroka& value));
    MOCK_METHOD1(OnInt64Scalar, void(i64 value));
    MOCK_METHOD1(OnDoubleScalar, void(double value));
    MOCK_METHOD0(OnEntityScalar, void());

    MOCK_METHOD0(OnBeginList, void());
    MOCK_METHOD1(OnListItem, void(int index));
    MOCK_METHOD0(OnEndList, void());

    MOCK_METHOD0(OnBeginMap, void());
    MOCK_METHOD1(OnMapItem, void(const Stroka& name));
    MOCK_METHOD0(OnEndMap, void());

    MOCK_METHOD0(OnBeginAttributes, void());
    MOCK_METHOD1(OnAttributesItem, void(const Stroka& name));
    MOCK_METHOD0(OnEndAttributes, void());
};

////////////////////////////////////////////////////////////////////////////////

class TYsonReaderTest: public ::testing::Test
{ };

TEST_F(TYsonReaderTest, Int64)
{
    Stroka input = "100500";
    TMemoryInput inputStream(input.c_str(), input.length());

    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnInt64Scalar(100500));

    TYsonReader reader(&mock);
    reader.Read(&inputStream);
}

TEST_F(TYsonReaderTest, Double)
{
    Stroka input = "31415926e-7";
    TMemoryInput inputStream(input.c_str(), input.length());

    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnDoubleScalar(::testing::DoubleEq(3.1415926)));

    TYsonReader reader(&mock);
    reader.Read(&inputStream);
}

TEST_F(TYsonReaderTest, StringStartingWithLetter)
{
    Stroka input = "Hello_789_World_123";
    TMemoryInput inputStream(input.c_str(), input.length());

    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnStringScalar("Hello_789_World_123"));

    TYsonReader reader(&mock);
    reader.Read(&inputStream);
}

TEST_F(TYsonReaderTest, StringStartingWithQuote)
{
    Stroka input = "\"abcdeABCDE <1234567> + (10_000) - = 900   \"";
    TMemoryInput inputStream(input.c_str(), input.length());

    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnStringScalar("abcdeABCDE <1234567> + (10_000) - = 900   "));

    TYsonReader reader(&mock);
    reader.Read(&inputStream);
}

TEST_F(TYsonReaderTest, EntityWithEmptyAttributes)
{
    Stroka input = "< >";
    TMemoryInput inputStream(input.c_str(), input.length());

    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnEntityScalar());
    EXPECT_CALL(mock, OnBeginAttributes());
    EXPECT_CALL(mock, OnEndAttributes());

    TYsonReader reader(&mock);
    reader.Read(&inputStream);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
