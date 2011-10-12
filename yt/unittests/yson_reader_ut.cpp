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
    MOCK_METHOD0(BeginTree, void());
    MOCK_METHOD0(EndTree, void());

    MOCK_METHOD1(StringScalar, void(const Stroka& value));
    MOCK_METHOD1(Int64Scalar, void(i64 value));
    MOCK_METHOD1(DoubleScalar, void(double value));
    MOCK_METHOD0(EntityScalar, void());

    MOCK_METHOD0(BeginList, void());
    MOCK_METHOD1(ListItem, void(int index));
    MOCK_METHOD0(EndList, void());

    MOCK_METHOD0(BeginMap, void());
    MOCK_METHOD1(MapItem, void(const Stroka& name));
    MOCK_METHOD0(EndMap, void());

    MOCK_METHOD0(BeginAttributes, void());
    MOCK_METHOD1(AttributesItem, void(const Stroka& name));
    MOCK_METHOD0(EndAttributes, void());
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

    EXPECT_CALL(mock, Int64Scalar(100500));

    TYsonReader reader(&mock);
    reader.Read(&inputStream);
}

TEST_F(TYsonReaderTest, Double)
{
    Stroka input = "31415926e-7";
    TMemoryInput inputStream(input.c_str(), input.length());

    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, DoubleScalar(::testing::DoubleEq(3.1415926)));

    TYsonReader reader(&mock);
    reader.Read(&inputStream);
}

TEST_F(TYsonReaderTest, StringStartingWithLetter)
{
    Stroka input = "Hello_789_World_123";
    TMemoryInput inputStream(input.c_str(), input.length());

    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, StringScalar("Hello_789_World_123"));

    TYsonReader reader(&mock);
    reader.Read(&inputStream);
}

TEST_F(TYsonReaderTest, StringStartingWithQuote)
{
    Stroka input = "\"abcdeABCDE <1234567> + (10_000) - = 900   \"";
    TMemoryInput inputStream(input.c_str(), input.length());

    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, StringScalar("abcdeABCDE <1234567> + (10_000) - = 900   "));

    TYsonReader reader(&mock);
    reader.Read(&inputStream);
}

TEST_F(TYsonReaderTest, EntityWithEmptyAttributes)
{
    Stroka input = "< >";
    TMemoryInput inputStream(input.c_str(), input.length());

    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, EntityScalar());
    EXPECT_CALL(mock, BeginAttributes());
    EXPECT_CALL(mock, EndAttributes());

    TYsonReader reader(&mock);
    reader.Read(&inputStream);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
