#include "stdafx.h"
#include "framework.h"

#include <ytlib/formats/schemaful_dsv_parser.h>

#include <core/yson/consumer-mock.h>
#include <core/yson/null_consumer.h>

namespace NYT {
namespace NFormats {
namespace {

using namespace NYson;

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemafulDsvParserTest, Simple)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("5"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar("6"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("100"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar("max\tignat"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input =
        "5\t6\n"
        "100\tmax\\tignat\n";

    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = std::vector<Stroka>();
    config->Columns->push_back("a");
    config->Columns->push_back("b");

    ParseSchemafulDsv(input, &Mock, config);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemafulDsvParserTest, TableIndex)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("table_index"));
        EXPECT_CALL(Mock, OnInt64Scalar(1));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnEntity());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("x"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("table_index"));
        EXPECT_CALL(Mock, OnInt64Scalar(0));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnEntity());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("y"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("z"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input =
        "1\tx\n"
        "0\ty\n"
        "0\tz\n";

    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = std::vector<Stroka>();
    config->Columns->push_back("a");
    config->EnableTableIndex = true;

    ParseSchemafulDsv(input, &Mock, config);
}

TEST(TSchemafulDsvParserTest, TooManyRows)
{
    Stroka input = "5\t6\n";

    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = {"a"};

    EXPECT_THROW({ ParseSchemafulDsv(input, GetNullYsonConsumer(), config); }, std::exception);
}

TEST(TSchemafulDsvParserTest, SpecialSymbols)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    auto value = Stroka("6\0", 2);
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("5\r"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar(value));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input("5\r\t6\0\n", 6);

    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = std::vector<Stroka>();
    config->Columns->push_back("a");
    config->Columns->push_back("b");

    ParseSchemafulDsv(input, &Mock, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
