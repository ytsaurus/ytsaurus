#include "stdafx.h"
#include "framework.h"

#include <ytlib/formats/schemaful_dsv_parser.h>

#include <core/ytree/yson_consumer-mock.h>
#include <core/ytree/null_yson_consumer.h>

namespace NYT {
namespace NFormats {
namespace {

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemafulDsvParserTest, Simple)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
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
    StrictMock<NYTree::TMockYsonConsumer> Mock;
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

    EXPECT_THROW({ ParseSchemafulDsv(input, NYTree::GetNullYsonConsumer(), config); }, std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
