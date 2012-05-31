#include "stdafx.h"

#include <ytlib/formats/tsv_parser.h>
#include <ytlib/ytree/yson_consumer-mock.h>

#include <contrib/testing/framework.h>

using ::testing::InSequence;
using ::testing::StrictMock;

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TEST(TTsvParserTest, Simple)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("integer"));
        EXPECT_CALL(Mock, OnStringScalar("42"));
        EXPECT_CALL(Mock, OnKeyedItem("string"));
        EXPECT_CALL(Mock, OnStringScalar("some"));
        EXPECT_CALL(Mock, OnKeyedItem("double"));
        EXPECT_CALL(Mock, OnStringScalar("10"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
        EXPECT_CALL(Mock, OnKeyedItem("one"));
        EXPECT_CALL(Mock, OnStringScalar("1"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input =
        "integer=42\tstring=some\tdouble=10\n"
        "foo=bar\tone=1";

    ParseTsv(input, &Mock);
}

TEST(TTsvParserTest, Tskv)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("id"));
        EXPECT_CALL(Mock, OnStringScalar("1"));
        EXPECT_CALL(Mock, OnKeyedItem("guid"));
        EXPECT_CALL(Mock, OnStringScalar("100500"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("id"));
        EXPECT_CALL(Mock, OnStringScalar("2"));
        EXPECT_CALL(Mock, OnKeyedItem("guid"));
        EXPECT_CALL(Mock, OnStringScalar("20025"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input =
        "tskv\n"
        "tskv\tid=1\tguid=100500\n"
        "tskv\tid=2\tguid=20025\n";

    auto config = New<TTsvFormatConfig>();
    config->LinePrefix = "tskv";

    ParseTsv(input, &Mock, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
