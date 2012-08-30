#include "stdafx.h"

#include <ytlib/formats/yamred_dsv_parser.h>
#include <ytlib/ytree/yson_consumer-mock.h>

#include <contrib/testing/framework.h>

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;


namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TEST(TYamredDsvParserTest, Simple)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key_a"));
        EXPECT_CALL(Mock, OnStringScalar("1"));
        EXPECT_CALL(Mock, OnKeyedItem("key_b"));
        EXPECT_CALL(Mock, OnStringScalar("2"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey_x"));
        EXPECT_CALL(Mock, OnStringScalar("3"));
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("5"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar("6"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key_a"));
        EXPECT_CALL(Mock, OnStringScalar("7"));
        EXPECT_CALL(Mock, OnKeyedItem("key_b"));
        EXPECT_CALL(Mock, OnStringScalar("8"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey_x"));
        EXPECT_CALL(Mock, OnStringScalar("9"));
        EXPECT_CALL(Mock, OnKeyedItem("b"));
        EXPECT_CALL(Mock, OnStringScalar("max\tignat"));
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnStringScalar("100"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input =
        "1 2\t3\ta=5\tb=6\n"
        "7 8\t9\tb=max\\tignat\ta=100\n";
    
    auto config = New<TYamredDsvFormatConfig>();
    config->HasSubkey = true;
    config->KeyColumnNames.push_back("key_a");
    config->KeyColumnNames.push_back("key_b");
    config->SubkeyColumnNames.push_back("subkey_x");

    ParseYamredDsv(input, &Mock, config);
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT

