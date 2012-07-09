#include "stdafx.h"

#include <ytlib/formats/yamr_parser.h>
#include <ytlib/ytree/yson_consumer-mock.h>

#include <contrib/testing/framework.h>

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;


namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TEST(TYamrParserTest, Simple)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key1"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value1"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input =
        "key1\tvalue1\n"
        "key2\tvalue2\n";

    ParseYamr(input, &Mock);
}

TEST(TYamrParserTest, SimpleWithSubkey)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key1"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey1"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value1"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input =
        "key1\tsubkey1\tvalue1\n"
        "key2\tsubkey2\tvalue2\n";

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;

    ParseYamr(input, &Mock, config);
}

TEST(TYamrParserTest, SkippingRows)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key1"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey1"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value1"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input =
        "key\n"
        "key1\tsubkey1\tvalue1\n"
        "key\tsubkey\n"
        "key2\tsubkey2\tvalue2\n"
        "key\tsubkey\n";

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;

    ParseYamr(input, &Mock, config);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYamrLenvalParserTest, Simple)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key1"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value1"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input = Stroka(
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"

        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2"
        , 2 * (2 * 4 + 4 + 6) // all i32 + lengths of keys
    );

    auto config = New<TYamrFormatConfig>();
    config->Lenval = true;

    ParseYamr(input, &Mock, config);
}


TEST(TYamrLenvalParserTest, SimpleWithSubkey)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key1"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey1"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value1"));
    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input = Stroka(
        "\x04\x00\x00\x00" "key1"
        "\x07\x00\x00\x00" "subkey1"
        "\x06\x00\x00\x00" "value1"

        "\x04\x00\x00\x00" "key2"
        "\x07\x00\x00\x00" "subkey2"
        "\x06\x00\x00\x00" "value2"
        , 2 * (3 * 4 + 4 + 7 + 6) // all i32 + lengths of keys
    );

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;

    ParseYamr(input, &Mock, config);
}

TEST(TYamrLenvalParserTest, EmptyFields)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar(""));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar(""));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar(""));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input = Stroka(
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00"
        , 3 * 4
    );

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;

    ParseYamr(input, &Mock, config);
}


////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
