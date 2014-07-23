#include "stdafx.h"
#include "framework.h"

#include <core/ytree/yson_consumer-mock.h>
#include <core/ytree/null_yson_consumer.h>

#include <ytlib/formats/yamr_parser.h>

namespace NYT {
namespace NFormats {
namespace {

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

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
    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("table_index"));
        EXPECT_CALL(Mock, OnInt64Scalar(2));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnEntity());

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input =
        "key1\tvalue1\n"
        "2\n"
        "key2\tvalue2\n";

    ParseYamr(input, &Mock);
}

TEST(TYamrParserTest, ValueWithTabs)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar(TStringBuf("key1\0", 5)));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("value with \t and some other"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar(TStringBuf("another\0 value with \t", 21)));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input(
        "key1\0\tvalue with \t and some other\n"
        "key2\tanother\0 value with \t\n",
        34 +
        27);

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

TEST(TYamrParserTest, IncompleteRows)
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
        EXPECT_CALL(Mock, OnStringScalar("key"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("subkey"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar(""));
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
        "key\tsubkey\n"
        "key2\tsubkey2\tvalue2\n";

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;

    ParseYamr(input, &Mock, config);
}

TEST(TYamrParserTest, IncorrectIncompleteRows)
{
    auto Null = NYTree::GetNullYsonConsumer();

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = false;

    EXPECT_THROW(ParseYamr("\n", Null, config), std::exception);
    EXPECT_THROW(ParseYamr("key\n", Null, config), std::exception);
    EXPECT_THROW(ParseYamr("key\tvalue\nkey\n", Null, config), std::exception);
}

TEST(TYamrParserTest, TabsInValue)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("key"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("a\tb\\tc\t"));
    EXPECT_CALL(Mock, OnEndMap());

    auto config = New<TYamrFormatConfig>();
    Stroka input = "key\ta\tb\\tc\t";
    ParseYamr(input, &Mock, config);
}


TEST(TYamrParserTest, Escaping)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("key"));
        EXPECT_CALL(Mock, OnStringScalar("\tkey\t"));
        EXPECT_CALL(Mock, OnKeyedItem("subkey"));
        EXPECT_CALL(Mock, OnStringScalar("\n"));
        EXPECT_CALL(Mock, OnKeyedItem("value"));
        EXPECT_CALL(Mock, OnStringScalar("a\tb\t\n"));
    EXPECT_CALL(Mock, OnEndMap());

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->EnableEscaping = true;

    Stroka input = "\\tkey\\t\t\\n\ta\tb\t\\n\n";
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
    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("table_index"));
        EXPECT_CALL(Mock, OnInt64Scalar(1));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnEntity());

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

        "\xff\xff\xff\xff" "\x01\x00\x00\x00"

        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2"
        , 2 * (2 * 4 + 4 + 6) + 8 // all i32 + lengths of keys
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

TEST(TYamrLenvalParserTest, HugeLength)
{
    Stroka input = Stroka(
        "\xFF\xFF\xFF\xFF"
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00"
        , 3 * 4
    );

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;

    auto Null = NYTree::GetNullYsonConsumer();

    EXPECT_THROW(ParseYamr(input, Null, config), std::exception);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
