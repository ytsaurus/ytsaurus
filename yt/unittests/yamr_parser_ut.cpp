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
        EXPECT_CALL(Mock, OnKeyedItem("k"));
        EXPECT_CALL(Mock, OnStringScalar("key1"));
        EXPECT_CALL(Mock, OnKeyedItem("v"));
        EXPECT_CALL(Mock, OnStringScalar("value1"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("k"));
        EXPECT_CALL(Mock, OnStringScalar("key2"));
        EXPECT_CALL(Mock, OnKeyedItem("v"));
        EXPECT_CALL(Mock, OnStringScalar("value2"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input =
        "key1\tvalue1\n"
        "key2\tvalue2\n";

    ParseYamr(input, &Mock);
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
