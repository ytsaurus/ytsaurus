#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/python/yson/limited_yson_writer.h>

namespace NYT::NPython {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TLimitedYsonWriterTest, RawValueUpdatesOutputLimit)
{
    TLimitedYsonWriter writer(/*limit*/ 8, NYson::EYsonFormat::Text);

    writer.OnBeginMap();
    writer.OnKeyedItem("a");
    writer.OnRaw("\"123456\"", NYson::EYsonType::Node);
    writer.OnKeyedItem("b");
    writer.OnStringScalar("value");
    writer.OnEndMap();

    EXPECT_EQ("{\"a\"=\"123456\";}", writer.GetResult());
}

TEST(TLimitedYsonWriterTest, LongKeyDoesNotUnderflowValueLimit)
{
    TLimitedYsonWriter writer(/*limit*/ 4, NYson::EYsonFormat::Text);

    writer.OnBeginMap();
    writer.OnKeyedItem("long-key");
    writer.OnStringScalar("value");
    writer.OnEndMap();

    EXPECT_EQ("{\"lon\"=\"\";}", writer.GetResult());
}

TEST(TLimitedYsonWriterTest, RequiredMapAfterAttributesIsClosed)
{
    TLimitedYsonWriter writer(/*limit*/ 2, NYson::EYsonFormat::Text);

    writer.OnBeginAttributes();
    writer.OnKeyedItem("a");
    writer.OnStringScalar("value");
    writer.OnEndAttributes();
    writer.OnBeginMap();
    writer.OnEndMap();

    EXPECT_EQ("<\"a\"=\"\";>{}", writer.GetResult());
}

TEST(TLimitedYsonWriterTest, RequiredListAfterAttributesIsClosed)
{
    TLimitedYsonWriter writer(/*limit*/ 2, NYson::EYsonFormat::Text);

    writer.OnBeginAttributes();
    writer.OnKeyedItem("a");
    writer.OnStringScalar("value");
    writer.OnEndAttributes();
    writer.OnBeginList();
    writer.OnEndList();

    EXPECT_EQ("<\"a\"=\"\";>[]", writer.GetResult());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NPython
