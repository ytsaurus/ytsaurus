#include "stdafx.h"

#include <ytlib/formats/tsv_writer.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TEST(TTsvWriterTest, Simple)
{
    TStringStream outputStream;
    TTsvWriter writer(&outputStream);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("integer");
        writer.OnIntegerScalar(42);
        writer.OnKeyedItem("string");
        writer.OnStringScalar("some");
        writer.OnKeyedItem("double");
        writer.OnDoubleScalar(10.);     // let's hope that 10. will be serialized as 10.
    writer.OnEndMap();
    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("foo");
        writer.OnStringScalar("bar");
        writer.OnKeyedItem("one");
        writer.OnIntegerScalar(1);
    writer.OnEndMap();

    Stroka output =
        "integer=42\tstring=some\tdouble=10.\n"
        "foo=bar\tone=1";

    EXPECT_EQ(outputStream.Str(), output);
}

TEST(TTsvWriterTest, Tskv)
{
    auto config = New<TTsvFormatConfig>();
    config->LinePrefix = "tskv";

    TStringStream outputStream;
    TTsvWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginMap();
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("id");
        writer.OnStringScalar("1");
        writer.OnKeyedItem("guid");
        writer.OnIntegerScalar(100500);
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("id");
        writer.OnStringScalar("2");
        writer.OnKeyedItem("guid");
        writer.OnIntegerScalar(20025);
    writer.OnEndMap();

    Stroka output =
        "tskv\n"
        "tskv\tid=1\tguid=100500\n"
        "tskv\tid=2\tguid=20025";

    EXPECT_EQ(outputStream.Str(), output);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
