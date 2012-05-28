#include "stdafx.h"

#include <ytlib/driver/tsv_writer.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NDriver {

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
        writer.OnDoubleScalar(10);     // let's hope that 10 will be serialized as 10
    writer.OnEndMap();
    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("foo");
        writer.OnStringScalar("bar");
        writer.OnKeyedItem("one");
        writer.OnIntegerScalar(1);
    writer.OnEndMap();

    Stroka output =
        "integer=42\tstring=some\tdouble=10\n"
        "foo=bar\tone=1";

    EXPECT_EQ(outputStream.Str(), output);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
