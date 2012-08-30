#include "stdafx.h"

#include <ytlib/formats/yamred_dsv_writer.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TEST(TYamredDsvWriterTest, Simple)
{
    TStringStream outputStream;
    auto config = New<TYamredDsvFormatConfig>();
    config->HasSubkey = true;
    config->KeyColumnNames.push_back("key_a");
    config->KeyColumnNames.push_back("key_b");
    TYamredDsvWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("key_a");
        writer.OnStringScalar("a");
        writer.OnKeyedItem("key_b");
        writer.OnStringScalar("b");
    writer.OnEndMap();
    
    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("key_b");
        writer.OnStringScalar("1");
        writer.OnKeyedItem("column");
        writer.OnIntegerScalar(2);
        writer.OnKeyedItem("subkey");
        writer.OnStringScalar("3");
        writer.OnKeyedItem("key_a");
        writer.OnStringScalar("xxx");
    writer.OnEndMap();

    Stroka output =
        "a b\t\t\n"
        "xxx 1\t\tcolumn=2\tsubkey=3\n";

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT

