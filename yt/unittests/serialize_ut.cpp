#include "stdafx.h"

#include <ytlib/ytree/serialize.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NYTree{

////////////////////////////////////////////////////////////////////////////////

Stroka deleteSpaces(const Stroka& str) {
    Stroka res = str;
    while (true) {
        size_t pos = res.find(" ");
        if (pos == Stroka::npos) {
            break;
        }
        res.replace(pos, 1, "");
    }
    return res;
}

TEST(TYTreeSerializationTest, All)
{
    Stroka someYson = "<\"acl\"={\"read\"=[\"*\"];\"write\"=[\"sandello\"]};"
                      "\"lock_scope\"=\"mytables\">"
                      "{\"mode\"=755;\"path\"=\"/home/sandello\"}";                      ;
    auto root = DeserializeFromYson(someYson);
    auto deserializedYson = SerializeToYson(root.Get(), EYsonFormat::Text);
    EXPECT_EQ(deleteSpaces(someYson), deserializedYson) <<
        "Before deserialize/serialize: " << someYson << "\n" <<
        "After: " << deserializedYson;
}

TEST(TCustomTypeSerializationTest, TInstant)
{
    TInstant value = TInstant::MilliSeconds(100500);
    auto yson = SerializeToYson(value);
    auto deserializedValue = DeserializeFromYson<TInstant>(yson);
    EXPECT_EQ(value, deserializedValue);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
