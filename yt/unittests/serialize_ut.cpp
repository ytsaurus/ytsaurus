#include "stdafx.h"

#include "../ytlib/ytree/serialize.h"

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
    Stroka someYson = "{\"mode\"=755;\"path\"=\"/home/sandello\"}"
                      "<\"acl\"={\"read\"=[\"*\"];\"write\"=[\"sandello\"]};"
                      "\"lock_scope\"=\"mytables\">";
    auto root = DeserializeFromYson(someYson);
    auto deserializedYson = SerializeToYson(root.Get(), TYsonWriter::EFormat::Text);
    EXPECT_EQ(deleteSpaces(someYson), deserializedYson) <<
        "Before deserialize/serialize: " << someYson << "\n" <<
        "After: " << deserializedYson;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
