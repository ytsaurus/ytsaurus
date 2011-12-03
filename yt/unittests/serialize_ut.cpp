#include "stdafx.h"

#include "../ytlib/ytree/serialize.h"

#include <contrib/testing/framework.h>

using NYT::NYTree::INode;
using NYT::NYTree::TYsonWriter;
using NYT::NYTree::DeserializeFromYson;
using NYT::NYTree::SerializeToYson;

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
    INode::TPtr root = DeserializeFromYson(someYson);
    Stroka deserializedYson = SerializeToYson(root, TYsonWriter::EFormat::Text);
    EXPECT_EQ(deleteSpaces(someYson), deserializedYson) <<
        "Before deserialize/serialize: " << someYson << "\n" <<
        "After: " << deserializedYson;
}
