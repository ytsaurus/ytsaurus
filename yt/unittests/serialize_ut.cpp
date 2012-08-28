#include "stdafx.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/ytree/convert.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NYTree{

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(TSharedRef ref)
{
    return Stroka(ref.Begin(), ref.Size());
}

Stroka RemoveSpaces(const Stroka& str)
{
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
    TYsonString canonicalYson(
        "<\"acl\"={\"execute\"=[\"*\"]}>"
        "{\"mode\"=755;\"path\"=\"/home/sandello\"}"
    );
    auto root = ConvertToNode(canonicalYson);
    auto deserializedYson = ConvertToYsonString(root, EYsonFormat::Text);
    EXPECT_EQ(RemoveSpaces(canonicalYson.Data()), deserializedYson.Data());
}

TEST(TCustomTypeSerializationTest, TInstant)
{
    TInstant value = TInstant::MilliSeconds(100500);
    auto yson = ConvertToYsonString(value);
    auto deserializedValue = ConvertTo<TInstant>(yson);
    EXPECT_EQ(value, deserializedValue);
}

TEST(TSerializationTest, PackRefs)
{
    std::vector<TSharedRef> refs;
    refs.push_back(TSharedRef::FromString("abc"));
    refs.push_back(TSharedRef::FromString("12"));

    TSharedRef packed = PackRefs(refs);
    std::vector<TSharedRef> unpacked;
    UnpackRefs(packed, &unpacked);

    EXPECT_EQ(unpacked.size(), 2);
    EXPECT_EQ(ToString(unpacked[0]), "abc");
    EXPECT_EQ(ToString(unpacked[1]), "12");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
