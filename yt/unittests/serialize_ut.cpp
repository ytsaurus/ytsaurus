#include "stdafx.h"
#include "framework.h"

#include <core/misc/serialize.h>
#include <core/ytree/convert.h>

namespace NYT {
namespace NYTree {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

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
        "<\"acl\"={\"execute\"=[\"*\";];};>"
        "{\"mode\"=755;\"path\"=\"/home/sandello\";}"
    );
    auto root = ConvertToNode(canonicalYson);
    auto deserializedYson = ConvertToYsonString(root, NYson::EYsonFormat::Text);
    EXPECT_EQ(RemoveSpaces(canonicalYson.Data()), deserializedYson.Data());
}

TEST(TCustomTypeSerializationTest, TInstant)
{
    TInstant value = TInstant::MilliSeconds(100500);
    auto yson = ConvertToYsonString(value);
    auto deserializedValue = ConvertTo<TInstant>(yson);
    EXPECT_EQ(value, deserializedValue);
}

TEST(TCustomTypeSerializationTest, TNullable)
{
    {
        TNullable<int> value(10);
        auto yson = ConvertToYsonString(value);
        EXPECT_EQ(10, ConvertTo<TNullable<int>>(yson));
    }
    {
        TNullable<int> value = Null;
        auto yson = ConvertToYsonString(value);
        EXPECT_EQ(Stroka("#"), yson.Data());
        EXPECT_EQ(value, ConvertTo<TNullable<int>>(yson));
    }
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

} // namespace
} // namespace NYTree
} // namespace NYT
