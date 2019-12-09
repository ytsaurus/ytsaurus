#include <yt/core/test_framework/framework.h>

#include <yt/client/security_client/acl.h>

#include <yt/core/yson/pull_parser_deserialize.h>

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace {

using namespace NYson;
using namespace NYTree;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

template<typename TOriginal, typename TResult = TOriginal>
void TestSerializationDeserialization(const TOriginal& original) {
    auto yson = ConvertToYsonString(original);
    auto deserialized = ConvertTo<TResult>(yson);
    EXPECT_EQ(original, deserialized);
}

// TODO(levysotsky): Consider uniting this function with the previous one
// when all types are pull-parser-deserializable.
template<typename TOriginal, typename TResult = TOriginal>
void TestSerializationDeserializationPullParser(const TOriginal& original) {
    auto yson = ConvertToYsonString(original);
    TResult deserialized;
    TMemoryInput input(yson.GetData());
    TYsonPullParser parser(&input, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);
    Deserialize(deserialized, &cursor);
    EXPECT_EQ(original, deserialized);
}

TEST(TYtlibSerializationTest, Acl)
{
    TSerializableAccessControlList acl;
    TSerializableAccessControlEntry ace1;
    ace1.InheritanceMode = EAceInheritanceMode::ImmediateDescendantsOnly;
    ace1.Subjects = {"levysotsky", "ermolovd"};
    ace1.Action = ESecurityAction::Allow;
    ace1.Permissions = EPermission::Administer | EPermission::ModifyChildren;
    acl.Entries.push_back(std::move(ace1));

    TSerializableAccessControlEntry ace2;
    ace2.Subjects = {"everyone"};
    ace2.Action = ESecurityAction::Deny;
    ace2.Permissions = EPermission::Read;
    ace2.Columns = {"column_1", "column_2"};
    acl.Entries.push_back(std::move(ace2));

    TestSerializationDeserialization(acl);
    TestSerializationDeserializationPullParser(acl);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
