#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/serialize.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yson/node/node_builder.h>

using namespace NYT;
using namespace NYT::NTesting;

namespace {

TGUID GetOrCreateUser(const IClientBasePtr& client, const TString& user)
{
    if (!client->Exists("//sys/users/" + user)) {
        return client->Create("", NT_USER,
            TCreateOptions().Attributes(TNode()("name", user)));
    }
    return GetGuid(client->Get("//sys/users/" + user + "/@id").AsString());
}

TNode CreateAce(
    const TString& user,
    const TString& action,
    const TVector<TString>& permissions,
    const TMaybe<TVector<TString>>& columns = {})
{
    auto permissionsNode = TNode::CreateList();
    permissionsNode.AsList().assign(permissions.begin(), permissions.end());
    auto result = TNode()
        ("subjects", TNode().Add(user))
        ("permissions", std::move(permissionsNode))
        ("action", std::move(action));
    if (columns) {
        auto columnsNode = TNode::CreateList();
        columnsNode.AsList().assign(columns->begin(), columns->end());
        result["columns"] = std::move(columnsNode);
    }
    return result;
};

TEST(SecurityClient, TestCheckPermission)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TString user = "some_test_user";
    auto userId = GetOrCreateUser(client, user);

    client->Create(workingDir + "/no_rights", NT_MAP,  TCreateOptions()
        .Attributes(TNode()("inherit_acl", false)));
    client->Create(workingDir + "/read_only", NT_MAP,  TCreateOptions()
        .Attributes(TNode()
            ("inherit_acl", false)
            ("acl", TNode().Add(CreateAce(user, "allow", {"read"})))));
    client->Create(workingDir + "/read_write", NT_MAP,  TCreateOptions()
        .Attributes(TNode()
            ("inherit_acl", false)
            ("acl", TNode().Add(CreateAce(user, "allow", {"read", "write"})))));
    auto denyReadId = client->Create(workingDir + "/deny_read", NT_MAP,  TCreateOptions()
        .Attributes(TNode()
            ("inherit_acl", false)
            ("acl", TNode().Add(CreateAce(user, "deny", {"read"})))));

    {
        auto result = client->CheckPermission(user, EPermission::Read, workingDir + "/no_rights");
        EXPECT_EQ(result.Action, ESecurityAction::Deny);
    }

    {
        auto result = client->CheckPermission(user, EPermission::Read, workingDir + "/read_only");
        EXPECT_EQ(result.Action, ESecurityAction::Allow);
    }

    {
        auto result = client->CheckPermission(user, EPermission::Write, workingDir + "/read_only");
        EXPECT_EQ(result.Action, ESecurityAction::Deny);
    }

    {
        auto result = client->CheckPermission(user, EPermission::Write, workingDir + "/read_write");
        EXPECT_EQ(result.Action, ESecurityAction::Allow);
    }

    {
        auto result = client->CheckPermission(user, EPermission::Read, workingDir + "/deny_read");
        EXPECT_EQ(result.Action, ESecurityAction::Deny);
        EXPECT_EQ(result.SubjectName, user);
        EXPECT_TRUE(result.ObjectName.Defined());
        EXPECT_TRUE(result.ObjectName->Contains("/deny_read"));
        EXPECT_EQ(result.SubjectId, userId);
        EXPECT_EQ(result.ObjectId, denyReadId);
    }

    {
        auto result = client->CheckPermission(user, EPermission::Write, workingDir + "/deny_read");
        EXPECT_EQ(result.Action, ESecurityAction::Deny);
    }
}

TEST(SecurityClient, TestCheckColumnarPermissions)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TString user = "some_test_user";
    auto userId = GetOrCreateUser(client, user);

    auto schema = TTableSchema()
        .AddColumn(TColumnSchema().Name("a").Type(EValueType::VT_STRING))
        .AddColumn(TColumnSchema().Name("b").Type(EValueType::VT_STRING))
        .AddColumn(TColumnSchema().Name("c").Type(EValueType::VT_STRING));

    TNode schemaNode;
    TNodeBuilder builder(&schemaNode);
    Serialize(schema, &builder);

    client->Create(workingDir + "/table", NT_TABLE, TCreateOptions()
        .Attributes(TNode()
            ("inherit_acl", false)
            ("schema", schemaNode)
            ("acl", TNode()
                .Add(CreateAce(user, "allow", {"read"}))
                .Add(CreateAce(user, "allow", {"read"}, {{"a"}}))
                .Add(CreateAce(user, "deny", {"read"}, {{"c"}})))));

    TCheckPermissionOptions options;
    options.Columns_ = {"a", "b", "c"};

    {
        auto result = client->CheckPermission(user, EPermission::Read, workingDir + "/table", options);

        EXPECT_EQ(result.Action, ESecurityAction::Allow);

        EXPECT_EQ(std::ssize(result.Columns), 3);
        EXPECT_EQ(result.Columns[0].Action, ESecurityAction::Allow);
        EXPECT_EQ(result.Columns[1].Action, ESecurityAction::Allow);
        EXPECT_EQ(result.Columns[2].Action, ESecurityAction::Deny);
        EXPECT_EQ(result.Columns[2].SubjectId, userId);
        EXPECT_EQ(result.Columns[2].SubjectName, user);
    }

    {
        auto result = client->CheckPermission(user, EPermission::Write, workingDir + "/table", options);
        EXPECT_EQ(result.Action, ESecurityAction::Deny);
    }
}

} // namespace
