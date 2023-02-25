#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/serialize.h>

#include <library/cpp/yson/node/node_builder.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

Y_UNIT_TEST_SUITE(SecurityClient) {
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

    Y_UNIT_TEST(TestCheckPermission)
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
            UNIT_ASSERT_VALUES_EQUAL(result.Action, ESecurityAction::Deny);
        }

        {
            auto result = client->CheckPermission(user, EPermission::Read, workingDir + "/read_only");
            UNIT_ASSERT_VALUES_EQUAL(result.Action, ESecurityAction::Allow);
        }

        {
            auto result = client->CheckPermission(user, EPermission::Write, workingDir + "/read_only");
            UNIT_ASSERT_VALUES_EQUAL(result.Action, ESecurityAction::Deny);
        }

        {
            auto result = client->CheckPermission(user, EPermission::Write, workingDir + "/read_write");
            UNIT_ASSERT_VALUES_EQUAL(result.Action, ESecurityAction::Allow);
        }

        {
            auto result = client->CheckPermission(user, EPermission::Read, workingDir + "/deny_read");
            UNIT_ASSERT_VALUES_EQUAL(result.Action, ESecurityAction::Deny);
            UNIT_ASSERT_VALUES_EQUAL(result.SubjectName, user);
            UNIT_ASSERT(result.ObjectName.Defined());
            UNIT_ASSERT_STRING_CONTAINS(*result.ObjectName, "/deny_read");
            UNIT_ASSERT_VALUES_EQUAL(result.SubjectId, userId);
            UNIT_ASSERT_VALUES_EQUAL(result.ObjectId, denyReadId);
        }

        {
            auto result = client->CheckPermission(user, EPermission::Write, workingDir + "/deny_read");
            UNIT_ASSERT_VALUES_EQUAL(result.Action, ESecurityAction::Deny);
        }
    }

    Y_UNIT_TEST(TestCheckColumnarPermissions)
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

            UNIT_ASSERT_VALUES_EQUAL(result.Action, ESecurityAction::Allow);

            UNIT_ASSERT_VALUES_EQUAL(result.Columns.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(result.Columns[0].Action, ESecurityAction::Allow);
            UNIT_ASSERT_VALUES_EQUAL(result.Columns[1].Action, ESecurityAction::Allow);
            UNIT_ASSERT_VALUES_EQUAL(result.Columns[2].Action, ESecurityAction::Deny);
            UNIT_ASSERT_VALUES_EQUAL(result.Columns[2].SubjectId, userId);
            UNIT_ASSERT_VALUES_EQUAL(result.Columns[2].SubjectName, user);
        }

        {
            auto result = client->CheckPermission(user, EPermission::Write, workingDir + "/table", options);
            UNIT_ASSERT_VALUES_EQUAL(result.Action, ESecurityAction::Deny);
        }
    }
}
