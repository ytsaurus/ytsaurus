#include <yt/yt/flow/library/cpp/vanilla/spec.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow {
namespace {

using namespace NSecurityClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TVanillaSpec MakeSpec(TVanillaTaskSpec task)
{
    TVanillaSpec spec;
    task.Name = "worker";
    spec.Tasks.push_back(std::move(task));
    return spec;
}

IMapNodePtr GetTask(const IMapNodePtr& spec)
{
    return spec->GetChildOrThrow("tasks")->AsMap()->GetChildOrThrow("worker")->AsMap();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TVanillaSpecTest, EmitsDockerImage)
{
    TVanillaTaskSpec task;
    task.DockerImage = "registry.example.com/image:tag";

    auto operationSpec = BuildVanillaOperationSpec(MakeSpec(std::move(task)));

    EXPECT_EQ(
        "registry.example.com/image:tag",
        GetTask(operationSpec)->GetChildOrThrow("docker_image")->GetValue<std::string>());
    // A docker image is the counterpart of layers, so it must not pin the operation to porto nodes
    // the way UsePorto does.
    EXPECT_FALSE(operationSpec->FindChild("scheduling_tag_filter"));
}

TEST(TVanillaSpecTest, OmitsDockerImageWhenUnset)
{
    auto operationSpec = BuildVanillaOperationSpec(MakeSpec(TVanillaTaskSpec()));

    EXPECT_FALSE(GetTask(operationSpec)->FindChild("docker_image"));
}

TEST(TVanillaSpecTest, EnablesRpcProxyInJobProxy)
{
    TVanillaTaskSpec task;
    task.EnableRpcProxyInJobProxy = true;

    auto operationSpec = BuildVanillaOperationSpec(MakeSpec(std::move(task)));
    auto worker = GetTask(operationSpec);

    EXPECT_TRUE(worker->GetChildOrThrow("enable_rpc_proxy_in_job_proxy")->AsBoolean()->GetValue());
}

////////////////////////////////////////////////////////////////////////////////

TSerializableAccessControlList ParseAcl(TStringBuf yson)
{
    return ConvertTo<TSerializableAccessControlList>(TYsonStringBuf(yson));
}

TEST(TValidateVanillaOperationAclTest, AcceptsOperationPermissions)
{
    ValidateVanillaOperationAcl(ParseAcl(R"([
        {action=allow;subjects=[readers];permissions=[read]};
        {action=allow;subjects=[operators];permissions=[read;manage]};
        {action=allow;subjects=[admins];permissions=[administer]};
        {action=deny;subjects=[blocked];permissions=[read];subject_tag_filter="untrusted"};
    ])"));
    ValidateVanillaOperationAcl({});
}

TEST(TValidateVanillaOperationAclTest, RejectsNonOperationPermissions)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateVanillaOperationAcl(ParseAcl(R"([{action=allow;subjects=[writers];permissions=[read;write]}])")),
        "Only \"read\", \"manage\" and \"administer\" permissions");
}

TEST(TValidateVanillaOperationAclTest, RejectsUndefinedAction)
{
    TSerializableAccessControlList acl;
    acl.Entries.emplace_back(ESecurityAction::Undefined, std::vector<std::string>{"alice"}, EPermissionSet(EPermission::Read));

    EXPECT_THROW_WITH_SUBSTRING(ValidateVanillaOperationAcl(acl), "action");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TVanillaSpecTest, OmitsAclWhenAbsent)
{
    auto operationSpec = BuildVanillaOperationSpec(TVanillaSpec());

    EXPECT_FALSE(operationSpec->FindChild("acl"));
}

TEST(TVanillaSpecTest, EmitsPresentEmptyAcl)
{
    TVanillaSpec spec;
    spec.Acl.emplace();

    auto operationSpec = BuildVanillaOperationSpec(spec);

    ASSERT_TRUE(operationSpec->FindChild("acl"));
    EXPECT_TRUE(ConvertTo<TSerializableAccessControlList>(operationSpec->GetChildOrThrow("acl")).Entries.empty());
}

TEST(TVanillaSpecTest, EmitsExplicitOperationPermissions)
{
    auto explicitAcl = ParseAcl(R"([
        {action=allow;subjects=[readers];permissions=[read]};
        {action=allow;subjects=[operators];permissions=[read;manage]};
        {action=allow;subjects=[admins];permissions=[administer]};
        {action=deny;subjects=[blocked];permissions=[read]};
    ])");
    TVanillaSpec spec;
    spec.Acl = explicitAcl;

    auto operationSpec = BuildVanillaOperationSpec(spec);

    EXPECT_EQ(ConvertTo<TSerializableAccessControlList>(operationSpec->GetChildOrThrow("acl")), explicitAcl);
}

TEST(TVanillaSpecTest, RejectsInvalidAcl)
{
    TVanillaSpec spec;
    spec.Acl = ParseAcl(R"([{action=allow;subjects=[writers];permissions=[write]}])");

    EXPECT_THROW_WITH_SUBSTRING(BuildVanillaOperationSpec(spec), "permissions are allowed in operation ACL");

    spec.Acl->Entries[0] = TSerializableAccessControlEntry(
        ESecurityAction::Undefined,
        std::vector<std::string>{"alice"},
        EPermissionSet(EPermission::Read));
    EXPECT_THROW_WITH_SUBSTRING(BuildVanillaOperationSpec(spec), "action");
}

TEST(TVanillaSpecTest, NeverEmitsAcoOrDefaultAccessControls)
{
    TVanillaSpec spec;
    spec.Acl = ParseAcl(R"([{action=allow;subjects=[readers];permissions=[read]}])");

    for (const auto& specWithAcl : {BuildVanillaOperationSpec(spec), BuildVanillaOperationSpec(TVanillaSpec())}) {
        EXPECT_FALSE(specWithAcl->FindChild("aco_name"));
        EXPECT_FALSE(specWithAcl->FindChild("owners"));
        EXPECT_FALSE(specWithAcl->FindChild("add_authenticated_user_to_acl"));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
