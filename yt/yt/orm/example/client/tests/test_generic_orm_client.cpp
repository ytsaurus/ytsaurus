#include "common.h"

#include <yt/yt/orm/client/generic/generic_orm_client.h>

#include <yt/yt/orm/server/access_control/public.h>

#include <optional>

namespace NYT::NOrm::NExample::NClient::NTests {

using namespace NYT::NOrm::NServer::NAccessControl;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGenericOrmClientCommonTest
    : public TGenericOrmClientTest
{
public:
    inline static const TString ExampleServiceName = "NYT.NOrm.NExample.NClient.NProto.ObjectService";

    inline static const NYTree::IMapNodePtr DefaultAclNode = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("action").Value("allow")
            .Item("permissions")
                .BeginList()
                    .Item().Value("read")
                .EndList()
            .Item("subjects")
                .BeginList()
                    .Item().Value("everyone")
                .EndList()
        .EndMap()->AsMap();

    static void CreateObject(
        const TString& id,
        TObjectTypeValue objectType,
        NOrm::NClient::NGeneric::IOrmClientPtr client)
    {
        WaitFor(
            client->CreateObject(
                objectType,
                TYsonPayload{ .Yson = NYson::TYsonString(Format(R"({"meta"={"id"="%v"}})", id)) },
                TCreateObjectOptions{
                    .Format = EPayloadFormat::Yson,
                },
                /*updateIfExisting*/ std::nullopt))
                .ValueOrThrow();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TGenericOrmClientCommonTest, TestSelectObjects)
{
    auto ormClient = NOrm::NClient::NGeneric::CreateGenericOrmClient(GetChannelProvider(), ExampleServiceName);
    auto response = WaitFor(
        ormClient->SelectObjects(
            /*objectType*/ TObjectTypeValues::Group,
            /*selector*/ {"/meta"},
            TSelectObjectsOptions{
                .Format = EPayloadFormat::Yson,
                .AllowFullScan = true,
            }))
            .ValueOrThrow();
    EXPECT_EQ(response.Results.size(), 1ul);
    EXPECT_EQ(response.Results[0].ValuePayloads.size(), 1ul);
    auto ysonMeta = std::get<NOrm::NClient::NNative::TYsonPayload>(response.Results[0].ValuePayloads[0]).Yson;
    auto metaNode = NYTree::ConvertToNode(ysonMeta)->AsMap();

    EXPECT_EQ(metaNode->FindChild("id")->AsString()->GetValue(), "superusers");
    EXPECT_EQ(metaNode->FindChild("key")->AsString()->GetValue(), "superusers");
}

TEST_F(TGenericOrmClientCommonTest, TestGetObjects)
{
    auto ormClient = NOrm::NClient::NGeneric::CreateGenericOrmClient(GetChannelProvider(), ExampleServiceName);
    auto response = WaitFor(
        ormClient->GetObjects(
            {TObjectIdentity{"book"}},
            /*objectType*/ TObjectTypeValues::Schema,
            /*selector*/ {"/meta/acl"},
            TGetObjectOptions{ .Format = EPayloadFormat::Yson }))
            .ValueOrThrow();
    EXPECT_EQ(response.Subresults.size(), 1ul);
    EXPECT_EQ(response.Subresults[0].ValuePayloads.size(), 1ul);

    auto ysonMetaAcl = std::get<NOrm::NClient::NNative::TYsonPayload>(response.Subresults[0].ValuePayloads[0]).Yson;
    auto aclNodes = NYTree::ConvertToNode(ysonMetaAcl)->AsList()->GetChildren();

    EXPECT_EQ(aclNodes.size(), 1ul);
    EXPECT_EQ(NYson::ConvertToYsonString(DefaultAclNode), NYson::ConvertToYsonString(aclNodes[0]));
}

TEST_F(TGenericOrmClientCommonTest, TestCreateObject)
{
    auto ormClient = NOrm::NClient::NGeneric::CreateGenericOrmClient(GetChannelProvider(), ExampleServiceName);
    TString userId = "TestUser";
    CreateObject(userId, TObjectTypeValues::User, ormClient);

    auto getResponse = WaitFor(
        ormClient->GetObjects(
            {TObjectIdentity{userId}},
            /*objectType*/ TObjectTypeValues::User,
            /*selector*/ {"/meta/id"},
            TGetObjectOptions{ .Format = EPayloadFormat::Yson }))
            .ValueOrThrow();
    EXPECT_EQ(getResponse.Subresults.size(), 1ul);
    EXPECT_EQ(getResponse.Subresults[0].ValuePayloads.size(), 1ul);

    auto ysonId = std::get<NOrm::NClient::NNative::TYsonPayload>(getResponse.Subresults[0].ValuePayloads[0]).Yson;
    EXPECT_EQ(NYTree::ConvertTo<TString>(ysonId), userId);
}

TEST_F(TGenericOrmClientCommonTest, TestUpdateObject)
{
    auto ormClient = NOrm::NClient::NGeneric::CreateGenericOrmClient(GetChannelProvider(), ExampleServiceName);
    TString userId = "TestUser1";
    CreateObject(userId, TObjectTypeValues::User, ormClient);
    TString groupId = "TestGroup";
    CreateObject(groupId, TObjectTypeValues::Group, ormClient);

    auto getResponseBeforeUpdate = WaitFor(
        ormClient->GetObjects(
            {TObjectIdentity{groupId}},
            /*objectType*/ TObjectTypeValues::Group,
            /*selector*/ {"/spec/members"},
            TGetObjectOptions{ .Format = EPayloadFormat::Yson }))
            .ValueOrThrow();

    EXPECT_EQ(getResponseBeforeUpdate.Subresults.size(), 1ul);
    EXPECT_EQ(getResponseBeforeUpdate.Subresults[0].ValuePayloads.size(), 1ul);
    auto ysonListBeforeUpdate = std::get<NOrm::NClient::NNative::TYsonPayload>(
        getResponseBeforeUpdate.Subresults[0].ValuePayloads[0])
        .Yson;
    EXPECT_EQ(NYTree::ConvertToNode(ysonListBeforeUpdate)->GetType(), NYTree::ENodeType::Entity);

    WaitFor(
        ormClient->UpdateObject(
            {TObjectIdentity{groupId}},
            /*objectType*/ TObjectTypeValues::Group,
            {
                TSetUpdate{
                    "/spec/members/end",
                    BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
                        NYTree::BuildYsonFluently(consumer).Value(userId);
                    }))
                }
            },
            /*attributeTimestampPrerequisites*/ {},
            /*options*/ {}))
            .ValueOrThrow();

    auto getResponse = WaitFor(
        ormClient->GetObjects(
            {TObjectIdentity{groupId}},
            /*objectType*/ TObjectTypeValues::Group,
            /*selector*/ {"/spec/members"},
            TGetObjectOptions{ .Format = EPayloadFormat::Yson }))
            .ValueOrThrow();

    EXPECT_EQ(getResponse.Subresults.size(), 1ul);
    EXPECT_EQ(getResponse.Subresults[0].ValuePayloads.size(), 1ul);

    auto ysonList = std::get<NOrm::NClient::NNative::TYsonPayload>(getResponse.Subresults[0].ValuePayloads[0]).Yson;
    auto members = NYTree::ConvertToNode(ysonList)->AsList()->GetChildren();
    EXPECT_EQ(members.size(), 1ul);
    EXPECT_EQ(members[0]->AsString()->GetValue(), userId);
}

TEST_F(TGenericOrmClientCommonTest, TestRemoveObject)
{
    auto ormClient = NOrm::NClient::NGeneric::CreateGenericOrmClient(GetChannelProvider(), ExampleServiceName);

    TString userId = "TestRemoveUser";
    EXPECT_NO_THROW(WaitFor(
        ormClient->RemoveObject(
            TObjectIdentity{userId},
            /*objectType*/ TObjectTypeValues::User,
            TRemoveObjectOptions{
                .IgnoreNonexistent = true,
            }))
            .ValueOrThrow());

    CreateObject(userId, TObjectTypeValues::User, ormClient);
    EXPECT_NO_THROW(WaitFor(
        ormClient->RemoveObject(
            TObjectIdentity{userId},
            /*objectType*/ TObjectTypeValues::User,
            TRemoveObjectOptions{
                .IgnoreNonexistent = true
            }))
            .ValueOrThrow());

    auto getResponse = WaitFor(
        ormClient->GetObjects(
            {TObjectIdentity{userId}},
            /*objectType*/ TObjectTypeValues::User,
            /*selector*/ {"/meta/id"},
            TGetObjectOptions{
                .Format = EPayloadFormat::Yson,
                .IgnoreNonexistent = true
            }))
            .ValueOrThrow();
    EXPECT_EQ(getResponse.Subresults.size(), 1ul);
    EXPECT_EQ(getResponse.Subresults[0].ValuePayloads.size(), 0ul);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NExample::NClient::NTests
