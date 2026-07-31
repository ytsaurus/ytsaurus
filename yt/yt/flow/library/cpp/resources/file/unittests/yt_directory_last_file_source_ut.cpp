#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/resources/file/yt_directory_last_file_source.h>
#include <yt/yt/flow/library/cpp/resources/file/yt_file_source.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NClient::NCache;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TDirectoryTestClientsCache
    : public IClientsCache
{
public:
    explicit TDirectoryTestClientsCache(IClientPtr client)
        : Client_(std::move(client))
    { }

    IClientPtr GetClient(TStringBuf clusterUrl) override
    {
        EXPECT_EQ(clusterUrl, "primary");
        return Client_;
    }

private:
    const IClientPtr Client_;
};

TYTDirectoryLastFileSourcePtr MakeDirectorySource(const IClientPtr& client)
{
    auto parameters = New<TYTDirectoryLastFileSourceParameters>();
    parameters->Path = "//dir";

    auto spec = New<TFileSourceSpec>();
    spec->FileSourceClassName = TypeName<TYTDirectoryLastFileSource>();
    spec->Parameters = ConvertToNode(parameters)->AsMap();

    auto context = New<TFileSourceContext>();
    context->SourceSpec = std::move(spec);
    context->ClientsCache = New<TDirectoryTestClientsCache>(client);
    context->PipelinePath = "//pipeline";
    context->PipelinePath.SetCluster("primary");
    return New<TYTDirectoryLastFileSource>(std::move(context));
}

INodePtr MakeDirectoryListing(const std::vector<std::pair<std::string, EObjectType>>& entries)
{
    auto builder = BuildYsonNodeFluently()
        .BeginList();
    int revision = 1;
    for (const auto& [name, type] : entries) {
        builder
            .Item()
            .BeginAttributes()
            .Item("type")
            .Value(type)
            .Item("id")
            .Value(TObjectId{})
            .Item("revision")
            .Value(TRevision{static_cast<ui64>(revision++)})
            .Item("uncompressed_data_size")
            .Value(100)
            .EndAttributes()
            .Value(name);
    }
    return builder.EndList();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYTDirectoryLastFileSourceTest, SelectsLexicographicallyGreatestDirectFile)
{
    auto client = New<testing::StrictMock<TMockClient>>();
    EXPECT_CALL(*client, ListNode(TYPath("//dir"), testing::_))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeDirectoryListing({
            {"z-directory", EObjectType::MapNode},
            {"a-file", EObjectType::File},
            {"m-file", EObjectType::File},
        })))));
    auto source = MakeDirectorySource(client);

    auto revision = WaitFor(source->Discover()).ValueOrThrow();

    ASSERT_TRUE(revision);
    EXPECT_EQ(revision->Locator->GetChildValueOrThrow<std::string>("basename"), "m-file");
}

TEST(TYTDirectoryLastFileSourceTest, EmptyOrNonFileDirectoryHasNoRevision)
{
    auto client = New<testing::StrictMock<TMockClient>>();
    EXPECT_CALL(*client, ListNode(TYPath("//dir"), testing::_))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeDirectoryListing({
            {"nested", EObjectType::MapNode},
        })))))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeDirectoryListing({})))));
    auto source = MakeDirectorySource(client);

    EXPECT_FALSE(WaitFor(source->Discover()).ValueOrThrow());
    EXPECT_FALSE(WaitFor(source->Discover()).ValueOrThrow());
}

TEST(TYTDirectoryLastFileSourceTest, RejectsChildNameThatEscapesDownloadDirectory)
{
    auto client = New<testing::StrictMock<TMockClient>>();
    EXPECT_CALL(*client, ListNode(TYPath("//dir"), testing::_))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeDirectoryListing({
            {"../../tmp/escaped", EObjectType::File},
        })))));
    auto source = MakeDirectorySource(client);

    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(source->Discover()).ValueOrThrow(),
        "single normal path component");
}

TEST(TYTDirectoryLastFileSourceTest, GreaterInsertionChangesSelection)
{
    auto client = New<testing::StrictMock<TMockClient>>();
    EXPECT_CALL(*client, ListNode(TYPath("//dir"), testing::_))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeDirectoryListing({
            {"001", EObjectType::File},
        })))))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeDirectoryListing({
            {"001", EObjectType::File},
            {"002", EObjectType::File},
        })))));
    auto source = MakeDirectorySource(client);

    auto first = WaitFor(source->Discover()).ValueOrThrow();
    auto second = WaitFor(source->Discover()).ValueOrThrow();

    EXPECT_EQ(first->Locator->GetChildValueOrThrow<std::string>("basename"), "001");
    EXPECT_EQ(second->Locator->GetChildValueOrThrow<std::string>("basename"), "002");
    EXPECT_NE(first->ObjectId, second->ObjectId);
}

TEST(TYTDirectoryLastFileSourceTest, SharesObjectIdFamilyWithYTFileSource)
{
    auto objectId = TObjectId{};
    auto revision = TRevision{42};
    auto file = MakeYTFileSourceRevision(
        TypeName<TYTFileSource>(),
        TRichYPath("<cluster=primary>//dir/file"),
        "primary",
        objectId,
        revision,
        100,
        "file");
    auto directory = MakeYTFileSourceRevision(
        TypeName<TYTDirectoryLastFileSource>(),
        TRichYPath("<cluster=primary>//dir/file"),
        "primary",
        objectId,
        revision,
        100,
        "file");

    EXPECT_EQ(file->ObjectId, directory->ObjectId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
