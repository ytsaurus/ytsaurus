#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/resources/file/yt_file_source.h>

#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/ytree/fluent.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>

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

DECLARE_REFCOUNTED_CLASS(TTestFileReader);

class TTestFileReader
    : public IFileReader
{
public:
    TTestFileReader(TObjectId id, TRevision revision, std::vector<std::string> blocks)
        : Id_(id)
        , Revision_(revision)
        , Blocks_(std::move(blocks))
    { }

    TFuture<TSharedRef> Read() override
    {
        if (Index_ == Blocks_.size()) {
            return MakeFuture(TSharedRef());
        }
        return MakeFuture(TSharedRef::FromString(Blocks_[Index_++]));
    }

    TObjectId GetId() const override
    {
        return Id_;
    }

    TRevision GetRevision() const override
    {
        return Revision_;
    }

private:
    const TObjectId Id_;
    const TRevision Revision_;
    const std::vector<std::string> Blocks_;
    size_t Index_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TTestFileReader);

class TTestClientsCache
    : public IClientsCache
{
public:
    TTestClientsCache(std::string cluster, IClientPtr client)
        : Cluster_(std::move(cluster))
        , Client_(std::move(client))
    { }

    IClientPtr GetClient(TStringBuf clusterUrl) override
    {
        EXPECT_EQ(clusterUrl, Cluster_);
        return Client_;
    }

private:
    const std::string Cluster_;
    const IClientPtr Client_;
};

TYTFileSourcePtr MakeSource(
    const TRichYPath& path,
    const IClientPtr& client,
    TStringBuf pipelineCluster = "primary")
{
    auto parameters = New<TYTFileSourceParameters>();
    parameters->Path = path;

    auto spec = New<TFileSourceSpec>();
    spec->FileSourceClassName = TypeName<TYTFileSource>();
    spec->Parameters = ConvertToNode(parameters)->AsMap();

    auto context = New<TFileSourceContext>();
    context->SourceSpec = std::move(spec);
    context->ClientsCache = New<TTestClientsCache>(std::string(pipelineCluster), client);
    context->PipelinePath = "//pipeline";
    context->PipelinePath.SetCluster(std::string(pipelineCluster));
    return New<TYTFileSource>(std::move(context));
}

INodePtr MakeNode(EObjectType type, TObjectId id, TRevision revision, i64 size = 100)
{
    return BuildYsonNodeFluently()
        .BeginAttributes()
        .Item("type")
        .Value(type)
        .Item("id")
        .Value(id)
        .Item("revision")
        .Value(revision)
        .Item("uncompressed_data_size")
        .Value(size)
        .EndAttributes()
        .Entity();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYTFileSourceTest, DiscoversFileAndChangesIdentityWithRevision)
{
    TObjectId objectId{};
    auto client = New<testing::StrictMock<TMockClient>>();
    EXPECT_CALL(*client, GetNode(TYPath("//dir/file&"), testing::_))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeNode(EObjectType::File, objectId, TRevision{11})))))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeNode(EObjectType::File, objectId, TRevision{12})))));
    auto source = MakeSource("//dir/file", client);

    auto first = WaitFor(source->Discover()).ValueOrThrow();
    auto second = WaitFor(source->Discover()).ValueOrThrow();

    EXPECT_NE(first->ObjectId, second->ObjectId);
    EXPECT_TRUE(first->ObjectId.Underlying().starts_with("yt_file:v1:"));
    EXPECT_EQ(first->Size, 100);
    EXPECT_EQ(first->Locator->GetChildValueOrThrow<std::string>("cluster"), "primary");
    EXPECT_EQ(first->Locator->GetChildValueOrThrow<std::string>("basename"), "file");
}

TEST(TYTFileSourceTest, RejectsNonFile)
{
    auto client = New<testing::StrictMock<TMockClient>>();
    EXPECT_CALL(*client, GetNode(TYPath("//dir/node&"), testing::_))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeNode(EObjectType::MapNode, {}, TRevision{1})))));
    auto source = MakeSource("//dir/node", client);

    EXPECT_THROW_WITH_SUBSTRING(WaitFor(source->Discover()).ValueOrThrow(), "is not a file");
}

TEST(TYTFileSourceTest, StreamsBlocksAndRejectsRevisionMismatch)
{
    TObjectId objectId{};
    auto client = New<testing::StrictMock<TMockClient>>();
    auto source = MakeSource("<cluster=primary>//dir/file", client);
    auto revision = MakeYTFileSourceRevision(
        TypeName<TYTFileSource>(),
        TRichYPath("<cluster=primary>//dir/file"),
        "primary",
        objectId,
        TRevision{42},
        6,
        "file");

    EXPECT_CALL(*client, CreateFileReader(TYPath(Format("#%v", objectId)), testing::_))
        .WillOnce(testing::Return(MakeFuture<IFileReaderPtr>(
            New<TTestFileReader>(objectId, TRevision{42}, std::vector<std::string>{"one", "two"}))));
    TTempDir root;
    WaitFor(source->Download(revision, root.Name())).ThrowOnError();
    EXPECT_EQ(TFileInput((TFsPath(root.Name()) / "file").GetPath()).ReadAll(), "onetwo");

    EXPECT_CALL(*client, CreateFileReader(TYPath(Format("#%v", objectId)), testing::_))
        .WillOnce(testing::Return(MakeFuture<IFileReaderPtr>(
            New<TTestFileReader>(objectId, TRevision{43}, std::vector<std::string>{"new"}))));
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(source->Download(revision, root.Name())).ThrowOnError(),
        "changed between discovery and download");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
