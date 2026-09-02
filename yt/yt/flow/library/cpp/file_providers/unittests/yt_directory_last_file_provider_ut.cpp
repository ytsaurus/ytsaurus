#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/file_providers/yt_directory_last_file_provider.h>
#include <yt/yt/flow/library/cpp/file_providers/yt_file_provider.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/object_client/helpers.h>
#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NClient::NCache;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using testing::_;

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

TDynamicFileProviderContextPtr MakeDirectoryDynamicContext(
    std::optional<std::string> pinnedFileName = std::nullopt)
{
    auto parameters = New<TYTDirectoryLastFileProviderDynamicParameters>();
    parameters->PinnedFileName = std::move(pinnedFileName);

    auto spec = New<TDynamicFileProviderSpec>();
    spec->Parameters = ConvertToNode(parameters)->AsMap();

    auto context = New<TDynamicFileProviderContext>();
    context->DynamicFileProviderSpec = std::move(spec);
    return context;
}

TYTDirectoryLastFileProviderPtr MakeDirectoryProvider(
    const IClientPtr& client,
    std::optional<std::string> pinnedFileName = std::nullopt)
{
    auto parameters = New<TYTDirectoryLastFileProviderParameters>();
    parameters->Path = "//versions";

    auto spec = New<TFileProviderSpec>();
    spec->FileProviderClassName = TypeName<TYTDirectoryLastFileProvider>();
    spec->Parameters = ConvertToNode(parameters)->AsMap();

    auto context = New<TFileProviderContext>();
    context->ProviderSpec = std::move(spec);
    context->ClientsCache = New<TDirectoryTestClientsCache>(client);
    context->PipelinePath = "//pipeline";
    context->PipelinePath.SetCluster("primary");
    return New<TYTDirectoryLastFileProvider>(
        std::move(context),
        MakeDirectoryDynamicContext(std::move(pinnedFileName)));
}

TObjectId MakeFileId(ui64 counter)
{
    return MakeId(EObjectType::File, TCellTag{1}, counter, 0);
}

TObjectId MakeTableId(ui64 counter)
{
    return MakeId(EObjectType::Table, TCellTag{1}, counter, 0);
}

INodePtr MakeFileNode(TObjectId objectId, TRevision revision, i64 size)
{
    // clang-format off
    return BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("id").Value(objectId)
            .Item("type").Value(EObjectType::File)
            .Item("revision").Value(revision)
            .Item("uncompressed_data_size").Value(size)
        .EndAttributes()
        .Entity();
    // clang-format on
}

INodePtr MakeTableNode(TObjectId objectId, TRevision contentRevision)
{
    // clang-format off
    return BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("id").Value(objectId)
            .Item("type").Value(EObjectType::Table)
            .Item("dynamic").Value(false)
            .Item("content_revision").Value(contentRevision)
            .Item("schema").Value(GetYTFileProviderBlobTableSchema())
        .EndAttributes()
        .Entity();
    // clang-format on
}

INodePtr MakeDirectoryListing(const std::vector<std::pair<std::string, EObjectType>>& entries)
{
    // clang-format off
    auto builder = BuildYsonNodeFluently()
        .BeginList();
    for (const auto& [name, type] : entries) {
        builder
            .Item()
            .BeginAttributes()
                .Item("type").Value(type)
            .EndAttributes()
            .Value(name);
    }
    return builder.EndList();
    // clang-format on
}

void ExpectSnapshotTransaction(
    TMockClient* client,
    const TYPath& lockPath,
    TObjectId objectId,
    const INodePtr& node)
{
    auto transaction = New<testing::StrictMock<TMockTransaction>>();
    EXPECT_CALL(*client, StartTransaction(ETransactionType::Master, _))
        .WillOnce(testing::Return(MakeFuture<ITransactionPtr>(transaction)));
    TLockNodeResult lockResult;
    lockResult.NodeId = objectId;
    EXPECT_CALL(*transaction, LockNode(lockPath, ELockMode::Snapshot, _))
        .WillOnce(testing::Return(MakeFuture(lockResult)));
    EXPECT_CALL(*transaction, GetNode(TYPath(Format("#%v&", objectId)), _))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(node))));
    EXPECT_CALL(*transaction, Abort(_))
        .WillOnce(testing::Return(MakeFuture<void>(TError())));
}

void ExpectDiscovery(
    TMockClient* client,
    const INodePtr& listing,
    const TYPath& selectedPath,
    TObjectId objectId,
    const INodePtr& node)
{
    EXPECT_CALL(*client, ListNode(TYPath("//versions"), _))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(listing))));
    ExpectSnapshotTransaction(client, selectedPath, objectId, node);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYTDirectoryLastFileProviderTest, SelectsLexicographicallyGreatestBlobTableChild)
{
    auto tableId = MakeTableId(1);
    auto client = New<testing::StrictMock<TMockClient>>();
    ExpectDiscovery(
        client.Get(),
        MakeDirectoryListing({
            {"001", EObjectType::Table},
            {"002", EObjectType::Table},
            {"zzz-directory", EObjectType::MapNode},
        }),
        "//versions/002",
        tableId,
        MakeTableNode(tableId, TRevision{2}));
    auto provider = MakeDirectoryProvider(client);

    auto revision = WaitFor(provider->Discover()).ValueOrThrow();

    ASSERT_TRUE(revision);
    EXPECT_TRUE(revision->ObjectId.Underlying().starts_with("yt_blob_table:v1:"));
    EXPECT_EQ(
        revision->Locator->GetChildValueOrThrow<EYTFileProviderObjectKind>("object_kind"),
        EYTFileProviderObjectKind::BlobTable);
}

TEST(TYTDirectoryLastFileProviderTest, AlsoSelectsCypressFileChildren)
{
    auto fileId = MakeFileId(2);
    auto client = New<testing::StrictMock<TMockClient>>();
    ExpectDiscovery(
        client.Get(),
        MakeDirectoryListing({
            {"001", EObjectType::Table},
            {"002", EObjectType::File},
        }),
        "//versions/002",
        fileId,
        MakeFileNode(fileId, TRevision{3}, 10));
    auto provider = MakeDirectoryProvider(client);

    auto revision = WaitFor(provider->Discover()).ValueOrThrow();

    ASSERT_TRUE(revision);
    EXPECT_TRUE(revision->ObjectId.Underlying().starts_with("yt_file:v1:"));
    EXPECT_FALSE(revision->Locator->FindChild("basename"));
}

TEST(TYTDirectoryLastFileProviderTest, DynamicPinSelectsExactChildAndCanBeCleared)
{
    testing::InSequence sequence;
    auto firstTableId = MakeTableId(3);
    auto secondTableId = MakeTableId(4);
    auto listing = MakeDirectoryListing({
        {"001", EObjectType::Table},
        {"002", EObjectType::Table},
    });
    auto client = New<testing::StrictMock<TMockClient>>();
    ExpectDiscovery(
        client.Get(),
        listing,
        "//versions/002",
        secondTableId,
        MakeTableNode(secondTableId, TRevision{2}));
    ExpectDiscovery(
        client.Get(),
        listing,
        "//versions/001",
        firstTableId,
        MakeTableNode(firstTableId, TRevision{1}));
    ExpectDiscovery(
        client.Get(),
        listing,
        "//versions/002",
        secondTableId,
        MakeTableNode(secondTableId, TRevision{2}));
    auto provider = MakeDirectoryProvider(client);

    EXPECT_EQ(
        WaitFor(provider->Discover()).ValueOrThrow()->Locator->GetChildValueOrThrow<TObjectId>("object_id"),
        secondTableId);

    provider->Reconfigure(MakeDirectoryDynamicContext("001"));
    EXPECT_EQ(
        WaitFor(provider->Discover()).ValueOrThrow()->Locator->GetChildValueOrThrow<TObjectId>("object_id"),
        firstTableId);

    provider->Reconfigure(MakeDirectoryDynamicContext());
    EXPECT_EQ(
        WaitFor(provider->Discover()).ValueOrThrow()->Locator->GetChildValueOrThrow<TObjectId>("object_id"),
        secondTableId);
}

TEST(TYTDirectoryLastFileProviderTest, UnsupportedLinkDoesNotMaskGreatestSupportedChild)
{
    auto tableId = MakeTableId(5);
    auto client = New<testing::StrictMock<TMockClient>>();
    ExpectDiscovery(
        client.Get(),
        MakeDirectoryListing({
            {"001", EObjectType::Table},
            {"zzz-link", EObjectType::Link},
        }),
        "//versions/001",
        tableId,
        MakeTableNode(tableId, TRevision{1}));
    auto provider = MakeDirectoryProvider(client);

    auto revision = WaitFor(provider->Discover()).ValueOrThrow();

    ASSERT_TRUE(revision);
    EXPECT_EQ(
        revision->Locator->GetChildValueOrThrow<EYTFileProviderObjectKind>("object_kind"),
        EYTFileProviderObjectKind::BlobTable);
}

TEST(TYTDirectoryLastFileProviderTest, EmptyOrUnsupportedDirectoryHasNoRevision)
{
    auto client = New<testing::StrictMock<TMockClient>>();
    EXPECT_CALL(*client, ListNode(TYPath("//versions"), _))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeDirectoryListing({
            {"nested", EObjectType::MapNode},
        })))))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeDirectoryListing({})))));
    auto provider = MakeDirectoryProvider(client);

    EXPECT_FALSE(WaitFor(provider->Discover()).ValueOrThrow());
    EXPECT_FALSE(WaitFor(provider->Discover()).ValueOrThrow());
}

TEST(TYTDirectoryLastFileProviderTest, DynamicPinMustNameExistingSupportedChild)
{
    auto client = New<testing::StrictMock<TMockClient>>();
    EXPECT_CALL(*client, ListNode(TYPath("//versions"), _))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeDirectoryListing({
            {"001", EObjectType::Table},
        })))));
    auto missing = MakeDirectoryProvider(client, "missing");
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(missing->Discover()).ValueOrThrow(),
        "does not exist");

    EXPECT_CALL(*client, ListNode(TYPath("//versions"), _))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(MakeDirectoryListing({
            {"nested", EObjectType::MapNode},
        })))));
    auto unsupported = MakeDirectoryProvider(client, "nested");
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(unsupported->Discover()).ValueOrThrow(),
        "must be a Cypress file or a BLOB table");

    auto invalidParameters = New<TYTDirectoryLastFileProviderDynamicParameters>();
    EXPECT_THROW_WITH_SUBSTRING(
        invalidParameters->Load(ConvertTo<IMapNodePtr>(TYsonString(TStringBuf(
            R"({pinned_file_name="../bad";})")))),
        "single normal path component");
}

TEST(TYTDirectoryLastFileProviderTest, SharesBlobTableObjectIdFamilyWithYTFileProvider)
{
    auto objectId = MakeTableId(6);
    auto revision = TRevision{42};
    auto file = MakeYTBlobTableFileProviderRevision(
        TypeName<TYTFileProvider>(),
        TRichYPath("<cluster=primary>//versions/001"),
        "primary",
        objectId,
        revision);
    auto directory = MakeYTBlobTableFileProviderRevision(
        TypeName<TYTDirectoryLastFileProvider>(),
        TRichYPath("<cluster=primary>//versions/001"),
        "primary",
        objectId,
        revision);

    EXPECT_EQ(file->ObjectId, directory->ObjectId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
