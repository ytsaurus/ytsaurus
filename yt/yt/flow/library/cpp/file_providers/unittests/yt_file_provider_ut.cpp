#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/file_providers/yt_file_provider.h>

#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/object_client/helpers.h>
#include <yt/yt/client/table_client/blob_reader.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/table_reader.h>
#include <yt/yt/client/unittests/mock/transaction.h>

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

TYTFileProviderPtr MakeProvider(
    const TRichYPath& path,
    const IClientPtr& client,
    TStringBuf pipelineCluster = "primary")
{
    auto parameters = New<TYTFileProviderParameters>();
    parameters->Path = path;

    auto spec = New<TFileProviderSpec>();
    spec->FileProviderClassName = TypeName<TYTFileProvider>();
    spec->Parameters = ConvertToNode(parameters)->AsMap();

    auto context = New<TFileProviderContext>();
    context->ProviderSpec = std::move(spec);
    context->ClientsCache = New<TTestClientsCache>(std::string(pipelineCluster), client);
    context->PipelinePath = "//pipeline";
    context->PipelinePath.SetCluster(std::string(pipelineCluster));

    auto dynamicSpec = New<TDynamicFileProviderSpec>();
    dynamicSpec->Parameters = GetEphemeralNodeFactory()->CreateMap();
    auto dynamicContext = New<TDynamicFileProviderContext>();
    dynamicContext->DynamicFileProviderSpec = std::move(dynamicSpec);
    return New<TYTFileProvider>(std::move(context), std::move(dynamicContext));
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

INodePtr MakeTableNode(
    TObjectId objectId,
    TRevision contentRevision,
    bool dynamic = false,
    TTableSchemaPtr schema = GetYTFileProviderBlobTableSchema())
{
    // clang-format off
    return BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("id").Value(objectId)
            .Item("type").Value(EObjectType::Table)
            .Item("dynamic").Value(dynamic)
            .Item("content_revision").Value(contentRevision)
            .Item("schema").Value(schema)
        .EndAttributes()
        .Entity();
    // clang-format on
}

INodePtr MakeUnsupportedNode(TObjectId objectId)
{
    // clang-format off
    return BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("id").Value(objectId)
            .Item("type").Value(EObjectType::MapNode)
        .EndAttributes()
        .Entity();
    // clang-format on
}

IUnversionedRowBatchPtr MakeRowBatch(
    const std::vector<std::tuple<std::string, i64, std::string>>& rows)
{
    TUnversionedRowsBuilder builder;
    for (const auto& [fileName, partIndex, data] : rows) {
        builder.AddRow(fileName, partIndex, data);
    }
    return CreateBatchFromUnversionedRows(builder.Build());
}

ITableReaderPtr MakeTableReader(
    const std::vector<std::tuple<std::string, i64, std::string>>& rows,
    bool expectEof = true)
{
    auto reader = New<testing::StrictMock<TMockTableReader>>(GetYTFileProviderBlobTableSchema());
    if (!rows.empty() && expectEof) {
        EXPECT_CALL(*reader, Read(_))
            .WillOnce(testing::Return(MakeRowBatch(rows)))
            .WillOnce(testing::Return(nullptr));
    } else if (!rows.empty()) {
        EXPECT_CALL(*reader, Read(_))
            .WillOnce(testing::Return(MakeRowBatch(rows)));
    } else {
        EXPECT_CALL(*reader, Read(_))
            .WillOnce(testing::Return(nullptr));
    }
    return reader;
}

void ExpectSnapshotTransaction(
    TMockClient* client,
    const TYPath& lockPath,
    TObjectId objectId,
    const INodePtr& node,
    const ITableReaderPtr& reader = nullptr)
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
    if (reader) {
        EXPECT_CALL(*transaction, CreateTableReader(_, _))
            .WillOnce(testing::Return(MakeFuture<ITableReaderPtr>(reader)));
    }
    EXPECT_CALL(*transaction, Abort(_))
        .WillOnce(testing::Return(MakeFuture<void>(TError())));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYTFileProviderTest, DiscoversCypressFileAndDownloadsExactRevision)
{
    auto objectId = MakeFileId(1);
    auto client = New<testing::StrictMock<TMockClient>>();
    ExpectSnapshotTransaction(
        client.Get(),
        "//dir/file",
        objectId,
        MakeFileNode(objectId, TRevision{11}, 6));
    auto provider = MakeProvider("//dir/file", client);

    auto revision = WaitFor(provider->Discover()).ValueOrThrow();

    EXPECT_EQ(
        revision->ObjectId.Underlying(),
        Format("yt_file:v1:primary:%v:11", objectId));
    EXPECT_EQ(revision->Size, 6);
    EXPECT_EQ(
        revision->Locator->GetChildValueOrThrow<EYTFileProviderObjectKind>("object_kind"),
        EYTFileProviderObjectKind::CypressFile);
    EXPECT_FALSE(revision->Locator->FindChild("basename"));

    EXPECT_CALL(*client, CreateFileReader(TYPath(Format("#%v", objectId)), _))
        .WillOnce(testing::Return(MakeFuture<IFileReaderPtr>(
            New<TTestFileReader>(objectId, TRevision{11}, std::vector<std::string>{"one", "two"}))));
    TTempDir root;
    WaitFor(provider->Download(revision, root.Name())).ThrowOnError();
    EXPECT_EQ(TFileInput((TFsPath(root.Name()) / "data").GetPath()).ReadAll(), "onetwo");

    EXPECT_CALL(*client, CreateFileReader(TYPath(Format("#%v", objectId)), _))
        .WillOnce(testing::Return(MakeFuture<IFileReaderPtr>(
            New<TTestFileReader>(objectId, TRevision{12}, std::vector<std::string>{"new"}))));
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(provider->Download(revision, root.Name())).ThrowOnError(),
        "changed between discovery and download");
}

TEST(TYTFileProviderTest, DiscoversBlobTableThroughLinkAndDownloadsAllFiles)
{
    auto objectId = MakeTableId(2);
    auto client = New<testing::StrictMock<TMockClient>>();
    ExpectSnapshotTransaction(
        client.Get(),
        "//current",
        objectId,
        MakeTableNode(objectId, TRevision{42}));
    auto provider = MakeProvider("<cluster=primary>//current", client);

    auto revision = WaitFor(provider->Discover()).ValueOrThrow();

    EXPECT_TRUE(revision->ObjectId.Underlying().starts_with("yt_blob_table:v1:"));
    EXPECT_FALSE(revision->Size);
    EXPECT_EQ(
        revision->Locator->GetChildValueOrThrow<EYTFileProviderObjectKind>("object_kind"),
        EYTFileProviderObjectKind::BlobTable);

    ExpectSnapshotTransaction(
        client.Get(),
        Format("#%v", objectId),
        objectId,
        MakeTableNode(objectId, TRevision{42}),
        MakeTableReader({
            {"a", 0, "left-"},
            {"a", 1, "part"},
            {"b", 0, "right"},
        }));
    TTempDir root;
    WaitFor(provider->Download(revision, root.Name())).ThrowOnError();
    EXPECT_EQ(TFileInput((TFsPath(root.Name()) / "a").GetPath()).ReadAll(), "left-part");
    EXPECT_EQ(TFileInput((TFsPath(root.Name()) / "b").GetPath()).ReadAll(), "right");
}

TEST(TYTFileProviderTest, EmptyBlobTableMaterializesAnEmptyDirectory)
{
    auto objectId = MakeTableId(3);
    auto client = New<testing::StrictMock<TMockClient>>();
    ExpectSnapshotTransaction(
        client.Get(),
        "//empty",
        objectId,
        MakeTableNode(objectId, TRevision{1}));
    auto provider = MakeProvider("//empty", client);

    auto revision = WaitFor(provider->Discover()).ValueOrThrow();
    ASSERT_TRUE(revision);

    ExpectSnapshotTransaction(
        client.Get(),
        Format("#%v", objectId),
        objectId,
        MakeTableNode(objectId, TRevision{1}),
        MakeTableReader({}));
    TTempDir root;
    WaitFor(provider->Download(revision, root.Name())).ThrowOnError();
    TVector<TString> names;
    TFsPath(root.Name()).ListNames(names);
    EXPECT_TRUE(names.empty());
}

TEST(TYTFileProviderTest, RejectsUnsupportedDynamicOrIncompatibleNode)
{
    auto client = New<testing::StrictMock<TMockClient>>();

    auto mapNodeId = MakeId(EObjectType::MapNode, TCellTag{1}, 4, 0);
    ExpectSnapshotTransaction(client.Get(), "//map", mapNodeId, MakeUnsupportedNode(mapNodeId));
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(MakeProvider("//map", client)->Discover()).ValueOrThrow(),
        "must resolve to a Cypress file or a BLOB table");

    auto dynamicTableId = MakeTableId(5);
    ExpectSnapshotTransaction(
        client.Get(),
        "//dynamic",
        dynamicTableId,
        MakeTableNode(dynamicTableId, TRevision{1}, /*dynamic*/ true));
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(MakeProvider("//dynamic", client)->Discover()).ValueOrThrow(),
        "must be static");

    TBlobTableSchema wrongSchema;
    wrongSchema.BlobIdColumns.emplace_back("wrong", EValueType::String);
    auto wrongTableId = MakeTableId(6);
    ExpectSnapshotTransaction(
        client.Get(),
        "//wrong",
        wrongTableId,
        MakeTableNode(
            wrongTableId,
            TRevision{1},
            /*dynamic*/ false,
            wrongSchema.ToTableSchema()));
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(MakeProvider("//wrong", client)->Discover()).ValueOrThrow(),
        "incompatible schema");
}

TEST(TYTFileProviderTest, RejectsChangedBlobTableBeforeReadingRows)
{
    auto objectId = MakeTableId(7);
    auto client = New<testing::StrictMock<TMockClient>>();
    auto provider = MakeProvider("//blob", client);
    auto revision = MakeYTBlobTableFileProviderRevision(
        TypeName<TYTFileProvider>(),
        TRichYPath("//blob"),
        "primary",
        objectId,
        TRevision{42});
    ExpectSnapshotTransaction(
        client.Get(),
        Format("#%v", objectId),
        objectId,
        MakeTableNode(objectId, TRevision{43}));

    TTempDir root;
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(provider->Download(revision, root.Name())).ThrowOnError(),
        "changed between discovery and download");
}

TEST(TYTFileProviderTest, RejectsInvalidBlobTableRows)
{
    auto objectId = MakeTableId(8);
    auto client = New<testing::StrictMock<TMockClient>>();
    auto provider = MakeProvider("//blob", client);
    auto revision = MakeYTBlobTableFileProviderRevision(
        TypeName<TYTFileProvider>(),
        TRichYPath("//blob"),
        "primary",
        objectId,
        TRevision{1});
    ExpectSnapshotTransaction(
        client.Get(),
        Format("#%v", objectId),
        objectId,
        MakeTableNode(objectId, TRevision{1}),
        MakeTableReader(
            {
                {"file", 0, "first"},
                {"file", 2, "third"},
            },
            /*expectEof*/ false));

    TTempDir root;
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(provider->Download(revision, root.Name())).ThrowOnError(),
        "must be consecutive");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
