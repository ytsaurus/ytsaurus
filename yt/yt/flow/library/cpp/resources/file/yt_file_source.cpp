#include "yt_file_source.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/cache/cache.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/folder/path.h>
#include <util/stream/file.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::string ResolveCluster(
    const TRichYPath& path,
    const TFileSourceContextPtr& context)
{
    if (path.GetCluster()) {
        return *path.GetCluster();
    }
    THROW_ERROR_EXCEPTION_UNLESS(
        context->PipelinePath.GetCluster(),
        "Pipeline path must have a cluster to resolve YT file source path %v",
        path);
    return *context->PipelinePath.GetCluster();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TYTFileSourceLocator::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster", &TThis::Cluster);
    registrar.Parameter("object_path", &TThis::ObjectPath);
    registrar.Parameter("object_id", &TThis::ObjectId);
    registrar.Parameter("revision", &TThis::Revision);
    registrar.Parameter("basename", &TThis::Basename);
}

TFileSourceRevisionPtr MakeYTFileSourceRevision(
    TStringBuf fileSourceClassName,
    const TRichYPath& originalPath,
    const std::string& cluster,
    TObjectId objectId,
    TRevision revision,
    i64 size,
    const std::string& basename)
{
    ValidateFileSourceBasename(basename);
    THROW_ERROR_EXCEPTION_UNLESS(
        size >= 0,
        "YT file source size must be nonnegative");

    auto locator = New<TYTFileSourceLocator>();
    locator->Cluster = cluster;
    locator->ObjectPath = Format("#%v", objectId);
    locator->ObjectId = objectId;
    locator->Revision = revision;
    locator->Basename = basename;

    auto result = New<TFileSourceRevision>();
    result->FileSourceClassName = std::string(fileSourceClassName);
    result->ObjectId = NFileStorage::TFileStorageObjectId(
        Format("yt_file:v1:%v:%v:%v:%v", cluster, objectId, revision, basename));
    result->DisplayVersion = Format("%v@%v", originalPath, revision);
    result->Size = size;
    result->Locator = ConvertToNode(locator)->AsMap();
    return result;
}

TFuture<void> DownloadYTFile(
    const TFileSourceContextPtr& context,
    const TFileSourceRevisionPtr& revision,
    const std::string& stagingDirectory)
{
    auto locator = ConvertTo<TYTFileSourceLocatorPtr>(revision->Locator);
    ValidateFileSourceBasename(locator->Basename);
    auto client = context->ClientsCache->GetClient(locator->Cluster);
    auto reader = WaitFor(client->CreateFileReader(locator->ObjectPath)).ValueOrThrow();
    THROW_ERROR_EXCEPTION_UNLESS(
        reader->GetId() == locator->ObjectId && reader->GetRevision() == locator->Revision,
        "YT file changed between discovery and download")
        .With("expected_object_id", locator->ObjectId)
        .With("actual_object_id", reader->GetId())
        .With("expected_revision", locator->Revision)
        .With("actual_revision", reader->GetRevision());

    TFileOutput output((TFsPath(stagingDirectory) / locator->Basename).GetPath());
    while (auto block = WaitFor(reader->Read()).ValueOrThrow()) {
        output.Write(block.Begin(), block.Size());
    }
    output.Finish();
    return MakeFuture<void>(TError());
}

////////////////////////////////////////////////////////////////////////////////

void TYTFileSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
}

TFuture<TFileSourceRevisionPtr> TYTFileSource::Discover()
{
    auto path = GetParameters()->Path;
    auto cluster = ResolveCluster(path, GetContext());
    auto client = GetContext()->ClientsCache->GetClient(cluster);

    TGetNodeOptions options;
    options.Attributes = {"id", "type", "revision", "uncompressed_data_size"};
    return client->GetNode(path.GetPath() + "&", options)
        .Apply(BIND([path = std::move(path), cluster = std::move(cluster)] (const NYson::TYsonString& nodeYson) {
            auto node = ConvertToNode(nodeYson);
            THROW_ERROR_EXCEPTION_UNLESS(
                node->Attributes().Get<EObjectType>("type") == EObjectType::File,
                "YT file source path %v is not a file",
                path);

            auto basename = std::string(TFsPath(path.GetPath()).Basename());
            THROW_ERROR_EXCEPTION_UNLESS(!basename.empty(), "YT file source path %v has no basename", path);
            return MakeYTFileSourceRevision(
                TypeName<TYTFileSource>(),
                path,
                cluster,
                node->Attributes().Get<TObjectId>("id"),
                node->Attributes().Get<TRevision>("revision"),
                node->Attributes().Get<i64>("uncompressed_data_size"),
                basename);
        }));
}

TFuture<void> TYTFileSource::Download(
    const TFileSourceRevisionPtr& revision,
    const std::string& stagingDirectory)
{
    return DownloadYTFile(GetContext(), revision, stagingDirectory);
}

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_FILE_SOURCE(TYTFileSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
