#include "files.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/generic/buffer.h>
#include <util/generic/guid.h>
#include <util/generic/size_literals.h>

#include <util/stream/file.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr size_t UploadBlockSize = 16_MB;

//! Create-options for an uploaded cache file. On a local (test) YT the default replication factor of
//! 3 triples the on-disk footprint of the (debug-fat, ~GB) job binary and exhausts the small node
//! disks, collapsing the cluster mid-test; pin it to 1 there. Mirrors the mapreduce client's
//! IsLocalMode() file-cache optimization. Off a local cluster the cluster default is kept.
NApi::TCreateNodeOptions MakeCacheFileOptions(const NApi::IClientPtr& client)
{
    NApi::TCreateNodeOptions options;
    options.Recursive = true;
    if (IsLocalModeCluster(client)) {
        options.Attributes = CreateEphemeralAttributes();
        options.Attributes->Set("replication_factor", 1);
    }
    return options;
}

//! Streams `localPath`'s content block by block into an opened file writer.
void StreamLocalFile(const NApi::IFileWriterPtr& writer, const std::string& localPath)
{
    TFileInput input{TString(localPath)};
    TBuffer buffer(UploadBlockSize);
    while (size_t read = input.Read(buffer.Data(), UploadBlockSize)) {
        WaitFor(writer->Write(TSharedRef::FromString(TString(buffer.Data(), read)))).ThrowOnError();
    }
}

//! Returns the cache path of a blob with the given md5, uploading it via `fill` when absent.
//! `fill(writer)` must write the file content into an opened file writer. PutFileToCache
//! deduplicates by md5, so concurrent uploads from other operations are safe.
template <class TFill>
NYPath::TYPath EnsureInCache(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& cacheDir,
    const TString& md5,
    TFill&& fill)
{
    NApi::TCreateNodeOptions dirOptions;
    dirOptions.Recursive = true;
    dirOptions.IgnoreExisting = true;
    WaitFor(client->CreateNode(cacheDir, NObjectClient::EObjectType::MapNode, dirOptions)).ThrowOnError();

    NApi::TGetFileFromCacheOptions getOptions;
    getOptions.CachePath = cacheDir;
    auto cached = WaitFor(client->GetFileFromCache(md5, getOptions)).ValueOrThrow();
    if (!cached.Path.empty()) {
        return cached.Path;
    }

    // PutFileToCache copies the node into the cache, so this upload node is a throwaway. Stage it in
    // the parent temp dir (under //tmp, with its own TTL), not inside the cache — which has no
    // top-level cleanup of its own — and drop it once it has been copied in.
    auto uploadDir = cacheDir.substr(0, cacheDir.rfind('/'));
    auto tempPath = Format("%v/upload_%v", uploadDir, TGuid::Create());
    auto fileOptions = MakeCacheFileOptions(client);
    WaitFor(client->CreateNode(tempPath, NObjectClient::EObjectType::File, fileOptions)).ThrowOnError();

    NApi::TFileWriterOptions writerOptions;
    writerOptions.ComputeMD5 = true;
    auto writer = client->CreateFileWriter(NYPath::TRichYPath(tempPath), writerOptions);
    WaitFor(writer->Open()).ThrowOnError();
    fill(writer);
    WaitFor(writer->Close()).ThrowOnError();

    NApi::TPutFileToCacheOptions putOptions;
    putOptions.CachePath = cacheDir;
    auto put = WaitFor(client->PutFileToCache(tempPath, md5, putOptions)).ValueOrThrow();

    NApi::TRemoveNodeOptions removeOptions;
    removeOptions.Force = true;
    WaitFor(client->RemoveNode(tempPath, removeOptions)).ThrowOnError();
    return put.Path;
}

//! Ensures the Cypress file `srcPath` (on `srcClient`) is present in `destClient`'s cache, streaming
//! it through the cache by its `@md5` only when the cache lacks it. Returns the dest cache path.
NYPath::TYPath EnsureCypressFileInCache(
    const NApi::IClientPtr& srcClient,
    const NYPath::TYPath& srcPath,
    const NApi::IClientPtr& destClient,
    const NYPath::TYPath& cacheDir)
{
    // Cache-uploaded files carry @md5, but an arbitrary user file may not — then compute it with an
    // extra read pass.
    TString md5;
    auto md5OrError = WaitFor(srcClient->GetNode(srcPath + "/@md5"));
    if (md5OrError.IsOK()) {
        md5 = ConvertTo<TString>(md5OrError.Value());
    } else {
        MD5 hasher;
        auto reader = WaitFor(srcClient->CreateFileReader(srcPath)).ValueOrThrow();
        while (auto block = WaitFor(reader->Read()).ValueOrThrow()) {
            hasher.Update(block.Begin(), block.Size());
        }
        char buffer[33];
        md5 = hasher.End(buffer);
    }
    return EnsureInCache(destClient, cacheDir, md5, [&] (const NApi::IFileWriterPtr& writer) {
        auto reader = WaitFor(srcClient->CreateFileReader(srcPath)).ValueOrThrow();
        while (auto block = WaitFor(reader->Read()).ValueOrThrow()) {
            WaitFor(writer->Write(block)).ThrowOnError();
        }
    });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsLocalModeCluster(const NApi::IClientPtr& client)
{
    NApi::TNodeExistsOptions existsOptions;
    existsOptions.ReadFrom = NApi::EMasterChannelKind::Cache;
    return WaitFor(client->NodeExists("//sys/@local_mode_fqdn", existsOptions)).ValueOrThrow();
}

NYPath::TYPath GetVanillaFilesDir(const NYPath::TYPath& pipelinePath)
{
    return Format("%v/vanilla/files", pipelinePath);
}

TString ComputeLocalFileMD5(const std::string& localPath)
{
    return MD5::File(TString(localPath));
}

NYPath::TYPath EnsureFileInCache(
    const NApi::IClientPtr& client,
    const std::string& localPath,
    const TString& md5,
    const NYPath::TYPath& cacheDir)
{
    return EnsureInCache(client, cacheDir, md5, [&] (const NApi::IFileWriterPtr& writer) {
        StreamLocalFile(writer, localPath);
    });
}

void PruneVanillaFiles(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& filesDir,
    const THashSet<std::string>& keepNames)
{
    if (!WaitFor(client->NodeExists(filesDir)).ValueOrThrow()) {
        return;
    }
    auto names = ConvertTo<std::vector<std::string>>(WaitFor(client->ListNode(filesDir)).ValueOrThrow());
    std::vector<TFuture<void>> removals;
    for (const auto& name : names) {
        if (!keepNames.contains(name)) {
            removals.push_back(client->RemoveNode(Format("%v/%v", filesDir, name)));
        }
    }
    WaitFor(AllSucceeded(std::move(removals))).ThrowOnError();
}

void RebaseVanillaSpecFiles(
    const IMapNodePtr& spec,
    const NApi::IClientPtr& srcClient,
    const NApi::IClientPtr& destClient,
    const NYPath::TYPath& destCacheDir)
{
    auto tasks = spec->FindChild("tasks");
    if (!tasks) {
        return;
    }

    for (const auto& [taskName, task] : tasks->AsMap()->GetChildren()) {
        auto filePaths = task->AsMap()->FindChild("file_paths");
        if (!filePaths) {
            continue;
        }
        auto rebased = GetEphemeralNodeFactory()->CreateList();
        for (const auto& entry : filePaths->AsList()->GetChildren()) {
            auto srcPath = TString(entry->AsString()->GetValue());
            auto destPath = EnsureCypressFileInCache(srcClient, srcPath, destClient, destCacheDir);

            auto rebasedEntry = ConvertToNode(destPath);
            rebasedEntry->MutableAttributes()->MergeFrom(entry->Attributes());
            rebased->AddChild(rebasedEntry);
        }
        task->AsMap()->RemoveChild("file_paths");
        task->AsMap()->AddChild("file_paths", rebased);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
