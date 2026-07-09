#pragma once

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/ytree/public.h>

#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>

#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! The shared content-addressed file cache (the YT wrapper's default), per cluster. Uploads to any
//! cluster go through it; the durable per-pipeline copy is a CopyNode from here.
inline constexpr TStringBuf VanillaFileCachePath = "//tmp/yt_wrapper/file_storage/new_cache";

//! The directory under a pipeline node that holds the durable copy of the job resources (binary,
//! configs, files) — the reanimate source both launch and reanimate bring the operation up from.
NYPath::TYPath GetVanillaFilesDir(const NYPath::TYPath& pipelinePath);

//! md5 of a local file — the key for EnsureFileInCache. The caller keeps the result when uploading
//! one file to several clusters, so a multi-gigabyte binary is read once.
TString ComputeLocalFileMD5(const std::string& localPath);

//! Whether `client` points at an in-process local (yt_local) cluster — recognised by the
//! //sys/@local_mode_fqdn attribute that only yt_local sets. Such a cluster's exec nodes share this
//! host's filesystem, so a job can run the launcher binary straight from its on-disk path and the
//! file cache needs no replication; both the launcher and the cache options branch on this.
bool IsLocalModeCluster(const NApi::IClientPtr& client);

//! Uploads `localPath` (whose content hash is `md5`) into the per-cluster content-addressed cache at
//! `cacheDir`, reusing an existing blob when the md5 already matches (no re-upload). The cache is
//! shared across all flow operations on the cluster. Returns the Cypress path of the cached file.
NYPath::TYPath EnsureFileInCache(
    const NApi::IClientPtr& client,
    const std::string& localPath,
    const TString& md5,
    const NYPath::TYPath& cacheDir);

//! Removes from `filesDir` every child whose name is not in `keepNames`, leaving exactly the current
//! file set. Does nothing if `filesDir` is absent.
void PruneVanillaFiles(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& filesDir,
    const THashSet<std::string>& keepNames);

//! Re-stages every file referenced by the spec's tasks onto `destClient`'s cache `destCacheDir`
//! (transferring from `srcClient` through the cache, by md5, only when absent) and rewrites each
//! task's `file_paths` entry to the cache path (attributes preserved). Used by reanimate to bring the
//! durable files onto a remote runtime cluster without re-uploading what the cache already holds.
void RebaseVanillaSpecFiles(
    const NYTree::IMapNodePtr& spec,
    const NApi::IClientPtr& srcClient,
    const NApi::IClientPtr& destClient,
    const NYPath::TYPath& destCacheDir);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
