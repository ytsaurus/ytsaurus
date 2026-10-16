#pragma once

#include <yql/essentials/core/file_storage/defs/downloader.h>

namespace NYql {

class TFileStorageConfig;

NYql::NFS::IDownloaderPtr MakeFmrDownloader(const TFileStorageConfig& config);

} // NYql
