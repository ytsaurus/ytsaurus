#pragma once
#include "clickhouse_config.h"

#include <filesystem>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueOrderedFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace fs = std::filesystem;
namespace DBPoco { class Logger; }

namespace DB
{
class StorageObjectStorageQueue;
struct ObjectStorageQueueTableMetadata;
struct StorageInMemoryMetadata;
using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

/**
 * A class for managing ObjectStorageQueue metadata in zookeeper, e.g.
 * the following folders:
 * - <path_to_metadata>/processed
 * - <path_to_metadata>/processing
 * - <path_to_metadata>/failed
 *
 * In case we use buckets for processing for Ordered mode, the structure looks like:
 * - <path_to_metadata>/buckets/<bucket>/processed -- persistent node, information about last processed file.
 * - <path_to_metadata>/buckets/<bucket>/lock -- ephemeral node, used for acquiring bucket lock.
 * - <path_to_metadata>/processing
 * - <path_to_metadata>/failed
 *
 * Depending on ObjectStorageQueue processing mode (ordered or unordered)
 * we can differently store metadata in /processed node.
 *
 * Implements caching of zookeeper metadata for faster responses.
 * Cached part is located in LocalFileStatuses.
 *
 * In case of Unordered mode - if files TTL is enabled or maximum tracked files limit is set
 * starts a background cleanup thread which is responsible for maintaining them.
 */
class ObjectStorageQueueMetadata
{
public:
    using FileStatus = ObjectStorageQueueIFileMetadata::FileStatus;
    using FileMetadataPtr = std::shared_ptr<ObjectStorageQueueIFileMetadata>;
    using FileStatusPtr = std::shared_ptr<FileStatus>;
    using FileStatuses = std::unordered_map<std::string, FileStatusPtr>;
    using Bucket = size_t;
    using Processor = std::string;

    ObjectStorageQueueMetadata(const fs::path & zookeeper_path_, const ObjectStorageQueueSettings & settings_);
    ~ObjectStorageQueueMetadata();

    void initialize(const ConfigurationPtr & configuration, const StorageInMemoryMetadata & storage_metadata);
    void checkSettings(const ObjectStorageQueueSettings & settings) const;
    void shutdown();

    FileMetadataPtr getFileMetadata(const std::string & path, ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr bucket_info = {});

    FileStatusPtr getFileStatus(const std::string & path);
    FileStatuses getFileStatuses() const;

    /// Method of Ordered mode parallel processing.
    bool useBucketsForProcessing() const;
    Bucket getBucketForPath(const std::string & path) const;
    ObjectStorageQueueOrderedFileMetadata::BucketHolderPtr tryAcquireBucket(const Bucket & bucket, const Processor & processor);

    static size_t getBucketsNum(const ObjectStorageQueueSettings & settings);
    static size_t getBucketsNum(const ObjectStorageQueueTableMetadata & settings);

private:
    void cleanupThreadFunc();
    void cleanupThreadFuncImpl();

    const ObjectStorageQueueSettings settings;
    const fs::path zookeeper_path;
    const size_t buckets_num;

    LoggerPtr log;

    std::atomic_bool shutdown_called = false;
    BackgroundSchedulePool::TaskHolder task;

    class LocalFileStatuses;
    std::shared_ptr<LocalFileStatuses> local_file_statuses;
};

}
