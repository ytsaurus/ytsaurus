#pragma once

#include "clickhouse_config.h"

#if USE_HDFS
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <string>
#include <memory>
#error #include <hdfs/hdfs.h>
#include <base/types.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{
/** Accepts HDFS path to file and opens it.
 * Closes file by himself (thus "owns" a file descriptor).
 */
class ReadBufferFromHDFS : public ReadBufferFromFileBase
{
struct ReadBufferFromHDFSImpl;

public:
    ReadBufferFromHDFS(
        const String & hdfs_uri_,
        const String & hdfs_file_path_,
        const DBPoco::Util::AbstractConfiguration & config_,
        const ReadSettings & read_settings_,
        size_t read_until_position_ = 0,
        bool use_external_buffer = false,
        std::optional<size_t> file_size = std::nullopt);

    ~ReadBufferFromHDFS() override;

    bool nextImpl() override;

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override;

    std::optional<size_t> tryGetFileSize() override;

    size_t getFileOffsetOfBufferEnd() const override;

    String getFileName() const override;

private:
    std::unique_ptr<ReadBufferFromHDFSImpl> impl;
    bool use_external_buffer;
};
}

#endif
