#pragma once

#include "public.h"

#include <yt/yt/client/api/table_client.h>

#include <yt/yt/library/arrow_adapter/arrow.h>
#include <yt/yt/library/s3/client.h>
#include <yt/yt/library/s3/object.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Arrow's synchronous parquet reader backed by ranged S3 reads.
class TS3ArrowRandomAccessFile
    : public NArrow::TStatelessArrowRandomAccessFileBase
{
public:
    //! The constructor requests the object size before returning.
    TS3ArrowRandomAccessFile(NS3::TObjectDescriptor object, NS3::IClientPtr client);

    arrow20::Result<int64_t> GetSize() override;
    arrow20::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;

private:
    const NS3::TObjectDescriptor Object_;
    const NS3::IClientPtr Client_;
    const i64 FileSize_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ITableChunkMetaGenerator)

struct ITableChunkMetaGenerator
    : public TRefCounted
{
    //! May perform I/O. Call this before accessing any result.
    virtual void Generate() = 0;

    virtual i64 GetRowCount() const = 0;
    virtual i64 GetUncompressedDataSize() const = 0;
    virtual i64 GetCompressedDataSize() const = 0;
    virtual NTableClient::TTableSchemaPtr GetChunkSchema() const = 0;
    virtual TRefCountedChunkMetaPtr GetChunkMeta() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableChunkMetaGenerator)

//! Reserved for the common Arrow external-format generator API. Parquet does
//! not currently need generator-specific options.
struct TArrowTableChunkMetaGeneratorOptions
{ };

//! Builds the YT chunk meta required to expose an external Arrow-format file.
//! Parquet is the only supported format in this initial implementation.
ITableChunkMetaGeneratorPtr CreateArrowTableChunkMetaGenerator(
    EChunkFormat chunkFormat,
    std::shared_ptr<arrow20::io::RandomAccessFile> chunkFile,
    TArrowTableChunkMetaGeneratorOptions options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
