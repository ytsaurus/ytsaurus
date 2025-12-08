#pragma once

#include "public.h"

#include <yt/yt/client/api/table_client.h>

#include <yt/yt/library/s3/client.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/interfaces.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

struct IChunkMetaGenerator
    : public TRefCounted
{
    //! Must be called before calling any other methods.
    //! The only method allowed to make asynchronous calls.
    virtual void Generate() = 0;

    //! Accessors to various generated statistics.
    virtual i64 GetUncompressedDataSize() const = 0;
    virtual i64 GetCompressedDataSize() const = 0;

    //! Static accessors to the generated objects.
    virtual TRefCountedChunkMetaPtr GetChunkMeta() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkMetaGenerator)

////////////////////////////////////////////////////////////////////////////

struct ITableChunkMetaGenerator
    : public virtual IChunkMetaGenerator
{
    virtual i64 GetRowCount() const = 0;
    virtual NTableClient::TTableSchemaPtr GetChunkSchema() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableChunkMetaGenerator)

////////////////////////////////////////////////////////////////////////////

struct TArrowTableChunkMetaGeneratorOptions
{
    //! Seed to random number generator which chooses samples to be in the meta;
    //! it's best to use the same seed for the same file/table as it will make
    //! the process deterministic - the same samples will always be chosen during
    //! the meta generation process. For instance, a full path to a file or a hash
    //! of chunk ID may be used.
    ui64 SampleRandomSeed = RandomNumber<ui64>();

    //! This magic constant is taken from the default value of sampling rate
    //! of TChunkWriterConfig; seems reasonable for now.
    double SampleRate = 0.0001;

    //! By default use the less precise but faster strategy.
    NTableClient::EChunkMetaSampleGenerationStrategy SampleStrategy =
        NTableClient::EChunkMetaSampleGenerationStrategy::Fast;
};

////////////////////////////////////////////////////////////////////////////

ITableChunkMetaGeneratorPtr CreateArrowTableChunkMetaGenerator(
    EChunkFormat chunkFormat,
    const std::shared_ptr<arrow20::io::RandomAccessFile>& chunkFile,
    TArrowTableChunkMetaGeneratorOptions options = {});

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient