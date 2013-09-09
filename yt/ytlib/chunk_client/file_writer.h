#pragma once

#include "public.h"
#include "async_writer.h"
#include "format.h"

#include <ytlib/chunk_client/chunk.pb.h>
#include <core/misc/checksum.h>

#include <util/system/file.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a local and synchronous implementation of #IAsyncWriter.
class TFileWriter
    : public IAsyncWriter
{
public:
    explicit TFileWriter(
        const Stroka& fileName,
        bool syncOnClose = true);

    virtual void Open();

    virtual bool WriteBlock(const TSharedRef& block);
    virtual TAsyncError GetReadyEvent();

    virtual TAsyncError AsyncClose(const NChunkClient::NProto::TChunkMeta& chunkMeta);

    void Abort();

    //! Returns chunk info. The writer must be already closed.
    virtual const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override;

    virtual const std::vector<int> GetWrittenIndexes() const override;

    //! The writer must be already closed.
    const NChunkClient::NProto::TChunkMeta& GetChunkMeta() const;

    //! Can be called at any time.
    i64 GetDataSize() const;

private:
    Stroka FileName;
    bool SyncOnClose;

    bool IsOpen;
    bool IsClosed;
    i64 DataSize;
    std::unique_ptr<TFile> DataFile;
    NChunkClient::NProto::TChunkInfo ChunkInfo;
    NChunkClient::NProto::TBlocksExt BlocksExt;
    NChunkClient::NProto::TChunkMeta ChunkMeta;

    TChecksumOutput ChecksumOutput;

    TAsyncError Result;

    bool EnsureOpen();

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
