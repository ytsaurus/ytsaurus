#pragma once

#include "public.h"
#include "writer.h"
#include "format.h"

#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <core/misc/checksum.h>

#include <util/system/file.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a local and synchronous implementation of #IAsyncWriter.
class TFileWriter
    : public IWriter
{
public:
    explicit TFileWriter(
        const Stroka& fileName,
        bool syncOnClose = true);

    virtual TAsyncError Open() override;

    virtual bool WriteBlock(const TSharedRef& block) override;
    virtual bool WriteBlocks(const std::vector<TSharedRef>& blocks) override;

    virtual TAsyncError GetReadyEvent() override;

    virtual TAsyncError Close(const NChunkClient::NProto::TChunkMeta& chunkMeta) override;

    void Abort();

    //! Returns chunk info. The writer must be already closed.
    virtual const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override;

    virtual TChunkReplicaList GetWrittenChunkReplicas() const override;

    //! The writer must be already closed.
    const NChunkClient::NProto::TChunkMeta& GetChunkMeta() const;

    //! Can be called at any time.
    i64 GetDataSize() const;

private:
    Stroka FileName_;
    bool SyncOnClose_;

    bool IsOpen_;
    bool IsClosed_;
    i64 DataSize_;
    std::unique_ptr<TFile> DataFile_;
    NChunkClient::NProto::TChunkInfo ChunkInfo_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;
    NChunkClient::NProto::TChunkMeta ChunkMeta_;

    TAsyncError Result_;

};

DEFINE_REFCOUNTED_TYPE(TFileWriter)

///////////////////////////////////////////////////////////////////////////////

void RemoveChunkFiles(const Stroka& dataFileName);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
