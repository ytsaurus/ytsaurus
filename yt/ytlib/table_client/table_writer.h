#pragma once

#include "../misc/common.h"
#include "../chunk_client/chunk_writer.h"
#include "value.h"
#include "schema.h"
#include "channel_writer.h"
#include "chunk_meta.pb.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class ICompressor
    : public TNonCopyable
{
public:
    typedef TAutoPtr<ICompressor> TPtr;

    virtual TSharedRef Compress(const TSharedRef& block) = 0;

    //! Compressor id, written to chunk meta
    virtual int GetId() const = 0;
    virtual ~ICompressor() { }
};

////////////////////////////////////////////////////////////////////////////////

//! For a given schema and input data creates a sequence of blocks and feeds them to chunkWriter.
//! Single-threaded
class TTableWriter 
{
public:
    struct TConfig
    {
        int BlockSize;

        TConfig()
            : BlockSize(8 * 1024 * 1024)
        { }
    };

    TTableWriter(
        const TConfig& config, 
        IChunkWriter::TPtr chunkWriter, 
        TSchema::TPtr schema,
        ICompressor::TPtr compressor);

    TTableWriter& Write(const TValue& column, const TValue& value);
    void AddRow();
    void Close();

private:
    // thread may block here, if chunkwriter window is overfilled
    void AddBlock(int channelIndex); 

private:
    TConfig Config;
    IChunkWriter::TPtr ChunkWriter;

    int CurrentBlockIndex;
    i64 CurrentRowIndex;

    TSchema::TPtr Schema;

    TTableRow CurrentRow;

    yvector<TChannelWriter::TPtr> ChannelWriters;

    ICompressor::TPtr Compressor;

    NTableClient::NProto::TChunkMeta ChunkMeta;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
