#pragma once

#include "common.h"
#include "../chunk_client/chunk_writer.h"
#include "value.h"
#include "schema.h"
#include "channel_writer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef int TCodecId;

////////////////////////////////////////////////////////////////////////////////

struct ICodec
    : public TNonCopyable
{
public:
    typedef TAutoPtr<ICodec> TPtr;

    virtual TSharedRef Compress(const TSharedRef& block) = 0;

    //! Compressor id, written to chunk meta
    virtual TCodecId GetId() const = 0;
    virtual ~ICodec() { }
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
            // ToDo: make configurable
            : BlockSize(8 * 1024 * 1024)
        { }
    };

    TTableWriter(
        const TConfig& config, 
        IChunkWriter::TPtr chunkWriter, 
        const TSchema& schema,
        ICodec::TPtr codec);

    TTableWriter& Write(TColumn column, TValue value);
    void EndRow();
    void Close();
    ~TTableWriter();

private:
    // thread may block here, if chunkwriter window is overfilled
    void AddBlock(int channelIndex); 

private:
    bool IsClosed;

    TConfig Config;
    IChunkWriter::TPtr ChunkWriter;

    int CurrentBlockIndex;

    // ToDo: consider changing to int or completely removing
    i64 CurrentRowIndex;

    TSchema Schema;

    //! Columns already set in current row
    yhash_set<TColumn> SetColumns;

    yvector<TChannelWriter::TPtr> ChannelWriters;

    ICodec::TPtr Codec;

    NProto::TChunkMeta ChunkMeta;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
