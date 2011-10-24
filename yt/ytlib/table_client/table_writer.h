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
{
    virtual TSharedRef Encode(const TSharedRef& block) = 0;

    //! Globally identifies codec type within YT.
    virtual TCodecId GetId() const = 0;
    virtual ~ICodec() { }
};

////////////////////////////////////////////////////////////////////////////////

//! For a given schema and input data creates a sequence of blocks and feeds them to chunkWriter.
//! Single-threaded
class  TTableWriter
    : public TNonCopyable
{
public:
    struct TConfig
    {
        int BlockSize;

        TConfig()
            // ToDo: make configurable
            : BlockSize(1024 * 1024)
        { }
    };

    TTableWriter(
        const TConfig& config, 
        IChunkWriter::TPtr chunkWriter, 
        const TSchema& schema,
        ICodec* codec);
    ~TTableWriter();

    void Write(const TColumn& column, TValue value);
    void EndRow();
    void Close();

private:
    void AddBlock(int channelIndex); 

private:
    bool IsClosed;

    TConfig Config;
    IChunkWriter::TPtr ChunkWriter;

    int CurrentBlockIndex;

    TSchema Schema;

    //! Columns already set in current row
    yhash_set<TColumn> UsedColumns;

    yvector<TChannelWriter::TPtr> ChannelWriters;

    ICodec* Codec;

    NProto::TChunkMeta ChunkMeta;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
