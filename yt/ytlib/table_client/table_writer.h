#pragma once

#include "value.h"
#include "schema.h"

namespace NYT {

//! For a given schema and input data creates a sequence of blocks and feeds them to given chunkWriter.
//! Single-threaded
class TTableWriter {
public:
    TTableWriter(
        TConfig config, 
        IChunkWriter::TPtr chunkWriter, 
        TSchema::TPtr schema,
        ICompressor::TPtr compressor = NULL);

    void Write(TValue column, TValue value);
    void AddRow();

    void Close();

private:
    void AddBlock(const TSharedRef& data); // thread may block in here

private:
    TConfig Config;

    int CurrentBlockIndex;

    TSchema::TPtr Schema;

    TTableRow CurrentRow;

    yvector<TChannelBuffer::TPtr> Channels;

    ICompressor::TPtr Compressor;

} // namespace NYT
