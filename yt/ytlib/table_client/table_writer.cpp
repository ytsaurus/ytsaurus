#include "table_writer.h"

namespace {

TTableWriter::AddRow() 
{
    FOREACH(auto channel, Channels) {
        channel.AddRow(CurrentRow);
        if (channel.GetBufferSize() > Config.BlockSize) {
            TSharedRef block = channel.GetBlock(CurrentBlockIndex, RowNumber);
            TSharedRef compressedBlock = Compressor.Compress(block);
            AddBlock(compressedBlock);
            
            ++CurrentBlockIndex;
        }
    }

    // ToDo: Extract sample key for master meta

    CurrentRow.clear();
}

TTableWriter::Close()
{
    YASSERT(CurrentRow.empty());

    TTableChunkFooter footer;
    FOREACH(auto channel, Channels) {
        if (channel->HasBlocks()) {
            footer.AddChannel(channel->GetDescription());
        }
    }
    footer.SetCompressor(Compressor->GetId());
}

};
