#include "memory_writer.h"

namespace NYT {
namespace NChunkClient {

using namespace NProto;

static auto AlwaysReady = MakeFuture(TError());

///////////////////////////////////////////////////////////////////////////////

TMemoryWriter::TMemoryWriter()
    : IsClosed(false)
{ }


void TMemoryWriter::Open()
{ }

bool TMemoryWriter::WriteBlock(const TSharedRef& block)
{
    Blocks.push_back(block);
    return true;
}

TAsyncError TMemoryWriter::GetReadyEvent()
{
    return AlwaysReady;
}

TAsyncError TMemoryWriter::AsyncClose(const TChunkMeta& meta)
{
    IsClosed = true;
    Meta = meta;
    return AlwaysReady;
}

const TChunkInfo& TMemoryWriter::GetChunkInfo() const
{
    YUNIMPLEMENTED();
}

const std::vector<int> TMemoryWriter::GetWrittenIndexes() const
{
    YUNIMPLEMENTED();
}


std::vector<TSharedRef>& TMemoryWriter::GetBlocks()
{
    return Blocks;
}

NProto::TChunkMeta& TMemoryWriter::GetMeta()
{
    return Meta;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

