#include "stdafx.h"
#include "journal_session.h"

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TJournalSession::TJournalSession(
    TDataNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap,
    const TChunkId& chunkId,
    EWriteSessionType type,
    bool syncOnClose,
    TLocationPtr location)
    : TSession(
        config,
        bootstrap,
        chunkId,
        type,
        syncOnClose,
        location)
{ }

const TChunkInfo& TJournalSession::GetChunkInfo() const
{
    YUNREACHABLE();
}

void TJournalSession::Start(TLeaseManager::TLease lease)
{
    TSession::Start(lease);
    YUNREACHABLE();
}

void TJournalSession::Cancel(const TError& error)
{
    YUNREACHABLE();
}

TFuture<TErrorOr<IChunkPtr>> TJournalSession::Finish(const TChunkMeta& chunkMeta)
{
    YUNREACHABLE();
}

TAsyncError TJournalSession::PutBlocks(
    int startBlockIndex,
    const std::vector<TSharedRef>& blocks,
    bool enableCaching)
{
    YUNREACHABLE();
}

TAsyncError TJournalSession::SendBlocks(
    int startBlockIndex,
    int blockCount,
    const TNodeDescriptor& target)
{
    YUNREACHABLE();
}

TAsyncError TJournalSession::FlushBlock(int blockIndex)
{
    YUNREACHABLE();
}

void TJournalSession::Ping()
{
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
