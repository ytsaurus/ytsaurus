#include "helpers.h"

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableQueryError(const TError& error)
{
    return
        error.FindMatching(NDataNode::EErrorCode::LocalChunkReaderFailed) ||
        error.FindMatching(NChunkClient::EErrorCode::NoSuchChunk) ||
        error.FindMatching(NTabletClient::EErrorCode::TabletSnapshotExpired);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
