#include "medium.h"

#include <yt/yt/server/master/chunk_server/config.h>
#include <yt/yt/server/master/chunk_server/medium.h>

namespace NYT::NChunkServer::NReplicator {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TMedium::TMedium(NChunkServer::TMedium* medium)
    : Id_(medium->GetId())
    , Index_(medium->GetIndex())
    , Name_(medium->GetName())
    , Config_(CloneYsonSerializable(medium->Config()))
{ }

std::unique_ptr<TMedium> TMedium::FromPrimary(NChunkServer::TMedium* medium)
{
    return std::make_unique<TMedium>(medium);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
