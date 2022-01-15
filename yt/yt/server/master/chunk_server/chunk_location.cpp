#include "chunk_location.h"
#include "medium.h"

#include <yt/yt/server/master/node_tracker_server/node.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

void TChunkLocation::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Uuid_);
    Save(context, Node_);
    Save(context, State_);
    Save(context, MediumOverride_);
    Save(context, Statistics_);
}

void TChunkLocation::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Uuid_);
    Load(context, Node_);
    Load(context, State_);
    Load(context, MediumOverride_);
    Load(context, Statistics_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
