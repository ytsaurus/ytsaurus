#include "medium.h"
#include "config.h"

#include <yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

TMedium::TMedium(TMediumId id)
    : TObjectBase(id)
    , Index_(-1)
    , Config_(New<TMediumConfig>())
    , Acd_(this)
{ }

void TMedium::Save(NCellMaster::TSaveContext& context) const
{
    TObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, Index_);
    Save(context, Priority_);
    Save(context, Transient_);
    Save(context, Cache_);
    Save(context, *Config_);
    Save(context, Acd_);
}

void TMedium::Load(NCellMaster::TLoadContext& context)
{
    TObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, Index_);
    Load(context, Priority_);
    Load(context, Transient_);
    Load(context, Cache_);
    Load(context, *Config_);
    Load(context, Acd_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
