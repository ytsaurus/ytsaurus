#include "medium.h"
#include "config.h"

#include <yt/server/cell_master/serialize.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

TMedium::TMedium(const TMediumId& id)
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
    // COMPAT(shakurov)
    if (context.GetVersion() < 502) {
        Priority_ = MediumDefaultPriority;
    } else {
        Load(context, Priority_);
    }
    Load(context, Transient_);
    Load(context, Cache_);
    //COMPAT(savrus)
    if (context.GetVersion() >= 629) {
        Load(context, *Config_);
    }
    Load(context, Acd_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
