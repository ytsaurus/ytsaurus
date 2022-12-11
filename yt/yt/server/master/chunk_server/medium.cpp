#include "medium.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TMedium::TMedium(TMediumId id)
    : TObject(id)
    , Acd_(this)
{ }

TString TMedium::GetLowercaseObjectName() const
{
    return Format("medium %Qv", GetName());
}

TString TMedium::GetCapitalizedObjectName() const
{
    return Format("Medium %Qv", GetName());
}

void TMedium::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, Index_);
    Save(context, Priority_);
    Save(context, Transient_);
    Save(context, *Config_);
    Save(context, Acd_);
    Save(context, DiskFamilyWhitelist_);
}

void TMedium::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, Index_);
    Load(context, Priority_);
    Load(context, Transient_);

    // COMPAT(gritukan)
    if (context.GetVersion() < EMasterReign::RemoveCacheMedium) {
        Load<bool>(context);
    }

    Load(context, *Config_);
    Load(context, Acd_);

    // COMPAT(kvk1920)
    if (context.GetVersion() >= EMasterReign::DiskFamilyWhitelist) {
        Load(context, DiskFamilyWhitelist_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
