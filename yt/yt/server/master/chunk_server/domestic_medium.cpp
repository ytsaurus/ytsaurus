#include "domestic_medium.h"

#include "config.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

bool TDomesticMedium::IsDomestic() const
{
    return true;
}

TString TDomesticMedium::GetLowercaseObjectName() const
{
    return Format("domestic medium %Qv", GetName());
}

TString TDomesticMedium::GetCapitalizedObjectName() const
{
    return Format("Domestic medium %Qv", GetName());
}

void TDomesticMedium::Save(NCellMaster::TSaveContext& context) const
{
    TMedium::Save(context);

    using NYT::Save;
    Save(context, Transient_);
    Save(context, *Config_);
    Save(context, DiskFamilyWhitelist_);
    Save(context, EnableSequoiaReplicas_);
}

void TDomesticMedium::Load(NCellMaster::TLoadContext& context)
{
    TMedium::Load(context);

    using NYT::Load;

    // COMPAT(gritukan)
    if (context.GetVersion() < EMasterReign::MediumBase) {
        Load(context, Name_);
        Load(context, Index_);
        Load(context, Priority_);
    }

    Load(context, Transient_);

    Load(context, *Config_);

    // COMPAT(gritukan)
    if (context.GetVersion() < EMasterReign::MediumBase) {
        Load(context, Acd_);
    }

    Load(context, DiskFamilyWhitelist_);

    // COMPAT(aleksandra-zh)
    if (context.GetVersion() >= EMasterReign::SequoiaReplicas) {
        Load(context, EnableSequoiaReplicas_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
