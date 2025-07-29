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

std::string TDomesticMedium::GetLowercaseObjectName() const
{
    return Format("domestic medium %Qv", GetName());
}

std::string TDomesticMedium::GetCapitalizedObjectName() const
{
    return Format("Domestic medium %Qv", GetName());
}

std::string TDomesticMedium::GetMediumType() const
{
    return "domestic";
}

void TDomesticMedium::FillMediumDescriptor(NChunkClient::NProto::TMediumDirectory::TMediumDescriptor* protoItem) const
{
    TMedium::FillMediumDescriptor(protoItem);

    // Nothing to be filled, just creating an empty message.
    Y_UNUSED(protoItem->mutable_domestic_medium_descriptor());
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
