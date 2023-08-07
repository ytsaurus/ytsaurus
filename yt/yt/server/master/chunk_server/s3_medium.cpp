#include "s3_medium.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

bool TS3Medium::IsDomestic() const
{
    return false;
}

TString TS3Medium::GetLowercaseObjectName() const
{
    return "S3 medium";
}

TString TS3Medium::GetCapitalizedObjectName() const
{
    return "S3 medium";
}

void TS3Medium::Save(NCellMaster::TSaveContext& context) const
{
    TMedium::Save(context);

    using NYT::Save;

    Save(context, *Config_);
}

void TS3Medium::Load(NCellMaster::TLoadContext& context)
{
    TMedium::Load(context);

    using NYT::Load;

    Load(context, *Config_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
