#include "medium_base.h"

#include "domestic_medium.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TMedium::TMedium(TMediumId id)
    : TObject(id)
    , Acd_(this)
{ }

bool TMedium::IsOffshore() const
{
    return !IsDomestic();
}

TDomesticMedium* TMedium::AsDomestic()
{
    YT_VERIFY(IsDomestic());
    return As<TDomesticMedium>();
}

const TDomesticMedium* TMedium::AsDomestic() const
{
    YT_VERIFY(IsDomestic());
    return As<TDomesticMedium>();
}

void TMedium::Save(TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, Index_);
    Save(context, Priority_);
    Save(context, Acd_);
}

void TMedium::Load(TLoadContext& context)
{
    TObject::Load(context);

    // COMPAT(gritukan);
    if (context.GetVersion() < EMasterReign::MediumBase) {
        return;
    }

    using NYT::Load;
    Load(context, Name_);
    Load(context, Index_);
    Load(context, Priority_);
    Load(context, Acd_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
