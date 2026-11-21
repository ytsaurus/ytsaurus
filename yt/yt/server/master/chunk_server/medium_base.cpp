#include "medium_base.h"

#include "domestic_medium.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

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

void TMedium::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Name_);
    PHOENIX_REGISTER_FIELD(2, Index_);
    PHOENIX_REGISTER_FIELD(3, Priority_);
    PHOENIX_REGISTER_FIELD(4, Acd_);
}

PHOENIX_DEFINE_TYPE(TMedium);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
