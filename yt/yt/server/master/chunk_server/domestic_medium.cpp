#include "domestic_medium.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

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

void TDomesticMedium::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TMedium>();

    PHOENIX_REGISTER_FIELD(1, Transient_);
    PHOENIX_REGISTER_FIELD(2, Config_,
        .template Serializer<TDerefSerializer<>>());
    PHOENIX_REGISTER_FIELD(3, DiskFamilyWhitelist_);
    PHOENIX_REGISTER_DELETED_FIELD(4, bool, EnableSequoiaReplicas_, EMasterReign::RemoveEnableSequoiaReplicasInMedium);
}

PHOENIX_DEFINE_TYPE(TDomesticMedium);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
