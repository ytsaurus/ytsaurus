#include "s3_medium.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

bool TS3Medium::IsDomestic() const
{
    return false;
}

std::string TS3Medium::GetLowercaseObjectName() const
{
    return "S3 medium";
}

std::string TS3Medium::GetCapitalizedObjectName() const
{
    return "S3 medium";
}

void TS3Medium::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TMedium>();

    PHOENIX_REGISTER_FIELD(1, Config_,
        .template Serializer<TDerefSerializer<>>());
}

PHOENIX_DEFINE_TYPE(TS3Medium);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
