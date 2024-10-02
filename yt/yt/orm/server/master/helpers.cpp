#include "helpers.h"

#include <yt/yt/orm/server/objects/attribute_policy.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

void ValidateDbName(const TString& dbName)
{
    static const auto DBNameValidator = NObjects::CreateStringAttributePolicy(
        NObjects::EAttributeGenerationPolicy::Manual,
        2, 16,
        "abcdefghijklmnopqrstuvwxyz-_");
    DBNameValidator->Validate(dbName, "DB name");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
