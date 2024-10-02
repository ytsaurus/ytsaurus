#include "public.h"

#include <yt/yt/core/misc/guid.h>

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TObjectTypeName TObjectTypeNames::Null = "null";
const TObjectTypeName TObjectTypeNames::User = "user";
const TObjectTypeName TObjectTypeNames::Group = "group";
const TObjectTypeName TObjectTypeNames::Schema = "schema";
const TObjectTypeName TObjectTypeNames::Semaphore = "semaphore";
const TObjectTypeName TObjectTypeNames::SemaphoreSet = "semaphore_set";

////////////////////////////////////////////////////////////////////////////////

TObjectId GenerateUuid()
{
    return ToString(TGuid::Create());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
