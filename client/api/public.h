#pragma once

#include <yt/core/misc/public.h>

namespace NYP {
namespace NClient {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TPodSpec_THostDevice;
class TPodSpec_TSysctlProperty;

} // namespace NProto

using TObjectId = TString;
using TTransactionId = NYT::TGuid;

// XXX(babenko): decrease by a factor of 10
constexpr int MaxObjectIdLength = 2560;
constexpr int MaxNodeShortNameLength = 250;
constexpr int MaxPodFqdnLength = 630;

DEFINE_ENUM(EErrorCode,
    ((InvalidObjectId)             (100000))
    ((DuplicateObjectId)           (100001))
    ((NoSuchObject)                (100002))
    ((NotEnoughResources)          (100003))
    ((InvalidObjectType)           (100004))
    ((AuthenticationError)         (100005))
    ((AuthorizationError)          (100006))
    ((InvalidTransactionState)     (100007))
    ((InvalidTransactionId)        (100008))
    ((InvalidObjectState)          (100009))
    ((NoSuchTransaction)           (100010))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NClient
} // namespace NYP
