#pragma once

#include "client.h"

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NOrm::NClient::NGeneric {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqCreateObject;
class TRspCreateObject;
class TReqRemoveObject;
class TRspRemoveObject;
class TReqUpdateObject;
class TRspUpdateObject;
class TReqGetObjects;
class TRspGetObjects;
class TReqSelectObjects;
class TRspSelectObjects;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IOrmClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NGeneric
