#pragma once
#include "defs.h"
#include <contrib/ydb/core/protos/sqs.pb.h>

namespace NKikimr::NSQS {

TString CalcMD5OfMessageAttributes(const google::protobuf::RepeatedPtrField<TMessageAttribute>& attributes);

} // namespace NKikimr::NSQS
