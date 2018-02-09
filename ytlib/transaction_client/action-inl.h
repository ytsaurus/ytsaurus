#pragma once
#ifndef ACTION_INL_H_
#error "Direct inclusion of this file is not allowed, include action.h"
#endif

#include <yt/core/misc/serialize.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

template <class TProto>
TTransactionActionData MakeTransactionActionData(const TProto& message)
{
    return TTransactionActionData{
        message.GetTypeName(),
        SerializeProtoToString(message)
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
