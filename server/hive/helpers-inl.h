#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
#endif

#include <yt/core/actions/callback.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction, class TProto, class... TArgs>
TTransactionActionHandlerDescriptor<TCallback<void(TTransaction*, const TString&, TArgs...)>> MakeTransactionActionHandlerDescriptor(
    TCallback<void(TTransaction*, TProto*, TArgs...)> handler)
{
    return {
        TProto::default_instance().GetTypeName(),
        BIND([=] (TTransaction* transaction, const TString& value, TArgs... args) {
            TProto typedValue;
            DeserializeProto(&typedValue, TRef::FromString(value));
            handler.Run(transaction, &typedValue, args...);
        })
    };
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NHiveServer
} // namespace NYT
