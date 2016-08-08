#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
#endif

#include <yt/core/actions/callback.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

template <class TProto, class... TArgs>
TTransactionActionHandlerDescriptor<TCallback<void(const Stroka&, TArgs...)>> MakeTransactionActionHandlerDescriptor(
    TCallback<void(TProto*, TArgs...)> handler)
{
    return {
        TProto::default_instance().GetTypeName(),
        BIND([=] (const Stroka& value, TArgs... args) {
            TProto typedValue;
            DeserializeFromProto(&typedValue, TRef::FromString(value));
            handler.Run(&typedValue, args...);
        })
    };
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NHiveServer
} // namespace NYT
