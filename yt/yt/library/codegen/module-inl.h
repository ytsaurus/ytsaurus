#ifndef CODEGEN_MODULE_INL_H_
#error "Direct inclusion of this file is not allowed, include module.h"
// For the sake of sane code completion.
#include "module.h"
#endif
#undef CODEGEN_MODULE_INL_H_

#include "caller.h"

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

template <class TSignature>
TCallback<TSignature> TCGModule::GetCompiledFunction(const TString& name)
{
    auto type = TTypeBuilder<TSignature>::Get(GetContext());
    YT_VERIFY(type == GetModule()->getFunction(name.c_str())->getFunctionType());
    auto function = reinterpret_cast<TSignature*>(GetFunctionAddress(name));

    using TCaller = TCGCaller<TSignature>;

    return TCallback<TSignature>(
        New<TCaller>(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            FROM_HERE,
#endif
            MakeStrong(this),
            function),
        &TCaller::StaticInvoke);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen

