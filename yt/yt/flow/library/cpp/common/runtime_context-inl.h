#pragma once

#ifndef RUNTIME_CONTEXT_INL_H_
    #error "Direct inclusion of this file is not allowed, include runtime_context.h"
    // For the sake of sane code completion.
    #include "runtime_context.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> IRuntimeContext::GetDynamicParameters() const
{
    auto object = GetDynamicParametersObject();
    if (!object) {
        // The spec names no processing function (possible only under the test builder): the
        // dynamic block is absent and defaults apply.
        return New<T>();
    }
    auto parameters = DynamicPointerCast<T>(object);
    THROW_ERROR_EXCEPTION_UNLESS(parameters,
        "Dynamic function parameters type mismatch: requested %Qv, registered %Qv",
        TypeName<T>(),
        TypeName(*object));
    return parameters;
}

template <class T>
TIntrusivePtr<T> IRuntimeContext::ConvertToYsonMessage(const TInputMessageConstPtr& message) const
{
    return ::NYT::NFlow::ConvertToYsonMessage<T>(message);
}

template <class T>
TIntrusivePtr<T> IRuntimeContext::ConvertToYsonKey(const TKey& key) const
{
    return NYsonSerializer::Deserialize<T>(key.Underlying(), GetKeySchema());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
