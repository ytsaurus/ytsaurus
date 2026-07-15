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
    auto node = GetDynamicParametersNode();
    if (DynamicParametersCache_.Node != node || DynamicParametersCache_.Type != std::type_index(typeid(T))) {
        DynamicParametersCache_.Node = node;
        DynamicParametersCache_.Type = std::type_index(typeid(T));
        DynamicParametersCache_.Value = NYTree::ConvertTo<TIntrusivePtr<T>>(std::move(node));
    }
    return StaticPointerCast<T>(DynamicParametersCache_.Value);
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
