#ifndef DISPATCH_BY_TYPE_INL_H_
#error "Direct inclusion of this file is not allowed, include dispatch_by_type.h"
// For the sake of sane code completion.
#include "dispatch_by_type.h"
#endif
#undef DISPATCH_BY_TYPE_INL_H_

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

template <template <NTableClient::EValueType Type> class TFunction, class... TArgs>
auto DispatchByDataType(NTableClient::EValueType type, TArgs&&... args)
{
    using NTableClient::EValueType;
    switch (type) {
        case EValueType::Int64:
            return TFunction<EValueType::Int64>::Do(std::forward<TArgs>(args)...);

        case EValueType::Uint64:
            return TFunction<EValueType::Uint64>::Do(std::forward<TArgs>(args)...);

        case EValueType::Double:
            return TFunction<EValueType::Double>::Do(std::forward<TArgs>(args)...);

        case EValueType::Boolean:
            return TFunction<EValueType::Boolean>::Do(std::forward<TArgs>(args)...);

        case EValueType::String:
            return TFunction<EValueType::String>::Do(std::forward<TArgs>(args)...);

        case EValueType::Any:
            return TFunction<EValueType::Any>::Do(std::forward<TArgs>(args)...);

        case EValueType::Composite:
            return TFunction<EValueType::Composite>::Do(std::forward<TArgs>(args)...);

        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }

    ThrowUnexpectedValueType(type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

