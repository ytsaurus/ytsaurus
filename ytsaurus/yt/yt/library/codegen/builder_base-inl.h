#ifndef BUILDER_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include builder_base.h"
// For the sake of sane code completion.
#include "builder_base.h"
#endif
#undef BUILDER_BASE_INL_H_

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires std::is_integral_v<T>
llvm::ConstantInt* TIRBuilderBase::ConstantIntValue(T value)
{
    return llvm::ConstantInt::get(TTypeBuilder<T>::Get(Context), value);
}

template <class T>
    requires std::is_enum_v<T>
llvm::ConstantInt* TIRBuilderBase::ConstantEnumValue(T value)
{
    using TUnderlying = std::underlying_type_t<T>;
    return ConstantIntValue<TUnderlying>(static_cast<TUnderlying>(value));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
