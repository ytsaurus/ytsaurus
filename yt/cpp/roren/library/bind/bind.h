#pragma once

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename F>
decltype(auto) WrapToSerializableFunctor(F&& fn);

////////////////////////////////////////////////////////////////////////////////

template <typename F, typename... TBindings>
auto BindBack(F&& fn, TBindings&&... bindings);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

#define BIND_INL_H_
#include "bind-inl.h"
#undef FBIND_INL_H_
