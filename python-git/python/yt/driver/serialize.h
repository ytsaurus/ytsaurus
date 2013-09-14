#pragma once

#include <contrib/libs/pycxx/Objects.hxx>

#include <core/yson/public.h>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

//! Binds Python objects to Convert framework.
void Serialize(const Py::Object& obj, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT


