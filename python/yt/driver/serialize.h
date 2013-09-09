#pragma once

#include <contrib/libs/pycxx/Objects.hxx>

#include <core/yson/consumer.h>

namespace NYT {
namespace NYson {

// This method allows use methods convertTo* with Py::Object.
void Serialize(const Py::Object& obj, IYsonConsumer* consumer);

} // namespace NYson
} // namespace NYT


