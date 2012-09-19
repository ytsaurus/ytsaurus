#pragma once

#include <contrib/libs/pycxx/Objects.hxx>

#include <ytlib/ytree/yson_consumer.h>

namespace NYT {
namespace NYTree {

// This method allows use methods convertTo* with Py::Object.
void Serialize(const Py::Object& obj, IYsonConsumer* consumer);

} // namespace NYTree
} // namespace NYT


