#pragma once

#include <contrib/libs/pycxx/Objects.hxx>

#include <ytlib/yson/consumer.h>
#include <ytlib/ytree/public.h>

namespace NYT {
namespace NYTree {

// This methods allow use methods convertTo* with Py::Object.
void Serialize(const Py::Object& obj, NYson::IYsonConsumer* consumer);

void Deserialize(Py::Object& obj, NYTree::INodePtr node);

} // namespace NYTree
} // namespace NYT


