#pragma once

#include <CXX/Objects.hxx> // pycxx

#include "serialize.h"

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

std::optional<std::string> ParseEncodingArgument(Py::Tuple& args, Py::Dict& kwargs);

Py::Bytes EncodeStringObject(const Py::Object& obj, const std::optional<std::string>& encoding, TContext* context = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

