#pragma once

#include <Objects.hxx> // pycxx

#include "serialize.h"

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

std::optional<TString> ParseEncodingArgument(Py::Tuple& args, Py::Dict& kwargs);

Py::Bytes EncodeStringObject(const Py::Object& obj, const std::optional<TString>& encoding, TContext* context = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

