#pragma once

#include <Objects.hxx> // pycxx

#include "serialize.h"

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

TNullable<TString> ParseEncodingArgument(Py::Tuple& args, Py::Dict& kwargs);

Py::Bytes EncodeStringObject(const Py::Object& obj, const TNullable<TString>& encoding, TContext* context = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT

