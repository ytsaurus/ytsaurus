#pragma once

#include "public.h"

#include <Objects.hxx> // pycxx

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYtError(const TString& message, const Py::Object& innerErrors);
Py::Exception CreateYtError(const TString& message);

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYsonError(const TString& message, const Py::Object& innerErrors);
Py::Exception CreateYsonError(const TString& message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
