#pragma once

#include "public.h"

#include <Objects.hxx> // pycxx

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYtError(const TString& message, const Py::Object& innerErrors);
Py::Exception CreateYtError(const TString& message);

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYsonError(const TString& message, const Py::Object& innerErrors);
Py::Exception CreateYsonError(const TString& message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
