#pragma once

#include "public.h"

#include <CXX/Objects.hxx> // pycxx

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateError(const Py::Callable& errorClass, const std::string& message, const Py::Object& innerErrors);

Py::Exception CreateYtError(const std::string& message, const Py::Object& innerErrors);
Py::Exception CreateYtError(const std::string& message);

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYsonError(const std::string& message, const Py::Object& innerErrors);
Py::Exception CreateYsonError(const std::string& message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
