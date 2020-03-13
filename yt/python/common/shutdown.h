#pragma once

#include <yt/core/actions/callback.h>

#include <Objects.hxx>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

void RegisterShutdownCallback(TCallback<void()> additionalCallback, int index);
void RegisterShutdown(const Py::Object& beforePythonFinalizeCallback);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

