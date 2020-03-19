#pragma once

#include <yt/core/actions/callback.h>

#include <Objects.hxx>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

void RegisterShutdown();
void RegisterBeforeFinalizeShutdownCallback(TCallback<void()> callback, int index);
void RegisterAfterFinalizeShutdownCallback(TCallback<void()> callback, int index);
void RegisterAfterShutdownCallback(TCallback<void()> callback, int index);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

