#pragma once

#include <yt/yt/core/actions/callback.h>

#include <CXX/Objects.hxx>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

void RegisterShutdown();
void RegisterBeforeFinalizeShutdownCallback(TCallback<void()> callback, int index);
void RegisterAfterFinalizeShutdownCallback(TCallback<void()> callback, int index);
void RegisterAfterShutdownCallback(TCallback<void()> callback, int index);

////////////////////////////////////////////////////////////////////////////////

void EnterReleaseAcquireGuard();
void LeaveReleaseAcquireGuard();

using TFutureCookie = i64;
constexpr TFutureCookie InvalidFutureCookie = -1;

TFutureCookie RegisterFuture(TFuture<void> future);
void UnregisterFuture(TFutureCookie cookie);
void FinalizeFutures();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

