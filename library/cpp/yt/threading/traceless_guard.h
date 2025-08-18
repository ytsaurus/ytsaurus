#pragma once

#define TRACELESS_GUARD_INL_H_
#include "traceless_guard-inl.h"
#undef TRACELESS_GUARD_INL_H_

namespace NYT::NThreading {

// This guards are zero-cost replacements for normal ones
// which allow user to avoid spinlocks being tracked.

////////////////////////////////////////////////////////////////////////////////

using NDetail::TTracelessGuard;
using NDetail::TTracelessInverseGuard;
using NDetail::TTracelessTryGuard;
using NDetail::TTracelessReaderGuard;
using NDetail::TTracelessWriterGuard;

////////////////////////////////////////////////////////////////////////////////

using NDetail::TracelessGuard;
using NDetail::TracelessTryGuard;
using NDetail::TracelessReaderGuard;
using NDetail::TracelessWriterGuard;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
