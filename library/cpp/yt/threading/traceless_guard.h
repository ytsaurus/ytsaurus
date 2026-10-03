#pragma once

// TODO(babenko): Drop this shim; include library/cpp/yt/system/traceless_guard.h instead.

#include "rw_spin_lock.h"
#include "spin_lock_count.h"

#include <library/cpp/yt/system/traceless_guard.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

using ::NYT::TTracelessGuard;
using ::NYT::TTracelessInverseGuard;
using ::NYT::TTracelessReaderGuard;
using ::NYT::TTracelessTryGuard;
using ::NYT::TTracelessWriterGuard;
using ::NYT::TracelessGuard;
using ::NYT::TracelessReaderGuard;
using ::NYT::TracelessTryGuard;
using ::NYT::TracelessWriterGuard;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
