#pragma once

#include <library/cpp/yt/logging/public.h>

#include <util/generic/ptr.h>

class TLogBackend;

namespace NYT::NLogging::NBridge {

////////////////////////////////////////////////////////////////////////////////

//! Create TLogBackend which redirects log messages to |logger|.
THolder<TLogBackend> CreateLogBackend(const TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging::NBridge
