#pragma once

#include <yt/yt/core/logging/log.h>

namespace NRoren {
    extern const NYT::NLogging::TLogger RorenLogger;
}

#define USE_ROREN_LOGGER() static const auto& Logger = NRoren::RorenLogger

