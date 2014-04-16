#pragma once

#include <core/logging/log.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger TableClientLogger;

int LowerBound(int lowerIndex, int upperIndex, std::function<bool(int)> less);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
