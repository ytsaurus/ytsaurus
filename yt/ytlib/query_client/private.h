#pragma once

#include "public.h"

#include <core/logging/log.h>

#include <ytlib/table_client/public.h>

// FIXME(lukyan): try to fix in new visual studio
#ifdef _win_
#define MOVE(name) name
#else
#define MOVE(name) name = std::move(name)
#endif

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger QueryClientLogger;

NLogging::TLogger BuildLogger(TConstQueryPtr query);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

