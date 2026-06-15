#pragma once

#include <yt/yt/core/http/public.h>

namespace NYT::NBacktraceIntrospector {

////////////////////////////////////////////////////////////////////////////////

//! Registers introspector handlers.
void Register(
    const NHttp::IRequestPathMatcherPtr& handlers,
    const std::string& prefix = {});

void Register(
    const NHttp::IServerPtr& server,
    const std::string& prefix = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktraceIntrospector
