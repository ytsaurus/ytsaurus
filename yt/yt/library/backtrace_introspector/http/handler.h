#pragma once

#include <yt/yt/core/http/public.h>

namespace NYT::NBacktraceIntrospector {

////////////////////////////////////////////////////////////////////////////////

//! Registers introspector handlers.
void Register(
    const NHttp::IServerPtr& server,
    const TString& prefix = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktraceIntrospector
