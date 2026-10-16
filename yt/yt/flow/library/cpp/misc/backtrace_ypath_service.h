#pragma once

#include <yt/yt/core/ytree/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Creates a YPath service that exposes thread and fiber backtraces
//! under /threads and /fibers keys respectively.
NYTree::IYPathServicePtr CreateBacktraceYPathService();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
