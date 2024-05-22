#pragma once

#include <yt/yt/client/api/private.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TListOperationsCountingFilter;
class TListOperationsFilter;

DECLARE_REFCOUNTED_STRUCT(ITypeHandler)

DECLARE_REFCOUNTED_CLASS(TClient)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, TvmSynchronizerLogger, "TvmSynchronizer");

YT_DEFINE_GLOBAL(const NLogging::TLogger, NativeConnectionLogger, "NativeConnection");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

