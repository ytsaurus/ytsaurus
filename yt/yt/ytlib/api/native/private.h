#pragma once

#include <yt/yt/client/api/private.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TListOperationsCountingFilter;
class TListOperationsFilter;

DECLARE_REFCOUNTED_STRUCT(ITypeHandler)

DECLARE_REFCOUNTED_CLASS(TClient)

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger TvmSynchronizerLogger("TvmSynchronizer");

inline const NLogging::TLogger NativeConnectionLogger("NativeConnection");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

