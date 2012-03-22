#pragma once

#include <ytlib/misc/intrusive_ptr.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TJobIoConfig;
typedef TIntrusivePtr<TJobIoConfig> TJobIoConfigPtr;

struct TJobProxyConfig;
typedef TIntrusivePtr<TJobProxyConfig> TJobProxyConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
