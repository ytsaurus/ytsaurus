#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TMultiplexerKeyState;

using TMultiplexerKeyStatePtr = TIntrusivePtr<TMultiplexerKeyState>;

////////////////////////////////////////////////////////////////////////////////

struct TEmptyMultiplexerUserState;
using TEmptyMultiplexerUserStatePtr = TIntrusivePtr<TEmptyMultiplexerUserState>;

struct TDynamicTableMultiplexerParameters;
using TDynamicTableMultiplexerParametersPtr = TIntrusivePtr<TDynamicTableMultiplexerParameters>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
