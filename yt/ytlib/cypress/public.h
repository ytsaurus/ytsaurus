#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TCypressManager;
typedef TIntrusivePtr<TCypressManager> TCypressManagerPtr;

class TWorldInitializer;
typedef TIntrusivePtr<TWorldInitializer> TWorldInitializerPtr;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NCypress
} // namespace NYT
