#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TCypressManager;
typedef TIntrusivePtr<TCypressManager> TCypressManagerPtr;

class TLock;

struct ICypressNode;
struct ICypressNodeProxy;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NCypress
} // namespace NYT
