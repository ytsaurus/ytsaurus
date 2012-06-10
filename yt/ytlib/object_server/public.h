#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TObjectManager;
typedef TIntrusivePtr<TObjectManager> TObjectManagerPtr;

struct TObjectManagerConfig;
typedef TIntrusivePtr<TObjectManagerConfig> TObjectManagerConfigPtr;

class TObjectBase;
class TObjectWithIdBase;

struct IObjectProxy;
typedef TIntrusivePtr<IObjectProxy> IObjectProxyPtr;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NObjectServer
} // namespace NYT
