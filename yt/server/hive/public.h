#pragma once

#include <core/misc/common.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class THiveManager;
typedef TIntrusivePtr<THiveManager> THiveManagerPtr;

class TCellRegistry;
typedef TIntrusivePtr<TCellRegistry> TCellRegistryPtr;

struct TMessage;

class TMailbox;

class THiveManagerConfig;
typedef TIntrusivePtr<THiveManagerConfig> THiveManagerConfigPtr;

////////////////////////////////////////////////////////////////////////////////

typedef NHydra::TCellGuid TCellGuid;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
