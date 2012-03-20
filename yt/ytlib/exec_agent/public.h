#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/guid.h>
#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

using NScheduler::TJobId;
using NScheduler::TTaskId;

class TBootstrap;

class TJobManagerConfig;
typedef TIntrusivePtr<TJobManagerConfig> TJobManagerConfigPtr;

class TJobManager;
typedef TIntrusivePtr<TJobManager> TJobManagerPtr;

class TJob;
typedef TIntrusivePtr<TJob> TJobPtr;

class TSlot;
typedef TIntrusivePtr<TSlot> TSlotPtr;

struct IProxyController;
typedef TIntrusivePtr<IProxyController> IProxyControllerPtr;

struct IEnvironmentBuilder;
typedef TIntrusivePtr<IEnvironmentBuilder> IEnvironmentBuilderPtr;

class TEnvironmentConfig;
typedef TIntrusivePtr<TEnvironmentConfig> TEnvironmentConfigPtr;

class TEnvironmentManagerConfig;
typedef TIntrusivePtr<TEnvironmentManagerConfig> TEnvironmentManagerConfigPtr;

class TEnvironmentManager;
typedef TIntrusivePtr<TEnvironmentManager> TEnvironmentManagerPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
