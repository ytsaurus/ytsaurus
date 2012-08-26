#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/guid.h>
#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

using NScheduler::TJobId;
using NScheduler::TOperationId;

class TBootstrap;

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

class TEnvironmentManager;
typedef TIntrusivePtr<TEnvironmentManager> TEnvironmentManagerPtr;

class TSchedulerConnector;
typedef TIntrusivePtr<TSchedulerConnector> TSchedulerConnectorPtr;

class TEnvironmentConfig;
typedef TIntrusivePtr<TEnvironmentConfig> TEnvironmentConfigPtr;

class TEnvironmentManagerConfig;
typedef TIntrusivePtr<TEnvironmentManagerConfig> TEnvironmentManagerConfigPtr;

class TJobManagerConfig;
typedef TIntrusivePtr<TJobManagerConfig> TJobManagerConfigPtr;

class TSchedulerConnectorConfig;
typedef TIntrusivePtr<TSchedulerConnectorConfig> TSchedulerConnectorConfigPtr;

class TExecAgentConfig;
typedef TIntrusivePtr<TExecAgentConfig> TExecAgentConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
