#pragma once

#include <core/misc/public.h>

#include <ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;
using NJobTrackerClient::EJobPhase;

DEFINE_ENUM(EErrorCode,
    ((ConfigCreationFailed)  (1100))
    ((AbortByScheduler)      (1101))
    ((ResourceOverdraft)     (1102))
);

extern const Stroka ProxyConfigFileName;

class TJob;
typedef TIntrusivePtr<TJob> TJobPtr;

class TSlotManager;
typedef TIntrusivePtr<TSlotManager> TSlotManagerPtr;

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

class TSlotManagerConfig;
typedef TIntrusivePtr<TSlotManagerConfig> TSlotManagerConfigPtr;

class TSchedulerConnectorConfig;
typedef TIntrusivePtr<TSchedulerConnectorConfig> TSchedulerConnectorConfigPtr;

class TExecAgentConfig;
typedef TIntrusivePtr<TExecAgentConfig> TExecAgentConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
