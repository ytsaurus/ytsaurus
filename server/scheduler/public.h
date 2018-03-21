#pragma once

#include <yp/server/misc/public.h>

#include <yp/server/master/public.h>

#include <yp/server/objects/public.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TResourceManager)
DECLARE_REFCOUNTED_CLASS(TScheduler)

DECLARE_REFCOUNTED_CLASS(TSchedulerConfig)

using NObjects::TObjectId;
using NObjects::EResourceKind;

extern const TString TopologyLabel;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
