#include "backtrace_ypath_service.h"

#include <yt/yt/library/backtrace_introspector/introspect.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/ytree/composite_map.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NBacktraceIntrospector;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Backtrace introspection must run on a dedicated thread because it suspends
// all other threads while collecting stack traces.
const TActionQueuePtr& GetBacktraceQueue()
{
    static const auto queue = New<TActionQueue>("BacktraceIntro");
    return queue;
}

template <class TIntrospect>
IYPathServicePtr CreateIntrospectionService(TIntrospect introspect)
{
    return IYPathService::FromProducerLazy(BIND([introspect = std::move(introspect)] (IYsonConsumer* consumer) {
        auto infos = WaitFor(
            BIND(introspect)
                .AsyncVia(GetBacktraceQueue()->GetInvoker())
                .Run())
            .ValueOrThrow();
        consumer->OnStringScalar(FormatIntrospectionInfos(infos));
    }));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

IYPathServicePtr CreateBacktraceYPathService()
{
    return CreateCompositeMapService()
        ->AddChild("threads", CreateIntrospectionService(&IntrospectThreads))
        ->AddChild("fibers", CreateIntrospectionService(&IntrospectFibers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
