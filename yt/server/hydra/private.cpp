#include "private.h"

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/common.h>
#include <yt/core/misc/lazy_ptr.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const Stroka SnapshotExtension("snapshot");
const Stroka ChangelogExtension("log");
const Stroka ChangelogIndexExtension("index");

IInvokerPtr GetHydraIOInvoker()
{
    static TLazyIntrusivePtr<TActionQueue> HydraIOQueue(
        TActionQueue::CreateFactory("HydraIO"));
    return HydraIOQueue->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
