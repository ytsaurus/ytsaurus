#include "stdafx.h"
#include "private.h"

#include <core/misc/lazy_ptr.h>

#include <core/concurrency/action_queue.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const Stroka SnapshotExtension("snapshot");
const Stroka ChangelogExtension("log");
const Stroka ChangelogIndexSuffix(".index");

IInvokerPtr GetHydraIOInvoker()
{
    static TLazyIntrusivePtr<TActionQueue> HydraIOQueue(
        TActionQueue::CreateFactory("HydraIO"));
    return HydraIOQueue->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
