#include "stdafx.h"
#include "private.h"

#include <core/misc/singleton.h>
#include <core/misc/lazy_ptr.h>

#include <core/concurrency/action_queue.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const Stroka SnapshotExtension("snapshot");
const Stroka ChangelogExtension("log");
const Stroka ChangelogIndexExtension("index");

class THydraIODispatcher
{
public:
    static THydraIODispatcher* Get()
    {
        return TSingleton::Get();
    }

    IInvokerPtr GetInvoker()
    {
        return Thread_->GetInvoker();
    }

    DECLARE_SINGLETON_DEFAULT_MIXIN(THydraIODispatcher);

private:
    THydraIODispatcher()
        : Thread_(TActionQueue::CreateFactory("HydraIO"))
    { }

    TLazyIntrusivePtr<TActionQueue> Thread_;
};

IInvokerPtr GetHydraIOInvoker()
{
    return THydraIODispatcher::Get()->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
