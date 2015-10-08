#include "stdafx.h"
#include "io_dispatcher.h"
#include "io_dispatcher_impl.h"

#include <core/misc/singleton.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

TIODispatcher::TIODispatcher()
    : Impl_(New<TIODispatcher::TImpl>())
{ }

TIODispatcher::~TIODispatcher()
{ }

TIODispatcher* TIODispatcher::Get()
{
    return TSingletonWithFlag<TIODispatcher>::Get();
}

void TIODispatcher::StaticShutdown()
{
    if (TSingletonWithFlag<TIODispatcher>::WasCreated()) {
        TIODispatcher::Get()->Shutdown();
    }
}

void TIODispatcher::Shutdown()
{
    return Impl_->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
