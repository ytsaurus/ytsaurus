#pragma once

#include "io_dispatcher.h"

#include <core/concurrency/action_queue_detail.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcher::TImpl
    : public NConcurrency::TEVSchedulerThread
{
public:
    TImpl();

    const ev::loop_ref& GetEventLoop() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
