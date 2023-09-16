#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRestartManager
    : public TRefCounted
{
public:
    explicit TRestartManager(IInvokerPtr invoker);

    void RequestRestart();

    bool IsRestartNeeded();

    NYTree::IYPathServicePtr GetOrchidService();

private:
    const IInvokerPtr Invoker_;
    const NYTree::IYPathServicePtr OrchidService_;

    std::atomic<bool> NeedRestart_ = false;

    NYTree::IYPathServicePtr CreateOrchidService();

    void BuildOrchid(NYT::NYson::IYsonConsumer* consumer);
};

DEFINE_REFCOUNTED_TYPE(TRestartManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
