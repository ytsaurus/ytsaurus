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

class TRebootManager
    : public TRefCounted
{
public:
    TRebootManager(IInvokerPtr invoker);

    void RequestReboot();

    bool NeedReboot();

    NYTree::IYPathServicePtr GetOrchidService();

private:
    const IInvokerPtr Invoker_;
    const NYTree::IYPathServicePtr OrchidService_;

    bool NeedReboot_ = false;

    NYTree::IYPathServicePtr CreateOrchidService();

    void BuildOrchid(NYT::NYson::IYsonConsumer* consumer);

};

DEFINE_REFCOUNTED_TYPE(TRebootManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
