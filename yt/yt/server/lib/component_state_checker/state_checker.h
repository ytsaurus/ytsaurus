#pragma once

#include <yt/yt/client/api/client.h>

namespace NYT::NComponentStateChecker {

////////////////////////////////////////////////////////////////////////////////

struct IComponentStateChecker
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void SetPeriod(TDuration stateCheckPeriod) = 0;

    virtual bool IsComponentBanned() const = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IComponentStateChecker)

////////////////////////////////////////////////////////////////////////////////

IComponentStateCheckerPtr CreateComponentStateChecker(
    IInvokerPtr invoker,
    NApi::IClientPtr nativeClient,
    NYPath::TYPath instancePath,
    TDuration stateCheckPeriod);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComponentStateChecker
