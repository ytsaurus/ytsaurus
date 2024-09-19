#pragma once

#include <yt/yt/client/api/client.h>

namespace NYT::NStateChecker {

////////////////////////////////////////////////////////////////////////////////

class TStateChecker
    : public TRefCounted
{
public:
    TStateChecker(IInvokerPtr invoker, NApi::IClientPtr nativeClient, NYPath::TYPath instancePath, TDuration stateCheckPeriod);

    void Start();
    void SetPeriod(TDuration stateCheckPeriod);

    bool IsComponentBanned() const;

private:
    const NLogging::TLogger Logger;

    const IInvokerPtr Invoker_;
    const NApi::IClientPtr NativeClient_;
    const NYPath::TYPath InstancePath_;

    NConcurrency::TPeriodicExecutorPtr StateCheckerExecutor_;
    std::atomic<bool> Banned_ = false;

    void DoCheckState();
};

DEFINE_REFCOUNTED_TYPE(TStateChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NStateChecker
