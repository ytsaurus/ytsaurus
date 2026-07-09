#pragma once

#include "public.h"

#include "spec.h"

#include <yt/yt/flow/library/cpp/common/state.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/ypath/rich.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TQueueInfoControllerState
    : public NYTree::TYsonStruct
{
    std::optional<int> CachedPartitionCount;

    REGISTER_YSON_STRUCT(TQueueInfoControllerState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueInfoControllerState);

////////////////////////////////////////////////////////////////////////////////

class TQueueInfoController
    : public TRefCounted
{
public:
    TQueueInfoController(
        TQueueInfoSpecPtr spec,
        NApi::IClientPtr client,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger,
        const IStatusProfilerPtr statusProfiler);

    void Init(IInitContextPtr initContext);
    void Sync();
    void Commit();

    std::optional<int> GetPartitionCount();

private:
    void TryUpdatePartitionCount();

protected:
    const NLogging::TLogger Logger;

private:
    const TQueueInfoSpecPtr Spec_;
    const NApi::IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const IStatusErrorStatePtr ErrorState_;
    TMutableStateClient<TQueueInfoControllerState> State_;

    NConcurrency::TPeriodicExecutorPtr Executor_;
};

DEFINE_REFCOUNTED_TYPE(TQueueInfoController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
