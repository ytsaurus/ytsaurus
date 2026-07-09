#pragma once

#include "public.h"

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERebalanceActionType,
    (Add)
    (Del)
);

////////////////////////////////////////////////////////////////////////////////

struct TRebalanceResultAction
{
    ERebalanceActionType Type{};
    TPartitionId PartitionId;
    std::string WorkerAddress;
};

struct TWorkerPreloadResultAction
{
    ERebalanceActionType Type;
    TResourceId ResourceId;
    std::string WorkerAddress;
};

////////////////////////////////////////////////////////////////////////////////

struct TRebalanceResult
{
    std::vector<TRebalanceResultAction> Actions;
    std::vector<TWorkerPreloadResultAction> PreloadResourceActions;
    std::optional<TSequenceId> SequenceId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
