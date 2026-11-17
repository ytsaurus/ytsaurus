#pragma once

#include <yt/yql/providers/dq/interface/yql_dq_task_preprocessor.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NYql::NDqs {

NActors::IActor* MakeGraphExecutionEventsActor(const TString& traceID, std::vector<IDqTaskPreprocessor::TPtr>&& taskPreprocessors);

// Task preprocessors may block in the synchronous SDK. Keep them off the main pool.
NActors::TActorId RegisterGraphExecutionEventsActor(NActors::TActorSystem& actorSystem, const TString& traceID, std::vector<IDqTaskPreprocessor::TPtr>&& taskPreprocessors);

} // namespace NYql::NDq
