#pragma once

#include <yt/yt/flow/library/cpp/pipeline_tables/public.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <utility>

namespace NYql::NYtflow::NPrepare::NPrivate {

TVector<std::pair<TString, NYT::NFlow::TPipelineNodeDefinition>> BuildYqlPipelineNodeDefinitions();

TVector<std::pair<TString, NYT::NYTree::IAttributeDictionaryPtr>> BuildYqlPipelineTableAttributes(
    bool createWorkerLogsTable);

} // namespace NYql::NYtflow::NPrepare::NPrivate
