#pragma once

#include <yt/yt/core/ytree/public.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <utility>

namespace NYql::NYtflow::NPrepare::NPrivate {

TVector<std::pair<TString, NYT::NYTree::IAttributeDictionaryPtr>> BuildYqlPipelineTableAttributes(
    bool createWorkerLogsTable);

} // namespace NYql::NYtflow::NPrepare::NPrivate
