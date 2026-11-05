#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void RegisterJobtruckLogWriterFactory();

NYTree::IMapNodePtr GetJobtruckLogWriterConfig(NYPath::TRichYPath pipelinePath);

void SetJobtruckLogWriterContext(
    NApi::IClientPtr client,
    TNodeInfoPtr nodeInfo,
    IStatusErrorStatePtr errorState);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
