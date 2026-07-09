#pragma once

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void RegisterQueueLogWriterFactory();

NYTree::IMapNodePtr GetQueueLogWriterConfig(NYPath::TRichYPath pipelinePath);

void SetQueueLogWriterClient(NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
