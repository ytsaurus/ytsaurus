#pragma once

#include <yt/cpp/roren/interface/fwd.h>
#include <yt/cpp/roren/yt/proto/config.pb.h>

#include "yt_read.h"
#include "yt_write.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TPipeline MakeYtPipeline(const TString& cluster, const TString& workingDir);
TPipeline MakeYtPipeline(TYtPipelineConfig config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
