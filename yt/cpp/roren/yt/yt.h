#pragma once

#include <yt/cpp/roren/interface/fwd.h>
#include <yt/cpp/roren/yt/proto/config.pb.h>

#include "yt_read.h"
#include "yt_write.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

/// \@brief Create pipeline that is going to be executed over YT.
///
/// \@param cluster is YT cluster to run pipeline.
/// \@param workingDir is a directory for storing temporary data.
///    `//tmp` could be used, though it is not recommended for production processes:
///    https://yt.yandex-team.ru/docs/user-guide/best-practice/howtorunproduction#zakazhite-neobhodimye-resursy
/// \@param config

TPipeline MakeYtPipeline(const TString& cluster, const TString& workingDir);
TPipeline MakeYtPipeline(TYtPipelineConfig config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
