#include "public.h"

#include <yt/yt/core/misc/statistic_path.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using namespace NStatisticPath;

const TStatisticPath InputRowCountPath = "/data/input/row_count"_SP;
const TStatisticPath InputUncompressedDataSizePath = "/data/input/uncompressed_data_size"_SP;
const TStatisticPath InputCompressedDataSizePath = "/data/input/compressed_data_size"_SP;
const TStatisticPath InputDataWeightPath = "/data/input/data_weight"_SP;
const TStatisticPath InputPipeIdleTimePath = "/user_job/pipes/input/idle_time"_SP;
const TStatisticPath JobProxyCpuUsagePath = "/job_proxy/cpu/user"_SP;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
