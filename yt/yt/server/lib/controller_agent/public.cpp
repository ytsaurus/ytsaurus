#include "public.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

const TString InputRowCountPath = "/data/input/row_count";
const TString InputUncompressedDataSizePath = "/data/input/uncompressed_data_size";
const TString InputCompressedDataSizePath = "/data/input/compressed_data_size";
const TString InputDataWeightPath = "/data/input/data_weight";
const TString InputPipeIdleTimePath = "/user_job/pipes/input/idle_time";
const TString JobProxyCpuUsagePath = "/job_proxy/cpu/user";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
