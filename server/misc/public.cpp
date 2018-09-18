#include "private.h"

#include "interned_attributes.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const char* ClusterMasterProgramName = "ytserver-master";
const char* ClusterNodeProgramName = "ytserver-node";
const char* ClusterSchedulerProgramName = "ytserver-scheduler";
const char* ExecProgramName = "ytserver-exec";
const char* JobProxyProgramName = "ytserver-job-proxy";

////////////////////////////////////////////////////////////////////////////////

#define XX(camelCaseName, snakeCaseName) \
    REGISTER_INTERNED_ATTRIBUTE(snakeCaseName, EInternedAttributeKey::camelCaseName)

FOR_EACH_INTERNED_ATTRIBUTE(XX)

#undef XX

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
