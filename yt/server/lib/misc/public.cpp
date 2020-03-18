#include "private.h"

#include "interned_attributes.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const TString ClusterMasterProgramName("ytserver-master");
const TString ClusterNodeProgramName("ytserver-node");
const TString ClusterSchedulerProgramName("ytserver-scheduler");
const TString ExecProgramName("ytserver-exec");
const TString JobProxyProgramName("ytserver-job-proxy");

////////////////////////////////////////////////////////////////////////////////

const TString BanMessageAttributeName("ban_message");
const TString ConfigAttributeName("config");

////////////////////////////////////////////////////////////////////////////////

#define XX(camelCaseName, snakeCaseName) \
    REGISTER_INTERNED_ATTRIBUTE(snakeCaseName, EInternedAttributeKey::camelCaseName)

FOR_EACH_INTERNED_ATTRIBUTE(XX)

#undef XX

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
