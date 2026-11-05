#include "private.h"

#include "interned_attributes.h"

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

const std::string ExecProgramName("ytserver-exec");
const std::string JobProxyProgramName("ytserver-job-proxy");

////////////////////////////////////////////////////////////////////////////////

const std::string BanMessageAttributeName("ban_message");
const std::string ConfigAttributeName("config");

////////////////////////////////////////////////////////////////////////////////

#define XX(camelCaseName, snakeCaseName) \
    REGISTER_INTERNED_ATTRIBUTE(snakeCaseName, EInternedAttributeKey::camelCaseName);

FOR_EACH_INTERNED_ATTRIBUTE(XX)

#undef XX

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
