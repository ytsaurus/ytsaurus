#include "config.h"

namespace NYT::NRpc {

using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const int TServiceConfig::DefaultMaxAuthenticationQueueSize = 10000;

////////////////////////////////////////////////////////////////////////////////

const bool TMethodConfig::DefaultHeavy = false;
const NCompression::ECodec TMethodConfig::DefaultResponseCodec = NCompression::ECodec::None;
const int TMethodConfig::DefaultMaxQueueSize = 10000;
const int TMethodConfig::DefaultMaxConcurrency = 1000;
const NLogging::ELogLevel TMethodConfig::DefaultLogLevel = NLogging::ELogLevel::Debug;

////////////////////////////////////////////////////////////////////////////////

const int TDispatcherConfig::DefaultHeavyPoolSize = 16;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
