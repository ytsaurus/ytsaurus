#include "config.h"

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const int TServiceConfig::DefaultMaxAuthenticationQueueSize = 10000;

////////////////////////////////////////////////////////////////////////////////

const bool TMethodConfig::DefaultHeavy = false;
const auto TMethodConfig::DefaultResponseCodec = NCompression::ECodec::None;
const int TMethodConfig::DefaultMaxQueueSize = 10000;
const int TMethodConfig::DefaultMaxConcurrency = 1000;
const auto TMethodConfig::DefaultLogLevel = NLogging::ELogLevel::Debug;

////////////////////////////////////////////////////////////////////////////////

const int TDispatcherConfig::DefaultHeavyPoolSize = 16;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
