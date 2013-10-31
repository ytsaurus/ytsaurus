#pragma once

#include <core/misc/common.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueryManager;
typedef TIntrusivePtr<TQueryManager> TQueryManagerPtr;

class TQueryAgentConfig;
typedef TIntrusivePtr<TQueryAgentConfig> TQueryAgentConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT
