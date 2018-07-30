#pragma once

#include <Interpreters/ISecurityManager.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::ISecurityManager> CreateSecurityManager();

} // namespace NClickHouse
} // namespace NYT
