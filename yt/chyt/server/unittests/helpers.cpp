#include "helpers.h"

#include <DBPoco/Util/AbstractConfiguration.h>
#include <DBPoco/Util/XMLConfiguration.h>

namespace {

using ConfigurationPtr = DBPoco::AutoPtr<DBPoco::Util::AbstractConfiguration>;

////////////////////////////////////////////////////////////////////////////////

// NOTE(dakovalkov): SharedContextPart is a singletone. Creating it multiple times leads to std::terminate().
// Storing and initializing SharedContextHolder as a global variable is also a bad idea:
// DB::Context::createShared() uses some global variables from other compilation units,
// and an initialization order of such variables is unspecified.

// NOTE(dakovalkov): Can be called only once.
DB::ContextPtr InitGlobalContext()
{
    static DB::SharedContextHolder sharedContextHolder = DB::Context::createShared();
    DB::ContextMutablePtr globalContext = DB::Context::createGlobal(sharedContextHolder.get());
    ConfigurationPtr config(new DBPoco::Util::XMLConfiguration());
    globalContext->setConfig(config);
    globalContext->makeGlobalContext();
    return globalContext;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

namespace NYT::NClickHouseServer {

using namespace NYT::NQueryClient;

////////////////////////////////////////////////////////////////////////////////

DB::ContextPtr GetGlobalContext()
{
    static DB::ContextPtr globalContext = InitGlobalContext();
    return globalContext;
}

////////////////////////////////////////////////////////////////////////////////

void PrintTo(TConstExpressionPtr expr, ::std::ostream* os)
{
    *os << InferName(expr);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
