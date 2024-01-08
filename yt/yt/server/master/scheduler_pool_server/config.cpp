#include "config.h"

#include <contrib/libs/re2/re2/re2.h>

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

void TDynamicSchedulerPoolManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_scheduler_pool_subtree_size", &TThis::MaxSchedulerPoolSubtreeSize)
        .Default(1000);

    registrar.Parameter("pool_name_regex_for_users", &TThis::PoolNameRegexForUsers)
        .Default("[-_a-z0-9]+");
    registrar.Parameter("pool_name_regex_for_administrators", &TThis::PoolNameRegexForAdministrators)
        .Default("[-_a-z0-9:A-Z]+");

    registrar.Postprocessor([] (TThis* config) {
        re2::RE2 dummy1(config->PoolNameRegexForUsers);
        if (!dummy1.ok()) {
            THROW_ERROR_EXCEPTION("Failed to parse pool name validation regex for users")
                << TErrorAttribute("regular_expression", config->PoolNameRegexForUsers)
                << TErrorAttribute("inner_error", dummy1.error());
        }

        re2::RE2 dummy2(config->PoolNameRegexForAdministrators);
        if (!dummy2.ok()) {
            THROW_ERROR_EXCEPTION("Failed to parse pool name validation regex for administrators")
                << TErrorAttribute("regular_expression", config->PoolNameRegexForAdministrators)
                << TErrorAttribute("inner_error", dummy2.error());
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
