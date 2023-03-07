#pragma once

#include "private.h"

#include <yt/core/yson/public.h>

// Save membership information of the AST into YSON recursive list in order to replicate it in the remote subquery
// counterpart. This is a temporary workaround for the issue which is described in the following tickets:
// * https://st.yandex-team.ru/CHYT-154
// * https://github.com/yandex/ClickHouse/issues/5976
// * https://github.com/yandex/ClickHouse/issues/6090
//
// TODO(max42): remove this stuff when it is no longer needed.

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString DumpMembershipHint(const DB::IAST& ast, const NLogging::TLogger& logger);

void ApplyMembershipHint(DB::IAST& ast, const NYson::TYsonString& hint, const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
