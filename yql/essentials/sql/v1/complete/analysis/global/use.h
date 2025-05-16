#pragma once

#include "parse_tree.h"

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    struct TUseContext {
        TString Provider;
        TString Cluster;
    };

    TMaybe<TUseContext> FindUseStatement(SQLv1::Sql_queryContext* ctx);

} // namespace NSQLComplete
