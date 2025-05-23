#pragma once

#include "parse_tree.h"

#include <yql/essentials/sql/v1/complete/core/environment.h>

#include <util/generic/maybe.h>

namespace NSQLComplete {

    TMaybe<TValue> Evaluate(SQLv1::Bind_parameterContext* ctx, const TEnvironment& env);

} // namespace NSQLComplete
