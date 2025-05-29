#pragma once

#include <library/cpp/yson/node/node.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    using TNamedExpressions = THashMap<TString, NYT::TNode>;

    struct TEnvironment {
        // Given `{ "$x": "{ "Data": "foo" }" }`,
        // it will contain `{ "$x": "foo" }`
        TNamedExpressions Parameters;
    };

} // namespace NSQLComplete
