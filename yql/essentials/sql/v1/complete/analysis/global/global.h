#pragma once

#include <yql/essentials/sql/v1/complete/core/input.h>

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    struct TGlobalContext {
        struct TObject {
            TString Provider;
            TString Cluster;
        };

        TMaybe<TObject> Object;
    };

    struct TEnvironment {
    };

    class IGlobalAnalysis {
    public:
        using TPtr = THolder<IGlobalAnalysis>;

        virtual ~IGlobalAnalysis() = default;
        virtual TGlobalContext Analyze(TCompletionInput input) = 0;
    };

    IGlobalAnalysis::TPtr MakeGlobalAnalysis();

} // namespace NSQLComplete
