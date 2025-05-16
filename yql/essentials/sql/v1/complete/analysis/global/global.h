#pragma once

#include <yql/essentials/sql/v1/complete/core/input.h>

#include <util/generic/ptr.h>

namespace NSQLComplete {

    struct TGlobalContext {
        TString Cluster;
    };

    class IGlobalAnalysis {
    public:
        using TPtr = THolder<IGlobalAnalysis>;

        virtual ~IGlobalAnalysis() = default;
        virtual TGlobalContext Analyze(TCompletionInput input) = 0;
    };

    IGlobalAnalysis::TPtr MakeLocalSyntaxAnalysis();

} // namespace NSQLComplete
