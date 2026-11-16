#pragma once

#include "dq_factories.h"

#include <yql/essentials/minikql/comp_nodes/test_support/mkql_test_setup.h>

#include <contrib/ydb/library/yql/dq/comp_nodes/dq_program_builder.h>


namespace NKikimr::NMiniKQL {

template<bool LLVM, bool Spilling = false>
struct TDqSetup: public TSetup<LLVM, Spilling> {
    explicit TDqSetup(TComputationNodeFactory nodeFactory = GetDqNodeFactory(), TVector<TUdfModuleInfo>&& modules = {})
        : TSetup<LLVM, Spilling>(nodeFactory, std::move(modules)) {
        this->PgmBuilder = MakeHolder<TDqProgramBuilder>(*this->Env, *this->FunctionRegistry);
    }

    TDqProgramBuilder& GetDqProgramBuilder() {
        return static_cast<TDqProgramBuilder&>(*this->PgmBuilder);
    }
};

} // namespace NKikimr::NMiniKQL
