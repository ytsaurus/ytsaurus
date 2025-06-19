#pragma once

#include <contrib/ydb/core/audit/audit_log_service.h>
#include <contrib/ydb/core/testlib/actors/test_runtime.h>
#include <contrib/ydb/library/actors/interconnect/interconnect.h>

namespace NActors {

    class TTestBasicRuntime : public TTestActorRuntime {
    public:
        using TTestActorRuntime::TTestActorRuntime;

        using TNodeLocationCallback = std::function<TNodeLocation(ui32)>;
        TNodeLocationCallback LocationCallback;

        ~TTestBasicRuntime();

        void Initialize(TEgg) override;

        void AddICStuff();
    };
}
