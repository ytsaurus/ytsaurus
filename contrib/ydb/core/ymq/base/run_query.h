#pragma once

#include <contrib/ydb/core/kqp/common/kqp.h>
#include <contrib/ydb/public/lib/deprecated/kicli/kicli.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/actors/core/actor.h>


namespace NKikimr::NSQS {

    void RunYqlQuery(
        const TString& query,
        std::optional<NYdb::TParams> params,
        bool readonly,
        TDuration sendAfter,
        const TString& database,
        const TActorContext& ctx
    );

} // namespace NKikimr::NSQS
