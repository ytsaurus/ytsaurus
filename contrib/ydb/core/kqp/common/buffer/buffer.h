#pragma once

#include <contrib/ydb/core/scheme/scheme_tabledefs.h>
#include <contrib/ydb/core/kqp/common/kqp_tx_manager.h>

#include <contrib/ydb/library/aclib/user_context.h>

namespace NKikimr {
namespace NKqp {

struct TKqpBufferWriterSettings {
    TActorId SessionActorId;
    IKqpTransactionManagerPtr TxManager;
    NWilson::TTraceId TraceId;
    ui64 QuerySpanId = 0;
    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<NTxProxy::TTxProxyMon> TxProxyMon;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    TIntrusivePtr<NACLib::TUserContext> UserCtx;
};

NActors::IActor* CreateKqpBufferWriterActor(TKqpBufferWriterSettings&& settings);

}
}
