#pragma once
#include "defs.h"

#include <contrib/ydb/core/base/appdata_fwd.h>
#include <contrib/ydb/library/actors/core/mailbox.h>
#include <contrib/ydb/library/actors/core/executor_thread.h>

namespace NKikimr {

struct TActorSystemStub {
    THolder<NActors::TActorSystem> System;
    THolder<NActors::TMailbox> Mailbox;
    THolder<NActors::TExecutorThread> ExecutorThread;
    NActors::TActorId SelfID;
    THolder<NActors::TActorContext> Ctx;
    NActors::TActivationContext* PrevCtx;
    TAppData AppData;

    TActorSystemStub(std::shared_ptr<IRcBufAllocator> alloc = {});
    ~TActorSystemStub();
};

}
