#pragma once

#include <yt/yt/core/misc/common.h>

#include <util/system/types.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOperationType,
    (Read)
    (Write)
);

DEFINE_ENUM(EDriverType,
    (Memcpy)
    (Rw)
    (Prwv2)
    (Async)
    (Uring)
);

DEFINE_ENUM(EPattern,
    (Sequential)
    (Linear)
    (Random)
);

DEFINE_ENUM(ESyncMode,
    (None)
    (Dsync)
    (Sync)
);

DEFINE_ENUM(EFallocateMode,
    (None)
    (Fallocate)
    (Zero)
    (Fill)
);

DEFINE_ENUM(EBurstAction,
    (Continue)
    (Wait)
    (Stop)
);

struct TOperation
{
    EOperationType Type;
    unsigned FileIndex;
    i64 Offset;
    i64 Size;
    EBurstAction BurstAction = EBurstAction::Continue;

    operator bool ()
    {
        return BurstAction != EBurstAction::Stop;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
