#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/enum.h>

// should replace with forward declaration
#include <contrib/libev/ev++.h>

namespace NYT {
namespace NPipes {

DECLARE_ENUM(EAsyncIOState, (Created)(Started)(Stopped)(StartAborted));

class TAsyncIOBase
    : public IFDWatcher
{
public:
    TAsyncIOBase();
    ~TAsyncIOBase();

    void Register();
    void Unregister();
protected:
    virtual void Start(ev::dynamic_loop& eventLoop) override;
    virtual void Stop() override;

    virtual void DoStart(ev::dynamic_loop& eventLoop) = 0;
    virtual void DoStop() = 0;

    virtual void OnRegistered(TError status) = 0;
    virtual void OnUnregister(TError status) = 0;

    EAsyncIOState State_;

    TSpinLock FlagsLock;
};

} // NPipes
} // NYT
