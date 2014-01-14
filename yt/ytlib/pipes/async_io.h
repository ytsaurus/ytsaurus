#pragma once

#include "public.h"

#include <core/misc/error.h>

// should replace with forward declaration
#include <contrib/libev/ev++.h>

namespace NYT {
namespace NPipes {

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

    bool IsStartAborted() const;
    bool IsStarted() const;
    bool IsStopped() const;

    bool IsStartAborted_;
    bool IsStarted_;
    bool IsStopped_;

    TSpinLock FlagsLock;
};

} // NPipes
} // NYT
