#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/enum.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EAsyncIOState,
    (Created)
    (Started)
    (Stopped)
    (StartAborted)
);

struct IFDWatcher
    : public virtual TRefCounted
{
    virtual void Start(ev::dynamic_loop& eventLoop) = 0;
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFDWatcher);

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

    TSpinLock FlagsLock_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
