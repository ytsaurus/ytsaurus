#pragma once

#include "public.h"

#include <yt/core/misc/shutdownable.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcher
    : public IShutdownable
{
public:
    ~TIODispatcher();

    static TIODispatcher* Get();

    static void StaticShutdown();

    virtual void Shutdown() override;

private:
    TIODispatcher();

    Y_DECLARE_SINGLETON_FRIEND();

    friend class NDetail::TAsyncReaderImpl;
    friend class NDetail::TAsyncWriterImpl;

    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
