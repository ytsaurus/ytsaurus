#pragma once

#include "public.h"

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcher
{
public:
    static TIODispatcher* Get();

    void Shutdown();

private:
    TIODispatcher();
    
    DECLARE_SINGLETON_FRIEND(TIODispatcher);
    friend class NDetail::TAsyncReaderImpl;
    friend class NDetail::TAsyncWriterImpl;

    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
