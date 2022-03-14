#pragma once

#include "public.h"

#include <util/stream/file.h>
#include <util/stream/output.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

struct IStreamLogOutput
    : public IOutputStream
    , public virtual TRefCounted
{ };

DEFINE_REFCOUNTED_TYPE(IStreamLogOutput)

////////////////////////////////////////////////////////////////////////////////

class TFixedBufferFileOutput
    : public IStreamLogOutput
{
public:
    inline TFixedBufferFileOutput(TFile file, size_t buf = 8192)
        : Underlying_(buf, file)
    {
        Underlying_.SetFinishPropagateMode(true);
    }

private:
    TBuffered<TUnbufferedFileOutput> Underlying_;

    void DoWrite(const void* buf, size_t len) override;
    void DoFlush() override;
    void DoFinish() override;
};

DECLARE_REFCOUNTED_TYPE(TFixedBufferFileOutput)
DEFINE_REFCOUNTED_TYPE(TFixedBufferFileOutput)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
