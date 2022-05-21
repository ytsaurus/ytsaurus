#pragma once

#include "public.h"

#include <util/stream/output.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Wraps another IOutputStream and measures the number of bytes
//! written through it.
class TLengthMeasuringOutputStream
    : public IOutputStream
{
public:
    explicit TLengthMeasuringOutputStream(IOutputStream* output);

    i64 GetLength() const;

protected:
    void DoWrite(const void* buf, size_t len) override;
    void DoFlush() override;
    void DoFinish() override;

private:
    IOutputStream* const Output_;

    i64 Length_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

void RemoveChangelogFiles(const TString& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
