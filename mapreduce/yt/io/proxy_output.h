#pragma once

#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProxyOutput
{
public:
    virtual ~TProxyOutput()
    { }

    virtual size_t GetStreamCount() const = 0;
    virtual TOutputStream* GetStream(size_t tableIndex) = 0;
    virtual void OnRowFinished(size_t tableIndex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
