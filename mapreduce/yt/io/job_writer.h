#pragma once

#include "proxy_output.h"

#include <util/generic/vector.h>
#include <util/generic/ptr.h>

class TPipedOutput;
class TBufferedOutput;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TJobWriter
    : public TProxyOutput
{
public:
    explicit TJobWriter(size_t outputTableCount);

    virtual size_t GetStreamCount() const override;
    virtual TOutputStream* GetStream(size_t tableIndex) override;
    virtual void OnRowFinished(size_t tableIndex) override;

private:
    struct TStream {
        TSimpleSharedPtr<TPipedOutput> PipedOutput;
        TSimpleSharedPtr<TBufferedOutput> BufferedOutput;

        explicit TStream(int fd);

        static const size_t BUFFER_SIZE = 1 << 20;
    };

    yvector<TStream> Streams_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
