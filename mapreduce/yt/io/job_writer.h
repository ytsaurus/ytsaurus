#pragma once

#include "proxy_output.h"

#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/stream/buffered.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TJobWriter
    : public TProxyOutput
{
public:
    explicit TJobWriter(size_t outputTableCount);

    size_t GetStreamCount() const override;
    TOutputStream* GetStream(size_t tableIndex) const override;
    void OnRowFinished(size_t tableIndex) override;

private:
    struct TStream {
        TFile FdFile;
        TFileOutput FdOutput;
        TBufferedOutput BufferedOutput;

        explicit TStream(int fd);
        ~TStream();

        static const size_t BUFFER_SIZE = 1 << 20;
    };

    yvector<THolder<TStream>> Streams_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
