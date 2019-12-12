#pragma once

#include "config.h"
#include "public.h"

namespace NYT::NLogTailer {

////////////////////////////////////////////////////////////////////////////////

class TLogTailer
    : public TIntrinsicRefCounted
{
public:
    TLogTailer(
        TBootstrap* bootstrap,
        TLogTailerConfigPtr config);

    void Run();

    const std::vector<TLogFileReaderPtr>& GetLogReaders() const;

private:
    TBootstrap* const Bootstrap_;

    TLogTailerConfigPtr Config_;

    TLogRotatorPtr LogRotator_;
    std::vector<TLogFileReaderPtr> LogReaders_;
};

DEFINE_REFCOUNTED_TYPE(TLogTailer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer

