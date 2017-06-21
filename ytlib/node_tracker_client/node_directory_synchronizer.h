#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

namespace NYT {
namespace NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TNodeDirectorySynchronizer
    : public TRefCounted
{
public:
    TNodeDirectorySynchronizer(
        TNodeDirectorySynchronizerConfigPtr config,
        NApi::IConnectionPtr directoryConnection,
        TNodeDirectoryPtr nodeDirectory);
    ~TNodeDirectorySynchronizer();

    void Start();
    void Stop();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TNodeDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT
