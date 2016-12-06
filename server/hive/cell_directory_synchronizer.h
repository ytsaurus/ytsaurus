#pragma once

#include "public.h"

#include <yt/ytlib/hive/public.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer
    : public TRefCounted
{
public:
    TCellDirectorySynchronizer(
        TCellDirectorySynchronizerConfigPtr config,
        NHiveClient::TCellDirectoryPtr cellDirectory,
        const TCellId& primaryCellId);
    ~TCellDirectorySynchronizer();

    void Start();
    void Stop();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
