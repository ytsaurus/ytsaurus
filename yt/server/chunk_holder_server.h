#pragma once

#include <yt/ytlib/chunk_holder/chunk_holder_service.h>

namespace NYT {

using NChunkHolder::TChunkHolderConfig;
using NChunkHolder::TChunkHolderService;

////////////////////////////////////////////////////////////////////////////////

class TChunkHolderServer
{
public:
    typedef TChunkHolderConfig TConfig;

    TChunkHolderServer(const TConfig& config);
    void Run();

private:
    TConfig Config;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
