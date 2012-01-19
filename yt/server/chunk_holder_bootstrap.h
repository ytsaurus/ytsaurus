#pragma once

#include <yt/ytlib/chunk_holder/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TChunkHolderBootstrap
{
public:
    typedef NChunkHolder::TChunkHolderConfig TConfig;

    TChunkHolderBootstrap(
        const Stroka& configFileName,
        TConfig* config);

    void Run();

private:
    Stroka ConfigFileName;
    TConfig::TPtr Config;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
