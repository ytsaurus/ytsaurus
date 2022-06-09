#pragma once

#include "changelog.h"

#include <yt/yt/server/lib/io/io_engine.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Represents a changelog backed by a file.
struct IFileChangelog
    : public IChangelog
{
    //! Provides a direct IO access to fragments of changelog records.
    /*!
    *  \note
    *  Only flushed records are guaranteed to be accessible.
    */
    virtual NIO::IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const NIO::TChunkFragmentDescriptor& fragmentDescriptor) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileChangelog)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
