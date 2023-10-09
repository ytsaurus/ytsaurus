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

    //! Similar to IChangelog::Close flushes all the data (and the index)
    //! but does not close the files.
    virtual TFuture<void> Finish() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileChangelog)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
