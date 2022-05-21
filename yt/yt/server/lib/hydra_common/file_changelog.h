#pragma once

#include "private.h"

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/core/misc/ref.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFileChangelogFormat,
    (V5)
);

//! An unbuffered file-based changelog implementation.
/*!
 *  The instances are single-threaded unless noted otherwise.
 *  See IChangelog for a similar partly asynchronous interface.
 */
struct IFileChangelog
    : public virtual TRefCounted
{
    //! Returns the configuration.
    /*
     *  \note
     *  Thread affinity: any
     */
    virtual const TFileChangelogConfigPtr& GetConfig() const = 0;

    //! Returns the data file name of the changelog.
    /*
     *  \note
     *  Thread affinity: any
     */
    virtual const TString& GetFileName() const = 0;

    //! Opens an existing changelog.
    virtual void Open() = 0;

    //! Closes the changelog.
    virtual void Close() = 0;

    //! Creates a new changelog.
    virtual void Create(
        const NProto::TChangelogMeta& meta,
        EFileChangelogFormat format = EFileChangelogFormat::V5) = 0;

    //! Returns the number of records in the changelog.
    /*
     *  \note
     *  Thread affinity: any
     */
    virtual int GetRecordCount() const = 0;

    //! Returns an approximate byte size of a changelog.
    /*
     *  \note
     *  Thread affinity: any
     */
    virtual i64 GetDataSize() const = 0;

    //! Returns |true| is the changelog is open.
    /*
     *  \note
     *  Thread affinity: any
     */
    virtual bool IsOpen() const = 0;

    //! Returns the meta.
    virtual const NProto::TChangelogMeta& GetMeta() const = 0;

    //! Appends records to the changelog.
    virtual void Append(
        int firstRecordIndex,
        const std::vector<TSharedRef>& records) = 0;

    //! Flushes the changelog.
    virtual void Flush() = 0;

    //! Reads at most #maxRecords records starting from record #firstRecordIndex.
    //! Stops if more than #maxBytes bytes are read.
    virtual std::vector<TSharedRef> Read(
        int firstRecordIndex,
        int maxRecords,
        i64 maxBytes) = 0;

    //! Truncates the changelog to #recordCount records.
    virtual void Truncate(int recordCount) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileChangelog)

////////////////////////////////////////////////////////////////////////////////

IFileChangelogPtr CreateFileChangelog(
    NIO::IIOEnginePtr ioEngine,
    TString fileName,
    TFileChangelogConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
