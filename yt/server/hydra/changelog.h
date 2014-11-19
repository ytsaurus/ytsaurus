#pragma once

#include "public.h"

#include <core/misc/ref.h>

#include <core/actions/future.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

///! Represents a changelog, that is an ordered sequence of records.
struct IChangelog
    : public virtual TRefCounted
{
    //! Returns the meta blob.
    virtual TSharedRef GetMeta() const = 0;

    //! Returns the number of records in the changelog.
    virtual int GetRecordCount() const = 0;

    //! Returns an approximate byte size in a changelog.
    virtual i64 GetDataSize() const = 0;

    //! Returns |true| if the changelog is sealed, i.e.
    //! no further appends are possible.
    virtual bool IsSealed() const = 0;

    //! Asynchronously appends a record to the changelog.
    /*!
     *  \param recordId Record ids must be contiguous.
     *  \param data Record data
     *  \returns an asynchronous flag either indicating an error or
     *  a successful flush of the just appended record.
     */
    virtual TAsyncError Append(const TSharedRef& data) = 0;

    //! Asynchronously flushes all previously appended records.
    /*!
     *  \returns an asynchronous flag either indicating an error or
     *  a successful flush of the just appended record.
     */
    virtual TAsyncError Flush() = 0;

    //! Synchronously reads records from the changelog.
    //! The call may return less records than requested.
    //! This call throws on error.
    /*!
     *  \param firstRecordId The record id to start from.
     *  \param maxRecords A hint limits the number of records to read.
     *  \param maxBytes A hint limiting the number of bytes to read.
     *  \returns A list of records.
     */
    virtual std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const = 0;

    //! Asynchronously seals the changelog flushing and truncating it if necessary.
    /*!
     *  \returns an asynchronous flag either indicating an error or a success.
     */
    virtual TAsyncError Seal(int recordCount) = 0;

    //! Asynchronously resets seal flag.
    /*!
     *  Mostly useful for administrative tools.
     */
    virtual TAsyncError Unseal() = 0;

    //! Asynchronously flushes and closes the changelog, releasing all underlying resources.
    /*
     *  Examining the result is useful when a certain underlying implementation is expected.
     *  E.g. if this changelog is backed by a local file, the returned promise is set
     *  when the file is closed.
     */
    virtual TAsyncError Close() = 0;

};

DEFINE_REFCOUNTED_TYPE(IChangelog)

////////////////////////////////////////////////////////////////////////////////

//! Manages a collection of changelogs within a cell.
struct IChangelogStore
    : public virtual TRefCounted
{
    //! Creates a new changelog.
    virtual TFuture<TErrorOr<IChangelogPtr>> CreateChangelog(
        int id,
        const TSharedRef& meta) = 0;

    //! Opens an existing changelog.
    virtual TFuture<TErrorOr<IChangelogPtr>> OpenChangelog(int id) = 0;

    //! Scans for the maximum contiguous sequence of existing
    //! changelogs starting from #initialId and returns the id of the latest one.
    //! Returns |NonexistingSegmentId| if the initial changelog does not exist.
    virtual TFuture<TErrorOr<int>> GetLatestChangelogId(int initialId) = 0;


    // Extension methods.

    //! Opens an existing changelog.
    //! If the requested changelog is not found then returns |nullptr|.
    TFuture<TErrorOr<IChangelogPtr>> TryOpenChangelog(int id);

};

DEFINE_REFCOUNTED_TYPE(IChangelogStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
