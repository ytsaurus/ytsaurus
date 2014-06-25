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

    //! Synchronously closes the changelog files.
    /*!
     *  Pending changes are not guaranteed to be flushed.
     */
    virtual void Close() = 0;

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

};

DEFINE_REFCOUNTED_TYPE(IChangelog)

////////////////////////////////////////////////////////////////////////////////

//! Manages a collection of changelogs within a cell.
struct IChangelogStore
    : public virtual TRefCounted
{
    //! Returns the guid of the cell.
    virtual const TCellGuid& GetCellGuid() const = 0;

    //! Synchronously creates a new changelog.
    virtual IChangelogPtr CreateChangelog(
        int id,
        const TSharedRef& meta) = 0;

    //! Synchronously opens an existing changelog.
    //! Returns |nullptr| if not changelog is found.
    virtual IChangelogPtr TryOpenChangelog(int id) = 0;

    //! Given an initial id, scans for the maximum contiguous sequence of existing
    //! changelogs and returns the id of the latest one.
    //! Returns |NonexistingSegmentId| if the initial changelog does not exist.
    virtual int GetLatestChangelogId(int initialId) = 0;


    // Extension methods.

    //! Synchronously opens an existing changelog.
    //! Throws on failure.
    IChangelogPtr OpenChangelogOrThrow(int id);

};

DEFINE_REFCOUNTED_TYPE(IChangelogStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
