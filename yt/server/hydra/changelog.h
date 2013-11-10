#pragma once

#include "public.h"

#include <core/actions/future.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

///! Represents a changelog, that is an ordered sequence of records.
struct IChangelog
    : public virtual TRefCounted
{
    //! Returns the changelog id.
    virtual int GetId() = 0;

    //! Returns the number of records in the changelog.
    virtual int GetRecordCount() = 0;

    //! Returns the number of records in the previous changelog;
    //! mostly for validation purposes.
    virtual int GetPrevRecordCount() = 0;  

    //! Returns |true| if the changelog is sealed, i.e.
    //! no further appends are possible.
    virtual bool IsSealed() = 0;

    //! Asynchronously appends a record to the changelog.
    /*!
     *  \param recordId Record ids must be contiguous.
     *  \param data Record data
     *  \returns a future that gets set when the record
     *  is actually flushed to a persistent storage.
     */
    virtual TFuture<void> Append(const TSharedRef& data) = 0;

    //! Asynchronously flushes all previously appended records.
    /*!
     *  \returns a future that gets set when all the records
     *  are actually flushed to a persistent storage.
     */
    virtual TFuture<void> Flush() = 0;

    //! Closes the changelog files.
    /*!
     *  Pending changes are not guaranteed to be flushed.
     */
    virtual void Close() = 0;

    //! Synchronously reads records from the changelog.
    //! The call may return less records than requested.
    /*!
     *  \param firstRecordId The record id to start from.
     *  \param maxRecords A hint limits the number of records to read.
     *  \param maxBytes A hint limiting the number of bytes to read.
     *  \returns A list of records.
     */
    virtual std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) = 0;

    //! Asynchronously seals the changelog flushing and truncating it if necessary.
    virtual TFuture<void> Seal(int recordCount) = 0;

    //! Resets seal flag. Mostly useful for administrative tools.
    virtual void Unseal() = 0;

};

////////////////////////////////////////////////////////////////////////////////

//! Parameters used for creating new changelogs.
struct TChangelogCreateParams
{
    TChangelogCreateParams();

    int PrevRecordCount;
};

//! Manages a collection of changelogs within a cell.
struct IChangelogStore
    : public virtual TRefCounted
{
    //! Returns the guid of the cell.
    virtual const TCellGuid& GetCellGuid() const = 0;

    //! Synchronously creates a new changelog.
    virtual IChangelogPtr CreateChangelog(
        int id,
        const TChangelogCreateParams& params) = 0;

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
