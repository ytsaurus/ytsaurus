#pragma once

#include "public.h"

#include <yt/ytlib/hydra/version.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/ref.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Represents a changelog, that is an ordered sequence of records.
/*!
 *  Except when speficially noted otherwise, all methods are assumed to
 *  be invoked from a single thread.
 */
struct IChangelog
    : public virtual TRefCounted
{
    //! Returns the meta.
    /*!
     *  Thread affinity: any
     */
    virtual const NProto::TChangelogMeta& GetMeta() const = 0;

    //! Returns the number of records in the changelog.
    /*!
     *  This includes appended but not yet flushed records as well.
     *
     *  Thread affinity: any
     */
    virtual int GetRecordCount() const = 0;

    //! Returns an approximate byte size in a changelog.
    /*!
     *  This includes appended but not yet flushed records as well.
     *
     *  Thread affinity: any
     */
    virtual i64 GetDataSize() const = 0;

    //! Asynchronously appends a record to the changelog.
    /*!
     *  \param recordId Record ids must be contiguous.
     *  \param data Record data
     *  \returns an asynchronous flag either indicating an error or
     *  a successful flush of the just appended record.
     */
    virtual TFuture<void> Append(const TSharedRef& data) = 0;

    //! Asynchronously flushes all previously appended records.
    /*!
     *  \returns an asynchronous flag either indicating an error or
     *  a successful flush of all the appended records.
     */
    virtual TFuture<void> Flush() = 0;

    //! Asynchronously reads records from the changelog.
    //! The call may return less records than requested.
    /*!
     *  \param firstRecordId The record id to start from.
     *  \param maxRecords A hint limits the number of records to read.
     *  \param maxBytes A hint limiting the number of bytes to read.
     *  \returns A list of records.
     */
    virtual TFuture<std::vector<TSharedRef>> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const = 0;

    //! Asynchronously flushes and truncates the changelog.
    /*!
     *  Most implementations will not allow appending more records to a truncated changelog.
     */
    virtual TFuture<void> Truncate(int recordCount) = 0;

    //! Asynchronously flushes and closes the changelog, releasing all underlying resources.
    /*
     *  Examining the result is useful when a certain underlying implementation is expected.
     *  E.g. if this changelog is backed by a local file, the returned promise is set
     *  when the file is closed.
     */
    virtual TFuture<void> Close() = 0;

};

DEFINE_REFCOUNTED_TYPE(IChangelog)

////////////////////////////////////////////////////////////////////////////////

//! Manages a collection of changelogs within a cell.
struct IChangelogStore
    : public virtual TRefCounted
{
    //! Returns the initial reachable version, i.e this is
    //! |(n,m)| if |n| is the maximum existing changelog id with |m| records in it.
    //! If no changelogs exist in the store then zero version is returned.
    //! This reachable version captures the initial state and is never updated.
    virtual TVersion GetReachableVersion() const = 0;

    //! Creates a new changelog.
    virtual TFuture<IChangelogPtr> CreateChangelog(int id, const NProto::TChangelogMeta& meta) = 0;

    //! Opens an existing changelog.
    /*!
     *  The resulting changelog cannot be appended but can be truncated.
     *
     *  It is also possible that some writers are concurrently appending to this changelog.
     *  Note that IChangelog::GetRecordCount may return, in this case, the number of records
     *  that were present at the moment when the changelog was opened.
     */
    virtual TFuture<IChangelogPtr> OpenChangelog(int id) = 0;

    // Extension methods.

    //! Opens an existing changelog.
    //! If the requested changelog is not found then returns |nullptr|.
    TFuture<IChangelogPtr> TryOpenChangelog(int id);

};

DEFINE_REFCOUNTED_TYPE(IChangelogStore)

////////////////////////////////////////////////////////////////////////////////

//! Enables constructing IChangelogStore instances.
struct IChangelogStoreFactory
    : public virtual TRefCounted
{
    //! Creates a changelog store but, more importantly,
    //! induces a barrier such that no record added via IChangelog instances
    //! obtained from IChangelogStore prior to this #Lock call may penetrate.
    virtual TFuture<IChangelogStorePtr> Lock() = 0;

};

DEFINE_REFCOUNTED_TYPE(IChangelogStoreFactory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
