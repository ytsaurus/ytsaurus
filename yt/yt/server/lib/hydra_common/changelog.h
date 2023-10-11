#pragma once

#include "public.h"

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Represents a changelog, that is an ordered sequence of records.
/*!
 *  Except when specifically noted otherwise, all methods are assumed to
 *  be invoked from a single thread.
 */
struct IChangelog
    : public virtual TRefCounted
{
    //! Returns the changelog id.
    /*!
     *  Some implementations lack meaningful id and return -1.
     */
    virtual int GetId() const = 0;

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
     *  \param records Records data
     *  \returns an asynchronous flag either indicating an error or
     *  a successful flush of all just appended records.
     */
    virtual TFuture<void> Append(TRange<TSharedRef> records) = 0;

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

struct TChangelogOptions
{
    //! If true, open/create requests to changelog store will return a changelog instance
    //! that is ready to accept and flush new records with minimal latency.
    bool CreateWriterEagerly = false;
};

//! Manages a collection of changelogs within a cell.
struct IChangelogStore
    : public virtual TRefCounted
{
    //! Returns |true| is the store is created in read-only mode.
    //! This is possible for remote stores instantiated by non-voting tablet cells.
    virtual bool IsReadOnly() const = 0;

    //! Returns the current term.
    //! If the store is read-only then |std::nullopt| is returned.
    /*!
     *  This is initially loaded from a persistent storage and is updated via #SetTerm.
     */
    virtual std::optional<int> TryGetTerm() const = 0;

    //! Asynchronously updates the term in persistent storage.
    /*!
     *  Once the update succeeds, #GetTerm starts returning a new value.
     */
    virtual TFuture<void> SetTerm(int term) = 0;

    //! Returns the maximum existing changelog id or |InvalidSegmentId| if no changelogs are known.
    //! If the store is read-only then |std::nullopt| is returned.
    /*!
     *  This is initially computed from a persistent storage and is updated via #CreateChangelog.
     */
    virtual std::optional<int> TryGetLatestChangelogId() const = 0;

    //! Returns the initial reachable state, i.e this is
    //! |(t, n, m)| where |t| is changelog term,
    // |n| is the maximum existing nonempty changelog id
    // and |m| is the sequence number of the last record in it.
    //! If no changelog exists in the store then |(0, 0, 0)| is returned.
    //! If the store is read-only then |std::nullopt| is returned.
    //! This reachable state captures the initial state and is never updated.
    virtual std::optional<TElectionPriority> GetElectionPriority() const = 0;

    //! Returns the initial reachable version, i.e this is
    //! |(n,m)| if |n| is the maximum existing changelog id with |m| records in it.
    //! If no changelog exists in the store then zero version is returned.
    //! If the store is read-only then |std::nullopt| is returned.
    //! This reachable version captures the initial state and is never updated.
    virtual std::optional<TVersion> GetReachableVersion() const = 0;

    //! Creates a new changelog.
    virtual TFuture<IChangelogPtr> CreateChangelog(
        int id,
        const NProto::TChangelogMeta& meta,
        const TChangelogOptions& options = {}) = 0;

    //! Opens an existing changelog.
    virtual TFuture<IChangelogPtr> OpenChangelog(
        int id,
        const TChangelogOptions& options = {}) = 0;

    //! Removes an existing changelog.
    virtual TFuture<void> RemoveChangelog(int id) = 0;

    // Extension methods.

    //! Delegates to #TryGetTerm, throws on null.
    int GetTermOrThrow() const;

    //! Delegates to #TryGetLatestChangelogId, throws on null.
    int GetLatestChangelogIdOrThrow() const;

    //! Opens an existing changelog.
    //! If the requested changelog is not found then returns |nullptr|.
    TFuture<IChangelogPtr> TryOpenChangelog(
        int id,
        const TChangelogOptions& options = {});

    //! Aborts the locks being held by the instance; cf. #IChangelogStoreFactory::Lock.
    virtual void Abort() = 0;
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

} // namespace NYT::NHydra
