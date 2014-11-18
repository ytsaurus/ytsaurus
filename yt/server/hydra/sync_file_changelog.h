#pragma once

#include "private.h"

#include <core/misc/ref.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! A fully synchronous file-based changelog implementation.
/*!
 *  The instances are fully thread-safe, at the cost of taking mutex on each invocation.
 *  Thus even trivial getters like #GetRecordCount are pretty expensive.
 *
 *  See IChangelog for a similar partly asynchronous interface.
 */
class TSyncFileChangelog
    : public TRefCounted
{
public:
    //! Basic constructor.
    TSyncFileChangelog(
        const Stroka& fileName,
        TFileChangelogConfigPtr config);

    ~TSyncFileChangelog();

    //! Returns the configuration passed in ctor.
    TFileChangelogConfigPtr GetConfig();

    //! Returns the data file name of the changelog.
    const Stroka& GetFileName() const;

    //! Opens an existing changelog.
    //! Throws an exception on failure.
    void Open();

    //! Closes the changelog.
    void Close();

    //! Creates a new changelog.
    //! Throws an exception on failure.
    void Create(const TSharedRef& meta);

    //! Returns the number of records in the changelog.
    int GetRecordCount() const;

    //! Returns an approximate byte size of a changelog.
    i64 GetDataSize() const;

    //! Returns the meta blob.
    TSharedRef GetMeta() const;

    //! Returns |true| if the changelog is sealed, i.e.
    //! no further appends are possible.
    bool IsSealed() const;

    //! Synchronously appends records to the changelog.
    void Append(
        int firstRecordId,
        const std::vector<TSharedRef>& records);

    //! Flushes the changelog.
    void Flush();

    //! Returns the timestamp of the last flush.
    TInstant GetLastFlushed();

    //! Synchronously reads at most #maxRecords records starting from record #firstRecordId.
    //! Stops if more than #maxBytes bytes are read.
    std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes);

    //! Synchronously seals the changelog truncating it if necessary.
    void Seal(int recordCount);

    //! Resets seal flag. Mostly useful for administrative tools.
    void Unseal();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TSyncFileChangelog)

////////////////////////////////////////////////////////////////////////////////

void RemoveChangelogFiles(const Stroka& dataFileName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
