#pragma once

#include "private.h"

#include <core/misc/ref.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! A fully synchronous file-based changelog implementation.
/*!
 *  See IChangelog for a similar partly asynchronous interface.
 */
class TSyncFileChangelog
    : public TRefCounted
{
public:
    //! Basic constructor.
    TSyncFileChangelog(
        const Stroka& fileName,
        int id,
        TFileChangelogConfigPtr config);

    ~TSyncFileChangelog();

    //! Returns the configuration passed in ctor.
    TFileChangelogConfigPtr GetConfig();

    //! Opens an existing changelog.
    //! Throws an exception on failure.
    void Open();

    //! Closes the changelog.
    void Close();

    //! Creates a new changelog.
    //! Throws an exception on failure.
    void Create(const TChangelogCreateParams& params);

    //! Returns the changelog id.
    int GetId() const;

    //! Returns the number of records in the changelog.
    int GetRecordCount() const;

    //! Returns an approximate byte size of a changelog.
    i64 GetDataSize() const;

    //! Returns the number of records in the previous changelog;
    //! mostly for validation purposes.
    int GetPrevRecordCount() const;

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
    std::unique_ptr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
