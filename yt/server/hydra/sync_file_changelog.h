#pragma once

#include "private.h"

#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/hydra/hydra_manager.pb.h>

#include <yt/core/misc/ref.h>

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
        const NChunkClient::IIOEnginePtr& ioEngine,
        const TString& fileName,
        TFileChangelogConfigPtr config);

    ~TSyncFileChangelog();

    //! Returns the configuration passed in ctor.
    const TFileChangelogConfigPtr& GetConfig();

    //! Returns the data file name of the changelog.
    const TString& GetFileName() const;

    //! Opens an existing changelog.
    //! Throws an exception on failure.
    void Open();

    //! Closes the changelog.
    void Close();

    //! Creates a new changelog.
    //! Throws an exception on failure.
    void Create(const NProto::TChangelogMeta& meta);

    //! Returns the number of records in the changelog.
    int GetRecordCount() const;

    //! Returns an approximate byte size of a changelog.
    i64 GetDataSize() const;

    //! Returns |true| is the changelog is open.
    bool IsOpen() const;

    //! Returns the meta blob.
    const NProto::TChangelogMeta& GetMeta() const;

    //! Synchronously appends records to the changelog.
    void Append(
        int firstRecordId,
        const std::vector<TSharedRef>& records);

    //! Flushes the changelog.
    void Flush();

    //! Synchronously reads at most #maxRecords records starting from record #firstRecordId.
    //! Stops if more than #maxBytes bytes are read.
    std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes);

    //! Synchronously seals the changelog truncating it if necessary.
    void Truncate(int recordCount);

    void Preallocate(size_t size);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TSyncFileChangelog)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
