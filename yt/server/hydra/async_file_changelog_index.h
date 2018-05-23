#pragma once

#include "format.h"
#include "file_helpers.h"

#include <yt/ytlib/chunk_client/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TAsyncFileChangelogIndex
{
public:
    TAsyncFileChangelogIndex(
        const NChunkClient::IIOEnginePtr& IOEngine,
        const TString& fileName,
        i64 indexBlockSize);

    void Create();
    void FlushData();
    void Close();

    void Append(int firstRecordId, i64 filePosition, const std::vector<int>& appendSizes);
    void Append(int recordId, i64 filePosition, int recordSize);
    bool IsEmpty() const;
    const TChangelogIndexRecord& LastRecord() const;
    const std::vector<TChangelogIndexRecord>& Records() const;

    void Search(
        TChangelogIndexRecord* lowerBound,
        TChangelogIndexRecord* upperBound,
        int firstRecordId,
        int lastRecordId,
        i64 maxBytes = -1) const;

    void Read(const TNullable<i32>& truncatedRecordCount = Null);
    void TruncateInvalidRecords(i64 correctPrefixSize);

private:
    void ProcessRecord(int recordId, i64 filePosition, int recordSize);
    void UpdateIndexHeader();

    const NChunkClient::IIOEnginePtr IOEngine_;
    const TString IndexFileName_;
    const i64 IndexBlockSize_;

    std::vector<TChangelogIndexRecord> Index_;
    std::unique_ptr<TFile> IndexFile_;

    i64 CurrentBlockSize_ = 0;

    bool OldFormat_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
