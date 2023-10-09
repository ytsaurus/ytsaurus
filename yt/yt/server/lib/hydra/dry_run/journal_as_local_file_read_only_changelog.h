#pragma once

#include <yt/yt/server/lib/hydra/changelog.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TJournalAsLocalFileReadOnlyChangelog
    : public IChangelog
{
public:
    explicit TJournalAsLocalFileReadOnlyChangelog(int changelogId = -1);

    void Open(const TString& path);

    int GetId() const override;

    const NHydra::NProto::TChangelogMeta& GetMeta() const override;

    int GetRecordCount() const override;

    i64 GetDataSize() const override;

    TFuture<std::vector<TSharedRef>> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const override;

    TFuture<void> Close() override;

    // Not implemented.
    TFuture<void> Append(TRange<TSharedRef> /*records*/) override;
    TFuture<void> Flush() override;
    TFuture<void> Truncate(int /*recordCount*/) override;

private:
    std::vector<TSharedRef> Records_;
    i64 TotalBytes_ = 0;
    int ChangelogId_;
};

////////////////////////////////////////////////////////////////////////////////

IChangelogPtr CreateJournalAsLocalFileReadOnlyChangelog(const TString& path, int changelogId = -1);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
