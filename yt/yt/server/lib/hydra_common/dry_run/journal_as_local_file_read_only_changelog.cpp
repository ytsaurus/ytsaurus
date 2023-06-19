#include "journal_as_local_file_read_only_changelog.h"

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/pull_parser_deserialize.h>

#include <util/stream/file.h>

namespace NYT::NHydra {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TJournalAsLocalFileReadOnlyChangelog::TJournalAsLocalFileReadOnlyChangelog(int changelogId)
    : ChangelogId_(changelogId)
{ }

void TJournalAsLocalFileReadOnlyChangelog::Open(const TString& path)
{
    TFileInput input(path);
    TYsonPullParser parser(&input, EYsonType::ListFragment);
    TYsonPullParserCursor cursor(&parser);
    YT_VERIFY(cursor.TryConsumeFragmentStart());
    static const TString PayloadKey("payload");
    while (!cursor->IsEndOfStream()) {
        cursor.ParseMap([&] (TYsonPullParserCursor* cursor) {
            auto valueType = (*cursor)->GetType();
            if (valueType != EYsonItemType::StringValue) {
                THROW_ERROR_EXCEPTION(
                    "Unexpected value type: %Qv, expected: %Qv",
                    valueType,
                    EYsonItemType::StringValue);
            }

            if (auto key = (*cursor)->UncheckedAsString(); key != PayloadKey) {
                THROW_ERROR_EXCEPTION("Expected %Qv but got %Qv while parsing changelog from YSON",
                    PayloadKey,
                    key);
            }
            cursor->Next();

            auto payload = ExtractTo<TString>(cursor);
            auto record = TSharedRef::FromString(std::move(payload));
            TotalBytes_ += record.Size();
            Records_.push_back(record);
        });
    }
}

int TJournalAsLocalFileReadOnlyChangelog::GetId() const
{
    return ChangelogId_;
}

const NHydra::NProto::TChangelogMeta& TJournalAsLocalFileReadOnlyChangelog::GetMeta() const
{
    static const NHydra::NProto::TChangelogMeta Meta;
    return Meta;
}

int TJournalAsLocalFileReadOnlyChangelog::GetRecordCount() const
{
    return std::ssize(Records_);
}

i64 TJournalAsLocalFileReadOnlyChangelog::GetDataSize() const
{
    return TotalBytes_;
}

TFuture<std::vector<TSharedRef>> TJournalAsLocalFileReadOnlyChangelog::Read(
    int firstRecordId,
    int maxRecords,
    i64 maxBytes) const
{
    std::vector<TSharedRef> result;
    i64 bytes = 0;
    for (int index = firstRecordId; index < std::ssize(Records_); ++index) {
        const auto& record = Records_[index];
        bytes += record.Size();
        result.push_back(record);
        if (std::ssize(result) >= maxRecords || bytes >= maxBytes) {
            break;
        }
    }
    return MakeFuture(std::move(result));
}

TFuture<void> TJournalAsLocalFileReadOnlyChangelog::Close()
{
    return VoidFuture;
}

TFuture<void> TJournalAsLocalFileReadOnlyChangelog::Append(TRange<TSharedRef> /*records*/)
{
    YT_ABORT();
}

TFuture<void> TJournalAsLocalFileReadOnlyChangelog::Flush()
{
    YT_ABORT();
}

TFuture<void> TJournalAsLocalFileReadOnlyChangelog::Truncate(int /*recordCount*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

IChangelogPtr CreateJournalAsLocalFileReadOnlyChangelog(const TString& path, int changelogId)
{
    auto changelog = New<TJournalAsLocalFileReadOnlyChangelog>(changelogId);
    changelog->Open(path);
    return changelog;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
