#include "file_resource_test.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/system/fstat.h>
#include <util/system/shellcommand.h>

namespace NYT::NFlow::NFileResourceTest {

////////////////////////////////////////////////////////////////////////////////

namespace {

std::string ReadFile(const TFsPath& path)
{
    auto value = TFileInput(path.GetPath()).ReadAll();
    return std::string(value.data(), value.size());
}

std::string ReadArchive(const TFsPath& archive)
{
    TTempDir directory;
    TShellCommand command("tar", {"-xf", archive.GetPath(), "-C", directory.Name()});
    command.Run().Wait();
    auto exitCode = command.GetExitCode();
    THROW_ERROR_EXCEPTION_UNLESS(
        exitCode.Defined() && *exitCode == 0,
        "Failed to unpack test file resource archive %Qv",
        archive.GetPath())
        .With("stderr", command.GetError());

    TVector<TFsPath> entries;
    TFsPath(directory.Name()).List(entries);
    Sort(entries, [] (const auto& lhs, const auto& rhs) {
        return lhs.GetPath() < rhs.GetPath();
    });
    THROW_ERROR_EXCEPTION_UNLESS(
        entries.size() == 2 && entries[0].IsFile() && entries[1].IsFile(),
        "Test file resource archive must contain exactly two regular files");
    return Format("%v|%v", ReadFile(entries[0]), ReadFile(entries[1]));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTextData::TTextData(std::string text)
    : Text(std::move(text))
{ }

TTextDataPtr TTestFileResource::Initialize(const TMaterializedFileSourceSnapshotPtr& fileSources)
{
    if (fileSources->GetFileSources().size() == 1) {
        TVector<TFsPath> entries;
        TFsPath(fileSources->GetOnlyFileSource()->GetRootPath()).List(entries);
        Sort(entries, [] (const auto& lhs, const auto& rhs) {
            return lhs.GetPath() < rhs.GetPath();
        });
        THROW_ERROR_EXCEPTION_UNLESS(
            !entries.empty() && AllOf(entries, [] (const auto& entry) {
                return entry.IsFile();
            }),
            "Test file resource expects regular files in its materialized root");

        if (entries.size() > 1) {
            std::vector<std::string> values;
            values.reserve(entries.size());
            for (const auto& entry : entries) {
                values.push_back(ReadFile(entry));
            }
            return New<TTextData>(JoinSeq("|", values));
        }

        const auto& file = entries.front();
        auto size = TFileStat(file, /*nofollow*/ true).Size;
        if (size >= 64_MB) {
            return New<TTextData>(Format("size:%v", size));
        }
        if (std::string(file.Basename()).ends_with(".tar")) {
            return New<TTextData>(ReadArchive(file));
        }
        return New<TTextData>(ReadFile(file));
    }

    std::vector<TFileSourceId> ids;
    ids.reserve(fileSources->GetFileSources().size());
    for (const auto& [id, _] : fileSources->GetFileSources()) {
        ids.push_back(id);
    }
    Sort(ids);

    std::vector<std::string> values;
    values.reserve(ids.size());
    for (const auto& id : ids) {
        TVector<TFsPath> entries;
        TFsPath(fileSources->GetFileSource(id)->GetRootPath()).List(entries);
        THROW_ERROR_EXCEPTION_UNLESS(
            entries.size() == 1 && entries.front().IsFile(),
            "Test file source %Qv expects exactly one regular file in its materialized root",
            id);
        values.push_back(ReadFile(entries.front()));
    }
    return New<TTextData>(JoinSeq("|", values));
}

void TTestFileResource::Validate(const TTextDataPtr& data)
{
    THROW_ERROR_EXCEPTION_IF(
        data->Text.contains("corrupt"),
        "Test file resource rejected corrupt payload");
}

////////////////////////////////////////////////////////////////////////////////

void TEnrichedMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("input", &TThis::Input)
        .Default();
    registrar.Parameter("file_text", &TThis::FileText)
        .Default();
    registrar.Parameter("resource_revision", &TThis::ResourceRevision)
        .Default();
    registrar.Parameter("file_snapshot_id", &TThis::FileSnapshotId)
        .Default();
}

void TEnrichWithFileFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    Resource_ = initContext->GetStaticResource("text")->As<TTestFileResource>();
}

void TEnrichWithFileFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto accessor = Resource_->Lock();
    auto enriched = New<TEnrichedMessage>();
    enriched->Input = GetColumnValue<std::string>(message, "text");
    enriched->FileText = accessor->Text;
    enriched->ResourceRevision = accessor.GetDeliveryRevisionId();
    enriched->FileSnapshotId = accessor.GetFileSnapshotId().Underlying();
    output->AddMessage(context->ConvertToMessage(enriched));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileResourceTest
