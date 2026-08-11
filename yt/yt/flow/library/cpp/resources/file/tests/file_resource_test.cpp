#include "file_resource_test.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
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

TTextDataPtr TTestFileResource::Initialize(const TMaterializedDirectoryPtr& directory)
{
    TVector<TFsPath> entries;
    TFsPath(directory->GetRootPath()).List(entries);
    THROW_ERROR_EXCEPTION_UNLESS(
        entries.size() == 1 && entries.front().IsFile(),
        "Test file resource expects exactly one regular file in its materialized root");

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

void TTestFileResource::Validate(const TTextDataPtr& data)
{
    THROW_ERROR_EXCEPTION_IF(
        data->Text == "corrupt",
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
    output->AddMessage(context->ConvertToMessage(enriched));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileResourceTest
