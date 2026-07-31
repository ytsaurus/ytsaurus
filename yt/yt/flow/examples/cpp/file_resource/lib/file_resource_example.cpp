#include "file_resource_example.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <util/folder/path.h>
#include <util/stream/file.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

TTextData::TTextData(std::string text)
    : Text(std::move(text))
{ }

TTextDataPtr TTextFileResource::Initialize(const TMaterializedDirectoryPtr& directory)
{
    TVector<TFsPath> entries;
    TFsPath(directory->GetRootPath()).List(entries);
    THROW_ERROR_EXCEPTION_UNLESS(
        entries.size() == 1 && entries.front().IsFile(),
        "Text file resource expects exactly one regular file in its materialized root");
    auto value = TFileInput(entries.front().GetPath()).ReadAll();
    return New<TTextData>(std::string(value.data(), value.size()));
}

////////////////////////////////////////////////////////////////////////////////

void TEnrichedMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("input", &TThis::Input)
        .Default();
    registrar.Parameter("file_text", &TThis::FileText)
        .Default();
}

void TEnrichWithFileFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    Resource_ = initContext->GetStaticResource("text")->As<TTextFileResource>();
}

void TEnrichWithFileFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    std::string fileText;
    {
        auto accessor = Resource_->Lock();
        fileText = accessor->Text;
    }

    auto enriched = New<TEnrichedMessage>();
    enriched->Input = GetColumnValue<std::string>(message, "text");
    enriched->FileText = std::move(fileText);
    output->AddMessage(context->ConvertToMessage(enriched));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
