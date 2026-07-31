#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/resources/file/file_resource.h>

namespace NYT::NFlow::NFileResourceTest {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTextData);

class TTextData
    : public TRefCounted
{
public:
    explicit TTextData(std::string text);

    const std::string Text;
};

DEFINE_REFCOUNTED_TYPE(TTextData);

class TTestFileResource
    : public TFileResourceBase<TTextData>
{
public:
    using TFileResourceBase::TFileResourceBase;

protected:
    TTextDataPtr Initialize(const TMaterializedDirectoryPtr& directory) override;
    void Validate(const TTextDataPtr& data) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TEnrichedMessage
    : public TYsonMessage
{
    std::string Input;
    std::string FileText;
    i64 ResourceRevision{};

    REGISTER_YSON_STRUCT(TEnrichedMessage);

    static void Register(TRegistrar registrar);
};

class TEnrichWithFileFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TIntrusivePtr<TTestFileResource> Resource_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileResourceTest
