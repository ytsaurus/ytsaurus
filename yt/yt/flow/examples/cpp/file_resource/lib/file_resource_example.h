#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/resources/file/file_resource.h>

namespace NYT::NFlow::NExample {

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

DECLARE_REFCOUNTED_CLASS(TTextFileResource);

class TTextFileResource
    : public TFileResourceBase<TTextData>
{
public:
    using TFileResourceBase::TFileResourceBase;

protected:
    TTextDataPtr Initialize(const TMaterializedFileSourceSnapshotPtr& fileSources) override;
};

DEFINE_REFCOUNTED_TYPE(TTextFileResource);

////////////////////////////////////////////////////////////////////////////////

struct TEnrichedMessage
    : public TYsonMessage
{
    std::string Input;
    std::string FileText;

    REGISTER_YSON_STRUCT(TEnrichedMessage);

    static void Register(TRegistrar registrar);
};

class TEnrichWithFileFunction
    : public IBatchProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TTextFileResourcePtr Resource_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
