#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <exception>
#include <utility>

namespace NYql::NYtflow {

namespace {

using namespace NYT::NFlow;
using namespace NYT::NYTree;

constexpr TStringBuf MapComputationClassNames[] = {
    "NYql::NYtflow::TTransformMap",
    "NYql::NYtflow::TSwiftMap",
};

TComputationSpecPtr MakeMapSpec(
    TStringBuf computationClassName,
    THashSet<TStreamId> inputStreamIds,
    bool extend = false)
{
    auto spec = NYT::New<TComputationSpec>();
    spec->ComputationClassName = computationClassName;
    spec->InputStreamIds = std::move(inputStreamIds);
    spec->Parameters = GetEphemeralNodeFactory()->CreateMap();
    if (extend) {
        spec->Parameters->AddChild("extend", ConvertToNode(true));
    }
    return spec;
}

void ValidateForEachMapClass(
    const THashSet<TStreamId>& inputStreamIds,
    bool extend)
{
    for (const auto computationClassName : MapComputationClassNames) {
        SCOPED_TRACE(TString(computationClassName));
        auto spec = MakeMapSpec(computationClassName, inputStreamIds, extend);
        ASSERT_NO_THROW(TRegistry::Get()->ValidateComputationSpec(spec));
    }
}

} // anonymous namespace

TEST(TYtflowMapSpecValidation, AcceptsSingleInputWithoutExtendMode)
{
    ValidateForEachMapClass({TStreamId("input")}, false);
}

TEST(TYtflowMapSpecValidation, RejectsMultipleInputsWithoutExtendMode)
{
    for (const auto computationClassName : MapComputationClassNames) {
        SCOPED_TRACE(TString(computationClassName));
        auto spec = MakeMapSpec(
            computationClassName,
            {TStreamId("first"), TStreamId("second")});
        ASSERT_THROW_MESSAGE_HAS_SUBSTR(
            TRegistry::Get()->ValidateComputationSpec(spec),
            std::exception,
            "supports multiple input streams only in Extend mode");
    }
}

TEST(TYtflowMapSpecValidation, AcceptsMultipleInputsInExtendMode)
{
    ValidateForEachMapClass(
        {TStreamId("first"), TStreamId("second")},
        true);
}

} // namespace NYql::NYtflow
