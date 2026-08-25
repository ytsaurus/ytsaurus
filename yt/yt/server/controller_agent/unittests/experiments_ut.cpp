#include <yt/yt/server/controller_agent/experiments.h>

#include <yt/yt/server/lib/scheduler/experiments.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NControllerAgent {
namespace {

using namespace NScheduler;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TExperimentAssignmentPtr MakeAssignment(const std::string& effectYson)
{
    auto assignment = New<TExperimentAssignment>();
    assignment->SetFields(
        "experiment",
        "treatment",
        "YTEXP-1",
        "default",
        /*experimentUniformSample*/ 0.1,
        /*groupUniformSample*/ 0.2,
        ConvertTo<TExperimentEffectConfigPtr>(TYsonStringBuf(effectYson)));
    return assignment;
}

IMapNodePtr ParseSpec(const std::string& yson)
{
    return ConvertTo<INodePtr>(TYsonStringBuf(yson))->AsMap();
}

void ExpectSpecEquals(const IMapNodePtr& spec, const std::string& expectedYson)
{
    EXPECT_TRUE(
        AreNodesEqual(spec, ConvertTo<INodePtr>(TYsonStringBuf(expectedYson))))
        << "actual: "
        << ConvertToYsonString(spec, EYsonFormat::Text).ToString();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TApplyExperimentsTest, UserJobSimple)
{
    auto spec = ParseSpec("{mapper={command=cat;memory_limit=100}}");
    auto assignment = MakeAssignment(
        "{controller_user_job_spec_patch={memory_limit=200}}");

    INodePtr optionsPatch;
    ApplyExperiments(spec, EOperationType::Map, {assignment}, &optionsPatch);

    ExpectSpecEquals(spec, "{mapper={command=cat;memory_limit=200}}");
    EXPECT_FALSE(optionsPatch);
}

TEST(TApplyExperimentsTest, UserJobTemplateSimple)
{
    auto spec = ParseSpec("{mapper={command=cat;memory_limit=100}}");
    auto assignment = MakeAssignment(R"({
        controller_user_job_spec_template_patch = {
            memory_limit = 200;
            memory_reserve_factor = 0.5;
        };
    })");

    INodePtr optionsPatch;
    ApplyExperiments(spec, EOperationType::Map, {assignment}, &optionsPatch);

    ExpectSpecEquals(
        spec,
        "{mapper={command=cat;memory_limit=100;memory_reserve_factor=0.5}}");
    EXPECT_FALSE(optionsPatch);
}

TEST(TApplyExperimentsTest, JobIOPatchCreatesMissingPath)
{
    auto spec = ParseSpec("{mapper={command=cat}}");
    auto assignment = MakeAssignment(
        "{controller_job_io_patch={table_writer={max_row_weight=32}}}");

    INodePtr optionsPatch;
    ApplyExperiments(spec, EOperationType::Map, {assignment}, &optionsPatch);

    ExpectSpecEquals(
        spec,
        R"({
            mapper = {command = cat};
            job_io = {table_writer = {max_row_weight = 32}};
            auto_merge = {job_io = {table_writer = {max_row_weight = 32}}};
        })");
}

TEST(TApplyExperimentsTest, PatchNodeIsNotReparented)
{
    auto spec = ParseSpec(
        "{mapper={command=cat};job_io={};auto_merge={job_io={}}}");
    auto assignment = MakeAssignment(
        "{controller_job_io_patch={table_writer={max_row_weight=32}}}");

    INodePtr optionsPatch;
    ApplyExperiments(spec, EOperationType::Map, {assignment}, &optionsPatch);

    // Spoil original patch.
    auto jobIOPatch = assignment->Effect->ControllerJobIOPatch->AsMap();
    jobIOPatch->RemoveChild("table_writer");

    ExpectSpecEquals(
        spec,
        R"({
            mapper = {command = cat};
            job_io = {table_writer = {max_row_weight = 32}};
            auto_merge = {job_io = {table_writer = {max_row_weight = 32}}};
        })");
}

TEST(TApplyExperimentsTest, SeveralAssignmentsAppliedInOrder)
{
    auto spec = ParseSpec("{mapper={command=cat}}");
    auto first = MakeAssignment(R"({
        controller_user_job_spec_patch = {memory_limit = 200};
        controller_options_patch = {a = 1; b = 1};
    })");
    auto second = MakeAssignment(R"({
        controller_user_job_spec_patch = {memory_limit = 300};
        controller_options_patch = {b = 2};
    })");

    INodePtr optionsPatch;
    ApplyExperiments(spec, EOperationType::Map, {first, second}, &optionsPatch);

    ExpectSpecEquals(spec, "{mapper={command=cat;memory_limit=300}}");
    ExpectSpecEquals(optionsPatch->AsMap(), "{a=1;b=2}");
}

// Templates are bases, so an earlier assignment wins over a later one; patches are applied
// on top, so a later assignment wins over an earlier one.
TEST(TApplyExperimentsTest, EarlierAssignmentWinsAmongTemplates)
{
    auto spec = ParseSpec("{mapper={command=cat}}");
    auto first = MakeAssignment("{controller_user_job_spec_template_patch={memory_limit=100;a=1}}");
    auto second = MakeAssignment("{controller_user_job_spec_template_patch={memory_limit=200;b=2}}");

    INodePtr optionsPatch;
    ApplyExperiments(spec, EOperationType::Map, {first, second}, &optionsPatch);

    ExpectSpecEquals(spec, "{mapper={command=cat;memory_limit=100;a=1;b=2}}");
}

TEST(TApplyExperimentsTest, PatchWinsOverTemplateOfAnotherAssignment)
{
    auto spec = ParseSpec("{mapper={command=cat}}");
    auto first = MakeAssignment("{controller_user_job_spec_patch={memory_limit=100}}");
    auto second = MakeAssignment("{controller_user_job_spec_template_patch={memory_limit=200}}");

    INodePtr optionsPatch;
    ApplyExperiments(spec, EOperationType::Map, {first, second}, &optionsPatch);

    ExpectSpecEquals(spec, "{mapper={command=cat;memory_limit=100}}");
}

TEST(TApplyExperimentsTest, VanillaSimple)
{
    auto spec = ParseSpec(
        "{tasks={first={command=cat};second={command=cat;job_io={}}}}");
    auto assignment = MakeAssignment(R"({
        controller_user_job_spec_patch = {memory_limit = 200};
        controller_job_io_patch = {table_writer = {max_row_weight = 32}};
    })");

    INodePtr optionsPatch;
    ApplyExperiments(
        spec,
        EOperationType::Vanilla,
        {assignment},
        &optionsPatch);

    ExpectSpecEquals(
        spec,
        R"({
            tasks = {
                first = {
                    command = cat;
                    memory_limit = 200;
                    job_io = {table_writer = {max_row_weight = 32}};
                };
                second = {
                    command = cat;
                    memory_limit = 200;
                    job_io = {table_writer = {max_row_weight = 32}};
                };
            };
            auto_merge = {job_io = {table_writer = {max_row_weight = 32}}};
        })");
}

TEST(TApplyExperimentsTest, VanillaTaskNameWithSlashes)
{
    auto spec = ParseSpec(R"({tasks={"process: //path/to/table"={command=true}}})");
    auto assignment = MakeAssignment(
        "{controller_job_io_patch={foo_spec=patched}}");

    INodePtr optionsPatch;
    ApplyExperiments(
        spec,
        EOperationType::Vanilla,
        {assignment},
        &optionsPatch);

    ExpectSpecEquals(
        spec,
        R"({
            tasks = {
                "process: //path/to/table" = {
                    command = true;
                    job_io = {foo_spec = patched};
                };
            };
            auto_merge = {job_io = {foo_spec = patched}};
        })");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent
