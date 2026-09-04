#include "experiments.h"

#include <yt/yt/server/lib/scheduler/experiments.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <ranges>

namespace NYT::NControllerAgent {

using namespace NScheduler;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

void ApplyPatch(
    const TYPath& path,
    const INodePtr& root,
    const INodePtr& templatePatch,
    const INodePtr& patch)
{
    auto node = FindNodeByYPath(root, path);
    // PatchNode always returns a new node, so #node needs no cloning if it was passed to PatchNode.
    // #owned tracks whether we own it (e.g. node was (deeply) copied by PatchNode).
    bool owned = false;
    if (templatePatch) {
        if (node) {
            node = PatchNode(templatePatch, node);
            owned = true;
        } else {
            node = templatePatch;
        }
    }
    if (patch) {
        if (node) {
            node = PatchNode(node, patch);
            owned = true;
        } else {
            node = patch;
        }
    }
    if (node) {
        ForceYPath(root, path);
        // Unless it is owned, #node is equal either to one of the #root's subtrees or to one
        // of the patches. In both cases we do not want to use it as an argument to SetNodeByYPath,
        // since this wonderful method would change the parent of the argument node, which may
        // lead to child-parent relation inconsistency.
        SetNodeByYPath(root, path, owned ? node : CloneNode(node));
    }
}

INodePtr PatchNodeFast(const INodePtr& accumulator, const INodePtr& patch)
{
    if (!patch) {
        return accumulator;
    }
    if (!accumulator) {
        return patch;
    }
    return PatchNode(accumulator, patch);
}

struct TMergedEffect
{
    INodePtr UserJobSpecTemplatePatch;
    INodePtr UserJobSpecPatch;
    INodePtr JobIOTemplatePatch;
    INodePtr JobIOPatch;
    INodePtr OptionsPatch;
};

TMergedEffect MergeEffects(const std::vector<TExperimentAssignmentPtr>& experimentAssignments)
{
    TMergedEffect merged;
    for (const auto& experiment : experimentAssignments) {
        const auto& effect = experiment->Effect;
        merged.UserJobSpecPatch = PatchNodeFast(merged.UserJobSpecPatch, effect->ControllerUserJobSpecPatch);
        merged.JobIOPatch = PatchNodeFast(merged.JobIOPatch, effect->ControllerJobIOPatch);
        merged.OptionsPatch = PatchNodeFast(merged.OptionsPatch, effect->ControllerOptionsPatch);
    }

    // COMPAT(coteeq): The fact that we are applying patches in order is purely
    // accidental. Ideally, we should check that patches do not intersect when
    // loading config from Cypress in scheduler.
    //
    // Applying the assignments one by one makes the template of a later assignment the base
    // of the already patched spec, so the templates merge in the reverse order.
    for (const auto& experiment : experimentAssignments | std::views::reverse) {
        const auto& effect = experiment->Effect;
        merged.UserJobSpecTemplatePatch = PatchNodeFast(
            merged.UserJobSpecTemplatePatch,
            effect->ControllerUserJobSpecTemplatePatch);
        merged.JobIOTemplatePatch = PatchNodeFast(
            merged.JobIOTemplatePatch,
            effect->ControllerJobIOTemplatePatch);
    }

    return merged;
}

struct TPatchPaths
{
    std::vector<TYPath> UserJobPaths;
    std::vector<TYPath> JobIOPaths;
};

TPatchPaths ComputePatchPaths(const IMapNodePtr& spec, EOperationType type)
{
    std::vector<TYPath> userJobPaths;
    std::vector<TYPath> jobIOPaths;
    jobIOPaths.push_back("/auto_merge/job_io");
    switch (type) {
        case EOperationType::Map: {
            userJobPaths.push_back("/mapper");
            jobIOPaths.push_back("/job_io");
            break;
        }
        case EOperationType::JoinReduce:
        case EOperationType::Reduce: {
            userJobPaths.push_back("/reducer");
            jobIOPaths.push_back("/job_io");
            break;
        }
        case EOperationType::MapReduce: {
            if (FindNodeByYPath(spec, "/mapper")) {
                userJobPaths.push_back("/mapper");
            }
            if (FindNodeByYPath(spec, "/reduce_combiner")) {
                userJobPaths.push_back("/reduce_combiner");
            }
            userJobPaths.push_back("/reducer");
            jobIOPaths.push_back("/map_job_io");
            jobIOPaths.push_back("/sort_job_io");
            jobIOPaths.push_back("/reduce_job_io");
            break;
        }
        case EOperationType::Sort: {
            jobIOPaths.push_back("/partition_job_io");
            jobIOPaths.push_back("/sort_job_io");
            jobIOPaths.push_back("/merge_job_io");
            break;
        }
        case EOperationType::Merge:
        case EOperationType::Erase:
        case EOperationType::RemoteCopy: {
            jobIOPaths.push_back("/job_io");
            break;
        }
        case EOperationType::Vanilla: {
            auto tasks = GetNodeByYPath(spec, "/tasks");
            for (const auto& key : tasks->AsMap()->GetKeys()) {
                auto escapedKey = ToYPathLiteral(key);
                userJobPaths.push_back("/tasks/" + escapedKey);
                jobIOPaths.push_back("/tasks/" + escapedKey + "/job_io");
            }
            break;
        }
    }

    return {
        .UserJobPaths = std::move(userJobPaths),
        .JobIOPaths = std::move(jobIOPaths),
    };
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ApplyExperiments(
    const IMapNodePtr& spec,
    EOperationType type,
    const std::vector<TExperimentAssignmentPtr>& experimentAssignments,
    INodePtr* optionsPatch)
{
    auto paths = ComputePatchPaths(spec, type);
    auto effect = MergeEffects(experimentAssignments);

    for (const auto& path : paths.UserJobPaths) {
        ApplyPatch(path, spec, effect.UserJobSpecTemplatePatch, effect.UserJobSpecPatch);
    }
    for (const auto& path : paths.JobIOPaths) {
        ApplyPatch(path, spec, effect.JobIOTemplatePatch, effect.JobIOPatch);
    }

    *optionsPatch = std::move(effect.OptionsPatch);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
