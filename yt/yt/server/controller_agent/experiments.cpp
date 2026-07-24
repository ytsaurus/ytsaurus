#include "experiments.h"

#include <yt/yt/server/lib/scheduler/experiments.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_client.h>

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
    if (node) {
        node = CloneNode(node);
    }
    if (templatePatch) {
        if (node) {
            node = PatchNode(templatePatch, node);
        } else {
            node = templatePatch;
        }
    }
    if (patch) {
        if (node) {
            node = PatchNode(node, patch);
        } else {
            node = patch;
        }
    }
    if (node) {
        ForceYPath(root, path);
        // Note that #node may be equal to one of the #root's subtrees or to one of the patches.
        // In any case, we do not want to use it as an argument to SetNodeByYPath, since this wonderful
        // method would change the parent of the argument node, which may lead to child-parent relation inconsistency.
        SetNodeByYPath(root, path, CloneNode(node));
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ApplyExperiments(
    const IMapNodePtr& spec,
    EOperationType type,
    const std::vector<TExperimentAssignmentPtr>& experimentAssignments,
    INodePtr* optionsPatch)
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
                userJobPaths.push_back("/tasks/" + key);
                jobIOPaths.push_back("/tasks/" + key + "/job_io");
            }
            break;
        }
    }

    INodePtr mergedOptionsPatch;

    for (const auto& experiment : experimentAssignments) {
        for (const auto& path : userJobPaths) {
            ApplyPatch(
                path,
                spec,
                experiment->Effect->ControllerUserJobSpecTemplatePatch,
                experiment->Effect->ControllerUserJobSpecPatch);
        }
        for (const auto& path : jobIOPaths) {
            ApplyPatch(
                path,
                spec,
                experiment->Effect->ControllerJobIOTemplatePatch,
                experiment->Effect->ControllerJobIOPatch);
        }
        if (const auto& node = experiment->Effect->ControllerOptionsPatch; node) {
            mergedOptionsPatch = mergedOptionsPatch
                ? PatchNode(mergedOptionsPatch, node)
                : node;
        }
    }

    *optionsPatch = std::move(mergedOptionsPatch);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
