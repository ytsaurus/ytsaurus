#include "experiments.h"

#include "config.h"

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/orm/query_helpers/filter_matcher.h>

#include <random>

namespace NYT::NScheduler {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TExperimentEffectConfig::TExperimentEffectConfig()
{
    RegisterParameter("scheduler_spec_template_patch", SchedulerSpecTemplatePatch)
        .Default();
    RegisterParameter("scheduler_spec_patch", SchedulerSpecPatch)
        .Default();

    RegisterParameter("controller_user_job_spec_template_patch", ControllerUserJobSpecTemplatePatch)
        .Default();
    RegisterParameter("controller_user_job_spec_patch", ControllerUserJobSpecPatch)
        .Default();

    RegisterParameter("controller_job_io_template_patch", ControllerJobIOTemplatePatch)
        .Default();
    RegisterParameter("controller_job_io_patch", ControllerJobIOPatch)
        .Default();

    RegisterParameter("controller_agent_tag", ControllerAgentTag)
        .Default()
        .NonEmpty();
}

////////////////////////////////////////////////////////////////////////////////

TExperimentGroupConfig::TExperimentGroupConfig()
{
    RegisterParameter("fraction", Fraction)
        .GreaterThanOrEqual(0.0)
        .LessThanOrEqual(1.0);
}

////////////////////////////////////////////////////////////////////////////////

TExperimentConfig::TExperimentConfig()
{
    RegisterParameter("ticket", Ticket)
        .NonEmpty();

    RegisterParameter("filter", Filter)
        .Default();

    RegisterParameter("dimension", Dimension)
        .Default("default")
        .NonEmpty();

    RegisterParameter("fraction", Fraction)
        .GreaterThanOrEqual(0.0)
        .LessThanOrEqual(1.0);

    RegisterParameter("groups", Groups)
        .Default();

    RegisterParameter("ab_treatment_group", AbTreatmentGroup)
        .Default();

    RegisterPostprocessor([&] {
        if ((Groups.empty() && !AbTreatmentGroup) || (!Groups.empty() && AbTreatmentGroup)) {
            THROW_ERROR_EXCEPTION("Exactly one of groups and ab_treatment_group experiment spec options should be specified");
        } else if (AbTreatmentGroup) {
            auto& controlGroup = Groups["control"];
            controlGroup = New<TExperimentGroupConfig>();
            controlGroup->Fraction = 1.0 - AbTreatmentGroup->Fraction;
            Groups["treatment"] = std::move(AbTreatmentGroup);
        }
        YT_VERIFY(!Groups.empty() && !AbTreatmentGroup);

        constexpr double AbsTolerance = 1e-6;
        double totalFraction = 0.0;
        for (const auto& [groupName, group] : Groups) {
            // Dot is used in explicit experiment override specification delimiting name and group.
            if (groupName.find('.') != TString::npos) {
                THROW_ERROR_EXCEPTION("Dots are not allowed in group name");
            }

            totalFraction += group->Fraction;
        }
        if (std::abs(totalFraction - 1.0) > AbsTolerance) {
            THROW_ERROR_EXCEPTION(
                "Total fraction of all groups should be equal to 1.0 with absolute tolerance %.1g; actual = %v",
                AbsTolerance,
                totalFraction);
        }

        // Check that query is well-formed.
        if (Filter) {
            NOrm::NQueryHelpers::CreateFilterMatcher(*Filter);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TExperimentAssignment::TExperimentAssignment()
{
    RegisterParameter("experiment", Experiment)
        .NonEmpty();

    RegisterParameter("group", Group)
        .NonEmpty();

    RegisterParameter("ticket", Ticket)
        .NonEmpty();

    RegisterParameter("dimension", Dimension)
        .NonEmpty();

    RegisterParameter("experiment_uniform_sample", ExperimentUniformSample)
        .GreaterThanOrEqual(0.0)
        .LessThanOrEqual(1.0);

    RegisterParameter("group_uniform_sample", GroupUniformSample)
        .GreaterThanOrEqual(0.0)
        .LessThanOrEqual(1.0);

    RegisterParameter("effect", Effect);
}

TExperimentAssignment::TExperimentAssignment(
    TString experiment,
    TString group,
    TString ticket,
    TString dimension,
    double experimentUniformSample,
    double groupUniformSample,
    TExperimentEffectConfigPtr effect)
    : TExperimentAssignment()
{
    Experiment = std::move(experiment);
    Group = std::move(group);
    Ticket = std::move(ticket);
    Dimension = std::move(dimension);
    ExperimentUniformSample = experimentUniformSample;
    GroupUniformSample = groupUniformSample;
    Effect = std::move(effect);
}

TString TExperimentAssignment::GetName() const
{
    return Format("%v.%v", Experiment, Group);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TExperimentAssignmentPtr> AssignExperiments(
    EOperationType type,
    const TString& user,
    const IMapNodePtr& specNode,
    const THashMap<TString, TExperimentConfigPtr>& experiments)
{
    std::vector<TExperimentAssignmentPtr> assignments;

    auto experimentOverridesNode = specNode->FindChild("experiment_overrides");

    // We feed this YSON to experiment filters. Constructing such string costs
    // some CPU time, so we construct it lazily.
    TYsonString filterAttributes;
    auto initFilterAttributes = [&] {
        if (filterAttributes) {
            return;
        }
        filterAttributes = BuildYsonStringFluently()
            .BeginMap()
                .Item("type").Value(type)
                .Item("user").Value(user)
                .Item("spec").Value(specNode)
            .EndMap();
    };

    auto matchFilter = [&] (const TExperimentConfigPtr& experiment) {
        if (!experiment->Filter) {
            return true;
        }
        auto filterMatcher = NOrm::NQueryHelpers::CreateFilterMatcher(*experiment->Filter);
        initFilterAttributes();
        return filterMatcher->Match(filterAttributes)
            .ValueOrThrow();
    };

    if (experimentOverridesNode) {
        auto experimentOverrides = ConvertTo<std::vector<TString>>(experimentOverridesNode);
        // Experiments are explicitly specified via spec option, use them.
        // If some experiment is unrecognized or is not viable due to filter, fail whole operation.
        for (const auto& experimentNameAndMaybeGroupName : experimentOverrides) {
            TString experimentName;
            TString groupName;
            if (auto dotPosition = experimentNameAndMaybeGroupName.find('.'); dotPosition != TString::npos) {
                experimentName = experimentNameAndMaybeGroupName.substr(0, dotPosition);
                groupName = experimentNameAndMaybeGroupName.substr(dotPosition + 1);
            } else {
                experimentName = experimentNameAndMaybeGroupName;
                groupName = "treatment";
            }

            auto experimentIt = experiments.find(experimentName);
            if (experimentIt == experiments.end()) {
                THROW_ERROR_EXCEPTION("Experiment %Qv is not known", experimentName);
            }
            const auto& experiment = experimentIt->second;
            auto groupIt = experiment->Groups.find(groupName);
            if (groupIt == experiment->Groups.end()) {
                THROW_ERROR_EXCEPTION("Group %Qv is not known in experiment %Qv", groupName, experimentName);
            }
            if (!matchFilter(experiment)) {
                THROW_ERROR_EXCEPTION("Operation does not match filter of experiment %Qv",
                    experimentName)
                    << TErrorAttribute("filter", experiment->Filter);
            }

            assignments.emplace_back(New<TExperimentAssignment>(
                experimentName,
                groupName,
                experiment->Ticket,
                experiment->Dimension,
                /* experimentUniformSample */ 0.0,
                /* groupUniformSample */ 0.0,
                groupIt->second));
        }
    } else {
        // Pick at most one experiment from each dimension, then pick
        // one group in each chosen experiment.

        // dimension -> name -> experiment.
        THashMap<TString, THashMap<TString, TExperimentConfigPtr>> dimensionToExperiments;
        for (auto it = experiments.begin(); it != experiments.end(); ++it) {
            const auto& experiment = it->second;
            YT_VERIFY(dimensionToExperiments[experiment->Dimension].insert(*it).second);
        }

        std::random_device randomDevice;
        std::mt19937 generator(randomDevice());
        std::uniform_real_distribution distribution(0.0, 1.0);

        //! Generic helper for picking at most one element from map "object name -> object" with
        //! each object having a Fraction field; used both for experiment dimensions and groups.
        //! If forcePick is set, some element is guaranteed to be picked (use this option to overcome
        //! double precision issues when total Fraction is expected to be 1.0).
        //! Returns a pair (iterator, uniformSample); iterator may point to collection's end().
        auto pickElement = [&] (const auto& collection, bool forcePick) {
            if (forcePick) {
                // Sanity check.
                YT_VERIFY(!collection.empty());
            }
            double uniformSample = distribution(generator);
            double cumulativeFraction = 0.0;
            typename std::remove_reference_t<decltype(collection)>::const_iterator sampledIt;
            for (auto it = collection.cbegin(); ; ++it) {
                if (it == collection.cend() && forcePick) {
                    // Keep previous value of sampledIt.
                    break;
                }
                sampledIt = it;
                if (it == collection.cend()) {
                    YT_VERIFY(!forcePick);
                    // By this moment sampledIt may be equal to end().
                    break;
                }
                if (cumulativeFraction + it->second->Fraction > uniformSample) {
                    break;
                }
                cumulativeFraction += it->second->Fraction;
            }
            return std::make_pair(sampledIt, uniformSample);
        };

        for (const auto& dimensionExperiments : GetValues(dimensionToExperiments)) {
            auto [experimentIt, experimentUniformSample] = pickElement(dimensionExperiments, /* forcePick */ false);
            if (experimentIt == dimensionExperiments.end()) {
                // Pick no experiment from this dimension.
                continue;
            }
            auto [experimentName, experiment] = *experimentIt;
            auto [groupIt, groupUniformSample] = pickElement(experiment->Groups, /* forcePick */ true);
            auto [groupName, group] = *groupIt;

            if (!matchFilter(experiment)) {
                // Skip such experiment.
                continue;
            }

            assignments.emplace_back(New<TExperimentAssignment>(
                experimentName,
                groupName,
                experiment->Ticket,
                experiment->Dimension,
                experimentUniformSample,
                groupUniformSample,
                group));
        }
    }

    if (specNode->FindChild("controller_agent_tag")) {
        // Discard those experiments which define controller agent tags.
        // If someone asked for a certain controller agent tag, he probably knows
        // what he's doing.
        auto it = std::remove_if(assignments.begin(), assignments.end(), [&] (const TExperimentAssignmentPtr& assignment) {
            return static_cast<bool>(assignment->Effect->ControllerAgentTag);
        });

        if (it != assignments.end() && experimentOverridesNode) {
            THROW_ERROR_EXCEPTION(
                "Requested experiments impose controller agent tag which is already explicitly specified in spec");
        }

        assignments.erase(it, assignments.end());
    }

    return assignments;
}

void ValidateExperiments(const THashMap<TString, TExperimentConfigPtr>& experiments)
{
    // Validate total fractions over each experiment dimension.
    THashMap<TString, double> dimensionToFraction;
    for (const auto& [experimentName, experiment] : experiments) {
        // Dot is used in explicit experiment override specification delimiting name and group.
        if (experimentName.find('.') != TString::npos) {
            THROW_ERROR_EXCEPTION("Dots are not allowed in experiment name");
        }

        dimensionToFraction[experiment->Dimension] += experiment->Fraction;
    }
    constexpr double AbsTolerance = 1e-6;
    for (const auto& [dimension, fraction] : dimensionToFraction) {
        if (fraction > 1.0 + AbsTolerance) {
            THROW_ERROR_EXCEPTION(
                "Total fraction of experiments from dimension %Qv is %v > 1 + %.1g",
                dimension,
                fraction,
                AbsTolerance);
        }
    }

    // Validate controller agent tag assignment validity. It should be true that
    // at least one of two conditions hold:
    // - there is at most one dimension with experiments assigning controller agent tags;
    // - all experiments assigning controller agent tags assign the same tag.
    THashSet<TString> controllerAgentTags;
    THashSet<TString> tagAssigningDimensions;
    for (const auto& experiment : GetValues(experiments)) {
        for (const auto& group : GetValues(experiment->Groups)) {
            if (group->ControllerAgentTag) {
                controllerAgentTags.insert(*group->ControllerAgentTag);
                tagAssigningDimensions.insert(experiment->Dimension);
            }
        }
    }

    if (controllerAgentTags.size() >= 2 && tagAssigningDimensions.size() >= 2) {
        THROW_ERROR_EXCEPTION(
            "Experiment configuration allows operation being assigned to two "
            "distinct controller agent tags simultaneously")
            << TErrorAttribute("controller_agent_tags", controllerAgentTags)
            << TErrorAttribute("tag_assigning_dimensions", tagAssigningDimensions);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
