#include "experiments.h"

#include "config.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/orm/library/query/filter_matcher.h>

#include <random>

namespace NYT::NScheduler {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TExperimentEffectConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("scheduler_spec_template_patch", &TThis::SchedulerSpecTemplatePatch)
        .Default();
    registrar.Parameter("scheduler_spec_patch", &TThis::SchedulerSpecPatch)
        .Default();

    registrar.Parameter("controller_user_job_spec_template_patch", &TThis::ControllerUserJobSpecTemplatePatch)
        .Default();
    registrar.Parameter("controller_user_job_spec_patch", &TThis::ControllerUserJobSpecPatch)
        .Default();

    registrar.Parameter("controller_job_io_template_patch", &TThis::ControllerJobIOTemplatePatch)
        .Default();
    registrar.Parameter("controller_job_io_patch", &TThis::ControllerJobIOPatch)
        .Default();

    registrar.Parameter("controller_agent_tag", &TThis::ControllerAgentTag)
        .Default()
        .NonEmpty();
}

////////////////////////////////////////////////////////////////////////////////

void TExperimentGroupConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("fraction", &TThis::Fraction)
        .GreaterThanOrEqual(0.0)
        .LessThanOrEqual(1.0);
}

////////////////////////////////////////////////////////////////////////////////

void TExperimentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("ticket", &TThis::Ticket)
        .NonEmpty();

    registrar.Parameter("filter", &TThis::Filter)
        .Default();

    registrar.Parameter("dimension", &TThis::Dimension)
        .Default("default")
        .NonEmpty();

    registrar.Parameter("fraction", &TThis::Fraction)
        .GreaterThanOrEqual(0.0)
        .LessThanOrEqual(1.0);

    registrar.Parameter("groups", &TThis::Groups)
        .Default();

    registrar.Parameter("ab_treatment_group", &TThis::AbTreatmentGroup)
        .Default();

    registrar.Postprocessor([] (TExperimentConfig* config) {
        if ((config->Groups.empty() && !config->AbTreatmentGroup) || (!config->Groups.empty() && config->AbTreatmentGroup)) {
            THROW_ERROR_EXCEPTION("Exactly one of groups and ab_treatment_group experiment spec options should be specified");
        } else if (config->AbTreatmentGroup) {
            auto& controlGroup = config->Groups["control"];
            controlGroup = New<TExperimentGroupConfig>();
            controlGroup->Fraction = 1.0 - config->AbTreatmentGroup->Fraction;
            config->Groups["treatment"] = std::move(config->AbTreatmentGroup);
        }
        YT_VERIFY(!config->Groups.empty() && !config->AbTreatmentGroup);

        constexpr double AbsTolerance = 1e-6;
        double totalFraction = 0.0;
        for (const auto& [groupName, group] : config->Groups) {
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
        if (config->Filter) {
            NOrm::NQuery::CreateFilterMatcher(*config->Filter);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TExperimentAssignment::Register(TRegistrar registrar)
{
    registrar.Parameter("experiment", &TThis::Experiment)
        .NonEmpty();

    registrar.Parameter("group", &TThis::Group)
        .NonEmpty();

    registrar.Parameter("ticket", &TThis::Ticket)
        .NonEmpty();

    registrar.Parameter("dimension", &TThis::Dimension)
        .NonEmpty();

    registrar.Parameter("experiment_uniform_sample", &TThis::ExperimentUniformSample)
        .GreaterThanOrEqual(0.0)
        .LessThanOrEqual(1.0);

    registrar.Parameter("group_uniform_sample", &TThis::GroupUniformSample)
        .GreaterThanOrEqual(0.0)
        .LessThanOrEqual(1.0);

    registrar.Parameter("effect", &TThis::Effect);
}

void TExperimentAssignment::SetFields(
    TString experiment,
    TString group,
    TString ticket,
    TString dimension,
    double experimentUniformSample,
    double groupUniformSample,
    TExperimentEffectConfigPtr effect)
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

TExperimentAssigner::TExperimentAssigner(THashMap<TString, TExperimentConfigPtr> experiments)
{
    UpdateExperimentConfigs(std::move(experiments));
}

bool TExperimentAssigner::MatchExperiment(
    const TPreparedExperimentPtr& experiment,
    TAssignmentContext& attributes) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!experiment->FilterMatcher) {
        return true;
    }

    return experiment->FilterMatcher->Match(attributes.GetAttributesAsYson())
        .ValueOrThrow();
}

void TExperimentAssigner::UpdateExperimentConfigs(const THashMap<TString, TExperimentConfigPtr>& experiments)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto preparedExperiments = New<TPreparedExperiments>();

    for (const auto& [name, experimentConfig] : experiments) {
        auto preparedExperiment = New<TPreparedExperiment>();
        preparedExperiment->Config = experimentConfig;
        if (experimentConfig->Filter) {
            preparedExperiment->FilterMatcher = NOrm::NQuery::CreateFilterMatcher(
                *experimentConfig->Filter);
        }
        EmplaceOrCrash(preparedExperiments->Experiments, name, preparedExperiment);
    }

    PreparedExperiments_.Store(preparedExperiments);
}

std::vector<TExperimentAssignmentPtr> TExperimentAssigner::Assign(
    EOperationType type,
    const TString& user,
    const IMapNodePtr& specNode) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<TExperimentAssignmentPtr> assignments;

    auto preparedExperiments = PreparedExperiments_.Acquire();

    auto experimentOverridesNode = specNode->FindChild("experiment_overrides");

    TAssignmentContext assignmentContext(type, user, specNode);

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

            auto experimentIt = preparedExperiments->Experiments.find(experimentName);
            if (experimentIt == preparedExperiments->Experiments.end()) {
                THROW_ERROR_EXCEPTION("Experiment %Qv is not known", experimentName);
            }
            const auto& experiment = experimentIt->second;
            auto groupIt = experiment->Config->Groups.find(groupName);
            if (groupIt == experiment->Config->Groups.end()) {
                THROW_ERROR_EXCEPTION("Group %Qv is not known in experiment %Qv", groupName, experimentName);
            }
            if (!MatchExperiment(experiment, assignmentContext)) {
                THROW_ERROR_EXCEPTION("Operation does not match filter of experiment %Qv",
                    experimentName)
                    << TErrorAttribute("filter", experiment->Config->Filter);
            }

            assignments.emplace_back(New<TExperimentAssignment>());
            assignments.back()->SetFields(
                experimentName,
                groupName,
                experiment->Config->Ticket,
                experiment->Config->Dimension,
                /*experimentUniformSample*/ 0.0,
                /*groupUniformSample*/ 0.0,
                groupIt->second);
        }
    } else {
        // Pick at most one experiment from each dimension, then pick
        // one group in each chosen experiment.

        // dimension -> name -> experiment.
        THashMap<TString, THashMap<TString, TPreparedExperimentPtr>> dimensionToExperiments;
        for (const auto& [name, experiment] : preparedExperiments->Experiments) {
            EmplaceOrCrash(dimensionToExperiments[experiment->Config->Dimension], name, experiment);
        }

        std::random_device randomDevice;
        std::mt19937 generator(randomDevice());
        std::uniform_real_distribution distribution(0.0, 1.0);

        //! Generic helper for picking at most one element from map "object name -> object" with
        //! each object having a Fraction field; used both for experiment dimensions and groups.
        //! If forcePick is set, some element is guaranteed to be picked (use this option to overcome
        //! double precision issues when total Fraction is expected to be 1.0).
        //! Returns a pair (iterator, uniformSample); iterator may point to collection's end().
        auto pickElement = [&] (auto& collection, bool forcePick, auto fractionGetter) {
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
                if (cumulativeFraction + fractionGetter(it->second) > uniformSample) {
                    break;
                }
                cumulativeFraction += fractionGetter(it->second);
            }
            return std::pair(sampledIt, uniformSample);
        };

        for (const auto& dimensionExperiments : GetValues(dimensionToExperiments)) {
            auto [experimentIt, experimentUniformSample] = pickElement(
                dimensionExperiments,
                /*forcePick*/ false,
                [] (const TPreparedExperimentPtr& experiment) {
                    return experiment->Config->Fraction;
                });
            if (experimentIt == dimensionExperiments.end()) {
                // Pick no experiment from this dimension.
                continue;
            }
            auto [experimentName, experiment] = *experimentIt;
            auto [groupIt, groupUniformSample] = pickElement(
                experiment->Config->Groups,
                /*forcePick*/ true,
                [] (const TExperimentGroupConfigPtr& experiment) {
                    return experiment->Fraction;
                });
            auto [groupName, group] = *groupIt;

            if (!MatchExperiment(experiment, assignmentContext)) {
                // Skip such experiment.
                continue;
            }

            assignments.emplace_back(New<TExperimentAssignment>());
            assignments.back()->SetFields(
                experimentName,
                groupName,
                experiment->Config->Ticket,
                experiment->Config->Dimension,
                experimentUniformSample,
                groupUniformSample,
                group);
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

////////////////////////////////////////////////////////////////////////////////

TExperimentAssigner::TAssignmentContext::TAssignmentContext(EOperationType type, TString user, IMapNodePtr spec)
    : Type_(type)
    , User_(std::move(user))
    , Spec_(std::move(spec))
{ }

const TYsonString& TExperimentAssigner::TAssignmentContext::GetAttributesAsYson()
{
    if (!AttributesAsYson_) {
        AttributesAsYson_ = BuildYsonStringFluently()
            .BeginMap()
                .Item("type").Value(Type_)
                .Item("user").Value(User_)
                .Item("spec").Value(Spec_)
            .EndMap();
    }

    return AttributesAsYson_;
}

////////////////////////////////////////////////////////////////////////////////

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
