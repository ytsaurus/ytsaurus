#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Parameters defining effect on operation.
//! Each patch is present in two versions; one is applied before user spec changing
//! default values of the fields and another is applied after user spec allowing you
//! to override some user-provided values (similar to TYsonSerializable preprocessors
//! and postprocessor).
struct TExperimentEffectConfig
    : public NYTree::TYsonSerializable
{
    //! Spec template patch applied in scheduler.
    NYTree::INodePtr SchedulerSpecTemplatePatch;
    //! Spec patch applied in scheduler.
    NYTree::INodePtr SchedulerSpecPatch;

    //! Spec template patch applied in controller agent before controller instantiation.
    NYTree::INodePtr ControllerSpecTemplatePatch;
    //! Spec patch applied in controller agent before controller instantiation.
    NYTree::INodePtr ControllerSpecPatch;

    //! User job spec template patch applied for all user jobs in spec in controller agent before controller instantiation.
    NYTree::INodePtr ControllerUserJobSpecTemplatePatch;
    //! User job spec patch applied for all user jobs in spec in controller agent before controller instantiation.
    NYTree::INodePtr ControllerUserJobSpecPatch;

    //! Job IO spec template patches applied for all job IO configs in controller agent before controller instantiation.
    NYTree::INodePtr ControllerJobIOTemplatePatch;
    //! Job IO spec patches applied for all job IO configs in controller agent before controller instantiation.
    NYTree::INodePtr ControllerJobIOPatch;

    //! If set, only controller agents with this tag may be assigned to operations of this group.
    std::optional<TString> ControllerAgentTag;

    TExperimentEffectConfig();
};

DEFINE_REFCOUNTED_TYPE(TExperimentEffectConfig)

////////////////////////////////////////////////////////////////////////////////

//! Definition of an experiment group.
struct TExperimentGroupConfig
    : public TExperimentEffectConfig
{
    //! Fraction of all operations assigned to enclosing experiment that will be assigned to this group.
    double Fraction;

    TExperimentGroupConfig();
};

DEFINE_REFCOUNTED_TYPE(TExperimentGroupConfig)

////////////////////////////////////////////////////////////////////////////////

//! Specification of a single experiment.
struct TExperimentConfig
    : public NYTree::TYsonSerializable
{
    //! Ticket containing details about this experiment (required, non-empty).
    TString Ticket;

    //! Query a-la YP filters defining precondition on operation to be included in experiment.
    //! e.g. [/type] == "map" && [/spec/ordered] == "ordered".
    std::optional<TString> Filter;

    //! Each operation is assigned to at most one experiment from each dimension by sampling an (independent) random variable
    //! from U[0,1] defining which experiment to take from this dimension. In particular, total fraction
    //! of all experiments from same dimension should not exceed 1.0 (with absolute tolerance 1e-6).
    TString Dimension = "default";

    //! Probability of experiment enabling among all operations from either exclusive or non-exclusive operation domain.
    double Fraction;

    //! Specification of testing groups. Typical situation with AB-experiment involving two groups
    //! (control and treatment) may be set up using shorthand option 'ab_treatment_group' below.
    //! You must specify all groups here, total group fraction should be equal to 1.0 (with absolute tolerance 1e-6)
    //! unless you are using 'ab_treatment_group' shorthand.
    THashMap<TString, TExperimentGroupConfigPtr> Groups;

    //! A shorthand allowing to specify only treatment group for a regular AB-experiment. Control group
    //! will be automatically generated with remaining fraction and empty spec patches and controller agent tag.
    TExperimentGroupConfigPtr AbTreatmentGroup = nullptr;

    TExperimentConfig();
};

DEFINE_REFCOUNTED_TYPE(TExperimentConfig)

////////////////////////////////////////////////////////////////////////////////

//! Finalized specification of an assignment to a particular experiment including group assigment.
struct TExperimentAssignment
    : public NYTree::TYsonSerializable
{
    //! Experiment name.
    TString Experiment;
    //! Group name.
    TString Group;
    //! Ticket for clarity.
    TString Ticket;
    //! Experiment dimension.
    TString Dimension;
    //! Assigned experiment uniform sample for debugging purposes.
    double ExperimentUniformSample;
    //! Assigned group uniform sample for debugging purposes.
    double GroupUniformSample;

    //! Effect provided by the chosen experiment and group.
    TExperimentEffectConfigPtr Effect;

    TExperimentAssignment();

    TExperimentAssignment(
        TString experiment,
        TString group,
        TString ticket,
        TString dimension,
        double experimentUniformSample,
        double groupUniformSample,
        TExperimentEffectConfigPtr effect);

    //! Returns experiment assignment name of form "<experiment>.<group>".
    TString GetName() const;
};

DEFINE_REFCOUNTED_TYPE(TExperimentAssignment)

////////////////////////////////////////////////////////////////////////////////

//! This method assigns experiments to an operation considering the possible
//! specification of experiment overrides in operation spec.
std::vector<TExperimentAssignmentPtr> AssignExperiments(
    EOperationType operationType,
    const TString& user,
    const NYTree::IMapNodePtr& specNode,
    const THashMap<TString, TExperimentConfigPtr>& experiments);

//! Validate experiment specification, in particular:
//! - validate total fraction sum over each dimension;
//! - validate that under no circumstances operation may be assigned
//!   to two different controller agent tags simultaneously.
void ValidateExperiments(const THashMap<TString, TExperimentConfigPtr>& experiments);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
