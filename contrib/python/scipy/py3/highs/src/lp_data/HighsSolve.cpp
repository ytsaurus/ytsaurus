/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/*                                                                       */
/*    This file is part of the HiGHS linear optimization suite           */
/*                                                                       */
/*    Written and engineered 2008-2024 by Julian Hall, Ivet Galabova,    */
/*    Leona Gottwald and Michael Feldmeier                               */
/*                                                                       */
/*    Available as open-source under the MIT License                     */
/*                                                                       */
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/**@file lp_data/HighsSolve.cpp
 * @brief Class-independent utilities for HiGHS
 */

#include "ipm/IpxWrapper.h"
#include "lp_data/HighsSolutionDebug.h"
#include "pdlp/CupdlpWrapper.h"
#include "simplex/HApp.h"

// The method below runs simplex or ipx solver on the lp.
HighsStatus solveLp(HighsLpSolverObject& solver_object, const string message) {
  HighsStatus return_status = HighsStatus::kOk;
  HighsStatus call_status;
  HighsOptions& options = solver_object.options_;
  // Reset unscaled model status and solution params - except for
  // iteration counts
  resetModelStatusAndHighsInfo(solver_object);
  highsLogUser(options.log_options, HighsLogType::kInfo,
               (message + "\n").c_str());
  if (options.highs_debug_level > kHighsDebugLevelMin) {
    // Shouldn't have to check validity of the LP since this is done when it is
    // loaded or modified
    call_status = assessLp(solver_object.lp_, options);
    // If any errors have been found or normalisation carried out,
    // call_status will be ERROR or WARNING, so only valid return is OK.
    assert(call_status == HighsStatus::kOk);
    return_status = interpretCallStatus(options.log_options, call_status,
                                        return_status, "assessLp");
    if (return_status == HighsStatus::kError) return return_status;
  }
  if (!solver_object.lp_.num_row_ || solver_object.lp_.a_matrix_.numNz() == 0) {
    // LP is unconstrained due to having no rows or a zero constraint
    // matrix, so solve directly
    call_status = solveUnconstrainedLp(solver_object);
    return_status = interpretCallStatus(options.log_options, call_status,
                                        return_status, "solveUnconstrainedLp");
    if (return_status == HighsStatus::kError) return return_status;
  } else if (options.solver == kIpmString || options.run_centring ||
             options.solver == kPdlpString) {
    // Use IPM or PDLP
    if (options.solver == kIpmString || options.run_centring) {
      // Use IPX to solve the LP
      try {
        call_status = solveLpIpx(solver_object);
      } catch (const std::exception& exception) {
        highsLogDev(options.log_options, HighsLogType::kError,
                    "Exception %s in solveLpIpx\n", exception.what());
        call_status = HighsStatus::kError;
      }
      return_status = interpretCallStatus(options.log_options, call_status,
                                          return_status, "solveLpIpx");
    } else {
      // Use cuPDLP-C to solve the LP
      try {
        call_status = solveLpCupdlp(solver_object);
      } catch (const std::exception& exception) {
        highsLogDev(options.log_options, HighsLogType::kError,
                    "Exception %s in solveLpCupdlp\n", exception.what());
        call_status = HighsStatus::kError;
      }
      return_status = interpretCallStatus(options.log_options, call_status,
                                          return_status, "solveLpCupdlp");
    }
    if (return_status == HighsStatus::kError) return return_status;
    // IPM (and PDLP?) can claim optimality with large primal and/or
    // dual residual errors, so must correct any residual errors that
    // exceed the tolerance in this scenario.
    //
    // OK to correct residual errors whatever the model status, as
    // it's only changed in the case of optimality
    correctResiduals(solver_object);

    // Non-error return requires a primal solution
    assert(solver_object.solution_.value_valid);
    // Get the objective and any KKT failures
    solver_object.highs_info_.objective_function_value =
        solver_object.lp_.objectiveValue(solver_object.solution_.col_value);
    getLpKktFailures(options, solver_object.lp_, solver_object.solution_,
                     solver_object.basis_, solver_object.highs_info_);
    if (solver_object.model_status_ == HighsModelStatus::kOptimal &&
        (solver_object.highs_info_.num_primal_infeasibilities > 0 ||
         solver_object.highs_info_.num_dual_infeasibilities))
      solver_object.model_status_ = HighsModelStatus::kUnknown;
    if (options.solver == kIpmString || options.run_centring) {
      // Setting the IPM-specific values of (highs_)info_ has been done in
      // solveLpIpx
      const bool unwelcome_ipx_status =
          solver_object.model_status_ == HighsModelStatus::kUnknown ||
          (solver_object.model_status_ ==
               HighsModelStatus::kUnboundedOrInfeasible &&
           !options.allow_unbounded_or_infeasible);
      if (unwelcome_ipx_status) {
        // When performing an analytic centre calculation, the setting
        // of options.run_crossover is ignored, so simplex clean-up is
        // not possible - or desirable, anyway!
        highsLogUser(
            options.log_options, HighsLogType::kWarning,
            "Unwelcome IPX status of %s: basis is %svalid; solution is "
            "%svalid; run_crossover is \"%s\"\n",
            utilModelStatusToString(solver_object.model_status_).c_str(),
            solver_object.basis_.valid ? "" : "not ",
            solver_object.solution_.value_valid ? "" : "not ",
            options.run_centring ? "off" : options.run_crossover.c_str());
        const bool allow_simplex_cleanup =
            options.run_crossover != kHighsOffString && !options.run_centring;
        if (allow_simplex_cleanup) {
          // IPX has returned a model status that HiGHS would rather
          // avoid, so perform simplex clean-up if crossover was allowed.
          //
          // This is an unusual situation, and the cost will usually be
          // acceptable. Worst case is if crossover wasn't run, in which
          // case there's no basis to start simplex
          //
          // ToDo: Check whether simplex can exploit the primal solution
          // returned by IPX
          highsLogUser(options.log_options, HighsLogType::kWarning,
                       "IPX solution is imprecise, so clean up with simplex\n");
          // Reset the return status since it will now be determined by
          // the outcome of the simplex solve
          return_status = HighsStatus::kOk;
          call_status = solveLpSimplex(solver_object);
          return_status = interpretCallStatus(options.log_options, call_status,
                                              return_status, "solveLpSimplex");
          if (return_status == HighsStatus::kError) return return_status;
          if (!isSolutionRightSize(solver_object.lp_,
                                   solver_object.solution_)) {
            highsLogUser(options.log_options, HighsLogType::kError,
                         "Inconsistent solution returned from solver\n");
            return HighsStatus::kError;
          }
        }  // options.run_crossover == kHighsOnString
           // clang-format off
      }  // unwelcome_ipx_status
      // clang-format on
    } else {
      // PDLP has been used, so check whether claim of optimality
      // satisfies the HiGHS criteria
      //
      // Even when PDLP terminates with primal and dual feasibility
      // and duality gap that are within the tolerances supplied by
      // HiGHS, the HiGHS primal and dual feasibility tolerances may
      // not be satisfied since they are absolute, and in PDLP they
      // are relative. Note that, even when only one PDLP row activity
      // fails to satisfy the absolute tolerance, the absolute norm
      // measure reported by PDLP will not necessarily be the same as
      // with HiGHS, since PDLP uses the 2-norm, and HiGHS the
      // infinity- and 1-norm
      //
      // A single small HiGHS primal infeasibility from PDLP can yield
      // a significant dual infeasibility, since the variable is
      // interpreted as being off its bound so any dual value is an
      // infeasibility. Hence, for context, the max and sum of
      // complementarity violations are also computed.
      const HighsInfo& info = solver_object.highs_info_;
      if (solver_object.model_status_ == HighsModelStatus::kOptimal) {
        if (info.num_primal_infeasibilities || info.num_dual_infeasibilities) {
          if (info.num_primal_infeasibilities) {
            highsLogUser(options.log_options, HighsLogType::kWarning,
                         "PDLP claims optimality, but with num/max/sum %d / "
                         "%9.4g / %9.4g primal infeasibilities\n",
                         int(info.num_primal_infeasibilities),
                         info.max_primal_infeasibility,
                         info.sum_primal_infeasibilities);
          } else if (info.num_dual_infeasibilities) {
            highsLogUser(options.log_options, HighsLogType::kWarning,
                         "PDLP claims optimality, but with num/max/sum %d / "
                         "%9.4g / %9.4g dual infeasibilities\n",
                         int(info.num_dual_infeasibilities),
                         info.max_dual_infeasibility,
                         info.sum_dual_infeasibilities);
          }
          highsLogUser(options.log_options, HighsLogType::kWarning,
                       "                        and          max/sum     %9.4g "
                       "/ %9.4g complementarity violations\n",
                       info.max_complementarity_violation,
                       info.sum_complementarity_violations);
          highsLogUser(
              options.log_options, HighsLogType::kWarning,
              "                        so set model status to \"unknown\"\n");
          solver_object.model_status_ = HighsModelStatus::kUnknown;
        }
      } else if (solver_object.model_status_ ==
                 HighsModelStatus::kUnboundedOrInfeasible) {
        if (info.num_primal_infeasibilities == 0)
          solver_object.model_status_ = HighsModelStatus::kUnbounded;
      }
    }
  } else {
    // Use Simplex
    call_status = solveLpSimplex(solver_object);
    return_status = interpretCallStatus(options.log_options, call_status,
                                        return_status, "solveLpSimplex");
    if (return_status == HighsStatus::kError) return return_status;
    if (!isSolutionRightSize(solver_object.lp_, solver_object.solution_)) {
      highsLogUser(options.log_options, HighsLogType::kError,
                   "Inconsistent solution returned from solver\n");
      return HighsStatus::kError;
    }
  }
  // Analyse the HiGHS (basic) solution
  if (debugHighsLpSolution(message, solver_object) ==
      HighsDebugStatus::kLogicalError)
    return_status = HighsStatus::kError;
  return return_status;
}

// Solves an unconstrained LP without scaling, setting HighsBasis, HighsSolution
// and HighsInfo
HighsStatus solveUnconstrainedLp(HighsLpSolverObject& solver_object) {
  return (solveUnconstrainedLp(solver_object.options_, solver_object.lp_,
                               solver_object.model_status_,
                               solver_object.highs_info_,
                               solver_object.solution_, solver_object.basis_));
}

// Solves an unconstrained LP without scaling, setting HighsBasis, HighsSolution
// and HighsInfo
HighsStatus solveUnconstrainedLp(const HighsOptions& options, const HighsLp& lp,
                                 HighsModelStatus& model_status,
                                 HighsInfo& highs_info, HighsSolution& solution,
                                 HighsBasis& basis) {
  // Aliase to model status and solution parameters
  resetModelStatusAndHighsInfo(model_status, highs_info);

  // Check that the LP really is unconstrained!
  assert(lp.num_row_ == 0 || lp.a_matrix_.numNz() == 0);
  if (lp.num_row_ > 0) {
    // LP has rows, but should only be here if the constraint matrix
    // is zero
    if (lp.a_matrix_.numNz() > 0) return HighsStatus::kError;
  }

  highsLogUser(options.log_options, HighsLogType::kInfo,
               "Solving an unconstrained LP with %" HIGHSINT_FORMAT
               " columns\n",
               lp.num_col_);
  solution.col_value.assign(lp.num_col_, 0);
  solution.col_dual.assign(lp.num_col_, 0);
  basis.col_status.assign(lp.num_col_, HighsBasisStatus::kNonbasic);
  // No rows for primal solution, dual solution or basis
  solution.row_value.clear();
  solution.row_dual.clear();
  basis.row_status.clear();

  double primal_feasibility_tolerance = options.primal_feasibility_tolerance;
  double dual_feasibility_tolerance = options.dual_feasibility_tolerance;

  // Initialise the objective value calculation. Done using
  // HighsSolution so offset is vanilla
  double objective = lp.offset_;

  highs_info.num_primal_infeasibilities = 0;
  highs_info.max_primal_infeasibility = 0;
  highs_info.sum_primal_infeasibilities = 0;
  highs_info.num_dual_infeasibilities = 0;
  highs_info.max_dual_infeasibility = 0;
  highs_info.sum_dual_infeasibilities = 0;

  if (lp.num_row_ > 0) {
    // Assign primal, dual and basis status for rows, checking for
    // infeasibility
    for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
      double primal_infeasibility = 0;
      double lower = lp.row_lower_[iRow];
      double upper = lp.row_upper_[iRow];
      if (lower > primal_feasibility_tolerance) {
        // Lower bound too large for zero activity
        primal_infeasibility = lower;
      } else if (upper < -primal_feasibility_tolerance) {
        // Upper bound too small for zero activity
        primal_infeasibility = -upper;
      }
      solution.row_value.push_back(0);
      solution.row_dual.push_back(0);
      basis.row_status.push_back(HighsBasisStatus::kBasic);
      if (primal_infeasibility > primal_feasibility_tolerance)
        highs_info.num_primal_infeasibilities++;
      highs_info.sum_primal_infeasibilities += primal_infeasibility;
      highs_info.max_primal_infeasibility =
          std::max(primal_infeasibility, highs_info.max_primal_infeasibility);
    }
  }

  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    double cost = lp.col_cost_[iCol];
    double dual = (HighsInt)lp.sense_ * cost;
    double lower = lp.col_lower_[iCol];
    double upper = lp.col_upper_[iCol];
    double value;
    double primal_infeasibility = 0;
    double dual_infeasibility = -1;
    HighsBasisStatus status = HighsBasisStatus::kNonbasic;
    if (lower > upper) {
      // Inconsistent bounds, so set the variable to lower bound,
      // unless it's infinite. Otherwise set the variable to upper
      // bound, unless it's infinite. Otherwise set the variable to
      // zero.
      if (highs_isInfinity(lower)) {
        // Lower bound of +inf
        if (highs_isInfinity(-upper)) {
          // Upper bound of -inf
          value = 0;
          status = HighsBasisStatus::kZero;
          primal_infeasibility = kHighsInf;
          dual_infeasibility = std::fabs(dual);
        } else {
          // Finite upper bound - since lower exceeds it
          value = upper;
          status = HighsBasisStatus::kUpper;
          primal_infeasibility = lower - value;
          dual_infeasibility = std::max(dual, 0.);
        }
      } else {
        // Finite lower bound
        value = lower;
        status = HighsBasisStatus::kLower;
        primal_infeasibility = value - upper;
        dual_infeasibility = std::max(-dual, 0.);
      }
    } else if (highs_isInfinity(-lower) && highs_isInfinity(upper)) {
      // Free column: set to zero and record dual infeasibility
      value = 0;
      status = HighsBasisStatus::kZero;
      dual_infeasibility = std::fabs(dual);
    } else if (dual >= dual_feasibility_tolerance) {
      // Column with sufficiently positive dual
      if (!highs_isInfinity(-lower)) {
        // Set to this finite lower bound
        value = lower;
        status = HighsBasisStatus::kLower;
        dual_infeasibility = 0;
      } else {
        // Infinite lower bound so set to upper bound and record dual
        // infeasibility
        value = upper;
        status = HighsBasisStatus::kUpper;
        dual_infeasibility = dual;
      }
    } else if (dual <= -dual_feasibility_tolerance) {
      // Column with sufficiently negative dual
      if (!highs_isInfinity(upper)) {
        // Set to this finite upper bound
        value = upper;
        status = HighsBasisStatus::kUpper;
        dual_infeasibility = 0;
      } else {
        // Infinite upper bound so set to lower bound and record dual
        // infeasibility
        value = lower;
        status = HighsBasisStatus::kLower;
        dual_infeasibility = -dual;
      }
    } else {
      // Column with sufficiently small dual: set to lower bound (if
      // finite) otherwise upper bound
      if (highs_isInfinity(-lower)) {
        value = upper;
        status = HighsBasisStatus::kUpper;
      } else {
        value = lower;
        status = HighsBasisStatus::kLower;
      }
      dual_infeasibility = std::fabs(dual);
    }
    assert(status != HighsBasisStatus::kNonbasic);
    assert(dual_infeasibility >= 0);
    solution.col_value[iCol] = value;
    solution.col_dual[iCol] = (HighsInt)lp.sense_ * dual;
    basis.col_status[iCol] = status;
    objective += value * cost;
    if (primal_infeasibility > primal_feasibility_tolerance)
      highs_info.num_primal_infeasibilities++;
    highs_info.sum_primal_infeasibilities += primal_infeasibility;
    highs_info.max_primal_infeasibility =
        std::max(primal_infeasibility, highs_info.max_primal_infeasibility);
    if (dual_infeasibility > dual_feasibility_tolerance)
      highs_info.num_dual_infeasibilities++;
    highs_info.sum_dual_infeasibilities += dual_infeasibility;
    highs_info.max_dual_infeasibility =
        std::max(dual_infeasibility, highs_info.max_dual_infeasibility);
  }
  highs_info.objective_function_value = objective;
  solution.value_valid = true;
  solution.dual_valid = true;
  basis.valid = true;
  highs_info.basis_validity = kBasisValidityValid;
  setSolutionStatus(highs_info);
  if (highs_info.num_primal_infeasibilities) {
    // Primal infeasible
    model_status = HighsModelStatus::kInfeasible;
  } else if (highs_info.num_dual_infeasibilities) {
    // Dual infeasible => primal unbounded for unconstrained LP
    model_status = HighsModelStatus::kUnbounded;
  } else {
    model_status = HighsModelStatus::kOptimal;
  }

  return HighsStatus::kOk;
}

void assessExcessiveBoundCost(const HighsLogOptions log_options,
                              const HighsModel& model) {
  auto assessFiniteNonzero = [&](const double value, double& min_value,
                                 double& max_value) {
    double abs_value = std::abs(value);
    if (abs_value > 0 && abs_value < kHighsInf) {
      min_value = std::min(abs_value, min_value);
      max_value = std::max(abs_value, max_value);
    }
  };
  const HighsLp& lp = model.lp_;
  double min_finite_col_cost = kHighsInf;
  double max_finite_col_cost = -kHighsInf;
  double min_finite_col_bound = kHighsInf;
  double max_finite_col_bound = -kHighsInf;
  double min_finite_row_bound = kHighsInf;
  double max_finite_row_bound = -kHighsInf;
  double min_matrix_value = kHighsInf;
  double max_matrix_value = -kHighsInf;
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    assessFiniteNonzero(lp.col_cost_[iCol], min_finite_col_cost,
                        max_finite_col_cost);
    assessFiniteNonzero(lp.col_lower_[iCol], min_finite_col_bound,
                        max_finite_col_bound);
    assessFiniteNonzero(lp.col_upper_[iCol], min_finite_col_bound,
                        max_finite_col_bound);
  }
  if (min_finite_col_cost == kHighsInf) min_finite_col_cost = 0;
  if (max_finite_col_cost == -kHighsInf) max_finite_col_cost = 0;
  if (min_finite_col_bound == kHighsInf) min_finite_col_bound = 0;
  if (max_finite_col_bound == -kHighsInf) max_finite_col_bound = 0;
  for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
    assessFiniteNonzero(lp.row_lower_[iRow], min_finite_row_bound,
                        max_finite_row_bound);
    assessFiniteNonzero(lp.row_upper_[iRow], min_finite_row_bound,
                        max_finite_row_bound);
  }
  if (min_finite_row_bound == kHighsInf) min_finite_row_bound = 0;
  if (max_finite_row_bound == -kHighsInf) max_finite_row_bound = 0;
  HighsInt num_nz = lp.a_matrix_.numNz();
  for (HighsInt iEl = 0; iEl < num_nz; iEl++)
    assessFiniteNonzero(lp.a_matrix_.value_[iEl], min_matrix_value,
                        max_matrix_value);

  highsLogUser(log_options, HighsLogType::kInfo, "Coefficient ranges:\n");
  if (num_nz)
    highsLogUser(log_options, HighsLogType::kInfo, "  Matrix [%5.0e, %5.0e]\n",
                 min_matrix_value, max_matrix_value);
  if (lp.num_col_) {
    highsLogUser(log_options, HighsLogType::kInfo, "  Cost   [%5.0e, %5.0e]\n",
                 min_finite_col_cost, max_finite_col_cost);
    highsLogUser(log_options, HighsLogType::kInfo, "  Bound  [%5.0e, %5.0e]\n",
                 min_finite_col_bound, max_finite_col_bound);
  }
  if (lp.num_row_)
    highsLogUser(log_options, HighsLogType::kInfo, "  RHS    [%5.0e, %5.0e]\n",
                 min_finite_row_bound, max_finite_row_bound);

  // LPs with no columns or no finite nonzero costs will have
  // max_finite_col_cost = 0
  assert(max_finite_col_cost >= 0);
  if (max_finite_col_cost > kExcessivelyLargeCostValue) {
    double user_cost_scale_value = std::pow(2, lp.user_cost_scale_);
    // Warn that costs are excessively large, and suggest scaling
    double ratio = kExcessivelyLargeCostValue /
                   (max_finite_col_cost / user_cost_scale_value);
    HighsInt suggested_user_cost_scale_setting = std::floor(std::log2(ratio));
    HighsInt suggested_cost_scale_exponent = std::floor(std::log10(ratio));
    highsLogUser(
        log_options, HighsLogType::kWarning,
        "%s has excessively large costs: consider scaling the costs "
        "by 1e%+1d or less, or setting option user_cost_scale to %d or less\n",
        lp.user_cost_scale_ ? "User-scaled problem" : "Problem",
        int(-suggested_cost_scale_exponent),
        int(suggested_user_cost_scale_setting));
  }
  // LPs with no columns or no finite nonzero bounds will have
  // max_finite_col_bound = 0
  assert(max_finite_col_bound >= 0);
  if (max_finite_col_bound > kExcessivelyLargeBoundValue) {
    double user_bound_scale_value = std::pow(2, lp.user_bound_scale_);
    // Warn that bounds are excessively large, and suggest scaling
    double ratio = kExcessivelyLargeBoundValue /
                   (max_finite_col_bound / user_bound_scale_value);
    HighsInt suggested_user_bound_scale = std::floor(std::log2(ratio));
    HighsInt suggested_bound_scale_exponent = std::floor(std::log10(ratio));
    if (lp.isMip()) {
      highsLogUser(
          log_options, HighsLogType::kWarning,
          "%s has excessively large bounds: consider scaling the bounds "
          "by 1e%+1d or less\n",
          lp.user_bound_scale_ ? "User-scaled problem" : "Problem",
          int(-suggested_bound_scale_exponent));
    } else {
      highsLogUser(
          log_options, HighsLogType::kWarning,
          "%s has excessively large bounds: consider scaling the bounds "
          "by 1e%+1d or less, "
          "or setting option user_bound_scale to %d or less\n",
          lp.user_bound_scale_ ? "User-scaled problem" : "Problem",
          int(-suggested_bound_scale_exponent),
          int(suggested_user_bound_scale));
    }
  }
  // LPs with no rows or no finite nonzero bounds will have
  // max_finite_row_bound = 0
  assert(max_finite_row_bound >= 0);
  if (max_finite_row_bound > kExcessivelyLargeBoundValue) {
    double user_bound_scale_value = std::pow(2, lp.user_bound_scale_);
    // Warn that bounds are excessively large, and suggest scaling
    double ratio = kExcessivelyLargeBoundValue /
                   (max_finite_row_bound / user_bound_scale_value);
    HighsInt suggested_user_bound_scale = std::floor(std::log2(ratio));
    HighsInt suggested_bound_scale_exponent = std::floor(std::log10(ratio));
    if (lp.isMip()) {
      highsLogUser(
          log_options, HighsLogType::kWarning,
          "%s has excessively large bounds: consider scaling the bounds "
          "by 1e%+1d or less\n",
          lp.user_bound_scale_ ? "User-scaled problem" : "Problem",
          int(-suggested_bound_scale_exponent));
    } else {
      highsLogUser(
          log_options, HighsLogType::kWarning,
          "%s has excessively large bounds: consider scaling the bounds "
          "by 1e%+1d or less, "
          "or setting option user_bound_scale to %d or less\n",
          lp.user_bound_scale_ ? "User-scaled problem" : "Problem",
          int(-suggested_bound_scale_exponent),
          int(suggested_user_bound_scale));
    }
  }
  // Now consider warning relating to small maximum costs and bounds
  if (max_finite_col_cost > 0 &&
      max_finite_col_cost < kExcessivelySmallCostValue) {
    double user_cost_scale_value = std::pow(2, lp.user_cost_scale_);
    // Warn that costs are excessively small, and suggest scaling
    double ratio = kExcessivelySmallCostValue /
                   (max_finite_col_cost / user_cost_scale_value);
    HighsInt suggested_user_cost_scale_setting = std::ceil(std::log2(ratio));
    HighsInt suggested_cost_scale_exponent = std::ceil(std::log10(ratio));
    highsLogUser(
        log_options, HighsLogType::kWarning,
        "%s has excessively small costs: consider scaling the costs up "
        "by 1e%+1d or more, "
        "or setting option user_cost_scale to %d or more\n",
        lp.user_cost_scale_ ? "User-scaled problem" : "Problem",
        int(suggested_cost_scale_exponent),
        int(suggested_user_cost_scale_setting));
  }
  if (max_finite_col_bound > 0 &&
      max_finite_col_bound < kExcessivelySmallBoundValue) {
    double user_bound_scale_value = std::pow(2, lp.user_bound_scale_);
    // Warn that bounds are excessively small, and suggest scaling
    double ratio = kExcessivelySmallBoundValue /
                   (max_finite_col_bound / user_bound_scale_value);
    HighsInt suggested_user_bound_scale = std::ceil(std::log2(ratio));
    HighsInt suggested_bound_scale_exponent = std::ceil(std::log10(ratio));
    if (lp.isMip()) {
      highsLogUser(
          log_options, HighsLogType::kWarning,
          "%s has excessively small bounds: consider scaling the bounds "
          "by 1e%+1d or more\n",
          lp.user_bound_scale_ ? "User-scaled problem" : "Problem",
          int(suggested_bound_scale_exponent));
    } else {
      highsLogUser(
          log_options, HighsLogType::kWarning,
          "%s has excessively small bounds: consider scaling the bounds "
          "by 1e%+1d or more, "
          "or setting option user_bound_scale to %d or more\n",
          lp.user_bound_scale_ ? "User-scaled problem" : "Problem",
          int(suggested_bound_scale_exponent), int(suggested_user_bound_scale));
    }
  }
  if (max_finite_row_bound > 0 &&
      max_finite_row_bound < kExcessivelySmallBoundValue) {
    double user_bound_scale_value = std::pow(2, lp.user_bound_scale_);
    // Warn that bounds are excessively small, and suggest scaling
    double ratio = kExcessivelySmallBoundValue /
                   (max_finite_row_bound / user_bound_scale_value);
    HighsInt suggested_user_bound_scale = std::ceil(std::log2(ratio));
    HighsInt suggested_bound_scale_exponent = std::ceil(std::log10(ratio));
    if (lp.isMip()) {
      highsLogUser(
          log_options, HighsLogType::kWarning,
          "%s has excessively small bounds: consider scaling the bounds "
          "by 1e%+1d or more\n",
          lp.user_bound_scale_ ? "User-scaled problem" : "Problem",
          int(suggested_bound_scale_exponent));
    } else {
      highsLogUser(
          log_options, HighsLogType::kWarning,
          "%s has excessively small bounds: consider scaling the bounds "
          "by 1e%+1d or more, "
          "or setting option user_bound_scale to %d or more\n",
          lp.user_bound_scale_ ? "User-scaled problem" : "Problem",
          int(suggested_bound_scale_exponent), int(suggested_user_bound_scale));
    }
  }
}
