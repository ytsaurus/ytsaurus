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
/**@file lp_data/HighsUtils.cpp
 * @brief Class-independent utilities for HiGHS
 */
#include "lp_data/HighsLpUtils.h"

#include <algorithm>
#include <cassert>

#include "HConfig.h"
#include "io/Filereader.h"
#include "io/HMPSIO.h"
#include "io/HighsIO.h"
#include "lp_data/HighsModelUtils.h"
#include "lp_data/HighsSolution.h"
#include "lp_data/HighsStatus.h"
#include "util/HighsCDouble.h"
#include "util/HighsMatrixUtils.h"
#include "util/HighsSort.h"
#include "util/HighsTimer.h"

using std::fabs;
using std::max;
using std::min;

const HighsInt kMaxLineLength = 80;

HighsStatus assessLp(HighsLp& lp, const HighsOptions& options) {
  HighsStatus return_status = HighsStatus::kOk;
  HighsStatus call_status = lpDimensionsOk("assessLp", lp, options.log_options)
                                ? HighsStatus::kOk
                                : HighsStatus::kError;
  return_status = interpretCallStatus(options.log_options, call_status,
                                      return_status, "assessLpDimensions");
  if (return_status == HighsStatus::kError) return return_status;

  if (lp.num_col_) {
    // Assess the LP column costs
    HighsIndexCollection index_collection;
    index_collection.dimension_ = lp.num_col_;
    index_collection.is_interval_ = true;
    index_collection.from_ = 0;
    index_collection.to_ = lp.num_col_ - 1;
    call_status = assessCosts(options, 0, index_collection, lp.col_cost_,
                              lp.has_infinite_cost_, options.infinite_cost);
    return_status = interpretCallStatus(options.log_options, call_status,
                                        return_status, "assessCosts");
    if (return_status == HighsStatus::kError) return return_status;
    // Assess the LP column bounds
    call_status = assessBounds(
        options, "Col", 0, index_collection, lp.col_lower_, lp.col_upper_,
        options.infinite_bound, lp.isMip() ? lp.integrality_.data() : nullptr);
    return_status = interpretCallStatus(options.log_options, call_status,
                                        return_status, "assessBounds");
    if (return_status == HighsStatus::kError) return return_status;
  }
  if (lp.num_row_) {
    // Assess the LP row bounds
    HighsIndexCollection index_collection;
    index_collection.dimension_ = lp.num_row_;
    index_collection.is_interval_ = true;
    index_collection.from_ = 0;
    index_collection.to_ = lp.num_row_ - 1;
    call_status =
        assessBounds(options, "Row", 0, index_collection, lp.row_lower_,
                     lp.row_upper_, options.infinite_bound);
    return_status = interpretCallStatus(options.log_options, call_status,
                                        return_status, "assessBounds");
    if (return_status == HighsStatus::kError) return return_status;
  }
  // If the LP has no columns the matrix must be empty and there is
  // nothing left to test
  if (lp.num_col_ == 0) {
    assert(!lp.a_matrix_.numNz());
    return HighsStatus::kOk;
  }
  // From here, any LP has lp.num_col_ > 0 and lp.a_matrix_.start_[lp.num_col_]
  // exists (as the number of nonzeros)
  assert(lp.num_col_ > 0);

  // Assess the LP matrix - even if there are no rows!
  call_status =
      lp.a_matrix_.assess(options.log_options, "LP", options.small_matrix_value,
                          options.large_matrix_value);
  return_status = interpretCallStatus(options.log_options, call_status,
                                      return_status, "assessMatrix");
  if (return_status == HighsStatus::kError) return return_status;
  // If entries have been removed from the matrix, resize the index
  // and value vectors to prevent bug in presolve
  HighsInt lp_num_nz = lp.a_matrix_.numNz();
  if ((HighsInt)lp.a_matrix_.index_.size() > lp_num_nz)
    lp.a_matrix_.index_.resize(lp_num_nz);
  if ((HighsInt)lp.a_matrix_.value_.size() > lp_num_nz)
    lp.a_matrix_.value_.resize(lp_num_nz);
  if (return_status != HighsStatus::kOk)
    highsLogDev(options.log_options, HighsLogType::kInfo,
                "assessLp returns HighsStatus = %s\n",
                highsStatusToString(return_status).c_str());
  return return_status;
}

bool lpDimensionsOk(std::string message, const HighsLp& lp,
                    const HighsLogOptions& log_options) {
  bool ok = true;
  const HighsInt num_col = lp.num_col_;
  const HighsInt num_row = lp.num_row_;
  if (!(num_col >= 0))
    highsLogUser(log_options, HighsLogType::kError,
                 "LP dimension validation (%s) fails on num_col = %d >= 0\n",
                 message.c_str(), (int)num_col);
  ok = num_col >= 0 && ok;
  if (!(num_row >= 0))
    highsLogUser(log_options, HighsLogType::kError,
                 "LP dimension validation (%s) fails on num_row = %d >= 0\n",
                 message.c_str(), (int)num_row);
  ok = num_row >= 0 && ok;
  if (!ok) return ok;

  HighsInt col_cost_size = lp.col_cost_.size();
  HighsInt col_lower_size = lp.col_lower_.size();
  HighsInt col_upper_size = lp.col_upper_.size();
  bool legal_col_cost_size = col_cost_size >= num_col;
  bool legal_col_lower_size = col_lower_size >= num_col;
  bool legal_col_upper_size = col_lower_size >= num_col;
  if (!legal_col_cost_size)
    highsLogUser(log_options, HighsLogType::kError,
                 "LP dimension validation (%s) fails on col_cost.size() = %d < "
                 "%d = num_col\n",
                 message.c_str(), (int)col_cost_size, (int)num_col);
  ok = legal_col_cost_size && ok;
  if (!legal_col_lower_size)
    highsLogUser(log_options, HighsLogType::kError,
                 "LP dimension validation (%s) fails on col_lower.size() = %d "
                 "< %d = num_col\n",
                 message.c_str(), (int)col_lower_size, (int)num_col);
  ok = legal_col_lower_size && ok;
  if (!legal_col_upper_size)
    highsLogUser(log_options, HighsLogType::kError,
                 "LP dimension validation (%s) fails on col_upper.size() = %d "
                 "< %d = num_col\n",
                 message.c_str(), (int)col_upper_size, (int)num_col);
  ok = legal_col_upper_size && ok;

  bool legal_format = lp.a_matrix_.format_ == MatrixFormat::kColwise ||
                      lp.a_matrix_.format_ == MatrixFormat::kRowwise;
  if (!legal_format)
    highsLogUser(log_options, HighsLogType::kError,
                 "LP dimension validation (%s) fails on a_matrix_.format\n",
                 message.c_str());
  ok = legal_format && ok;
  HighsInt num_vec;
  if (lp.a_matrix_.isColwise()) {
    num_vec = num_col;
  } else {
    num_vec = num_row;
  }
  const bool partitioned = false;
  vector<HighsInt> a_matrix_p_end;
  bool legal_matrix_dimensions =
      assessMatrixDimensions(log_options, num_vec, partitioned,
                             lp.a_matrix_.start_, a_matrix_p_end,
                             lp.a_matrix_.index_,
                             lp.a_matrix_.value_) == HighsStatus::kOk;
  if (!legal_matrix_dimensions)
    highsLogUser(log_options, HighsLogType::kError,
                 "LP dimension validation (%s) fails on a_matrix dimensions\n",
                 message.c_str());
  ok = legal_matrix_dimensions && ok;

  HighsInt row_lower_size = lp.row_lower_.size();
  HighsInt row_upper_size = lp.row_upper_.size();
  bool legal_row_lower_size = row_lower_size >= num_row;
  bool legal_row_upper_size = row_upper_size >= num_row;
  if (!legal_row_lower_size)
    highsLogUser(log_options, HighsLogType::kError,
                 "LP dimension validation (%s) fails on row_lower.size() = %d "
                 "< %d = num_row\n",
                 message.c_str(), (int)row_lower_size, (int)num_row);
  ok = legal_row_lower_size && ok;
  if (!legal_row_upper_size)
    highsLogUser(log_options, HighsLogType::kError,
                 "LP dimension validation (%s) fails on row_upper.size() = %d "
                 "< %d = num_row\n",
                 message.c_str(), (int)row_upper_size, (int)num_row);
  ok = legal_row_upper_size && ok;

  bool legal_a_matrix_num_col = lp.a_matrix_.num_col_ == num_col;
  bool legal_a_matrix_num_row = lp.a_matrix_.num_row_ == num_row;
  if (!legal_a_matrix_num_col)
    highsLogUser(log_options, HighsLogType::kError,
                 "LP dimension validation (%s) fails on a_matrix.num_col_ = %d "
                 "!= %d = num_col\n",
                 message.c_str(), (int)lp.a_matrix_.num_col_, (int)num_col);
  ok = legal_a_matrix_num_col && ok;
  if (!legal_a_matrix_num_row)
    highsLogUser(log_options, HighsLogType::kError,
                 "LP dimension validation (%s) fails on a_matrix.num_row_ = %d "
                 "!= %d = num_row\n",
                 message.c_str(), (int)lp.a_matrix_.num_row_, (int)num_row);
  ok = legal_a_matrix_num_row && ok;

  HighsInt scale_strategy = (HighsInt)lp.scale_.strategy;
  bool legal_scale_strategy = scale_strategy >= 0;
  if (!legal_scale_strategy)
    highsLogUser(
        log_options, HighsLogType::kError,
        "LP dimension validation (%s) fails on scale_.scale_strategy\n",
        message.c_str());
  ok = legal_scale_strategy && ok;
  HighsInt scale_row_size = (HighsInt)lp.scale_.row.size();
  HighsInt scale_col_size = (HighsInt)lp.scale_.col.size();
  bool legal_scale_num_col = false;
  bool legal_scale_num_row = false;
  bool legal_scale_row_size = false;
  bool legal_scale_col_size = false;
  if (lp.scale_.has_scaling) {
    legal_scale_num_col = lp.scale_.num_col == num_col;
    legal_scale_num_row = lp.scale_.num_row == num_row;
    legal_scale_row_size = scale_row_size >= num_row;
    legal_scale_col_size = scale_col_size >= num_col;
  } else {
    legal_scale_num_col = lp.scale_.num_col == 0;
    legal_scale_num_row = lp.scale_.num_row == 0;
    legal_scale_row_size = scale_row_size == 0;
    legal_scale_col_size = scale_col_size == 0;
  }
  if (!legal_scale_num_col)
    highsLogUser(
        log_options, HighsLogType::kError,
        "LP dimension validation (%s) fails on scale_.num_col = %d != %d\n",
        message.c_str(), (int)lp.scale_.num_col,
        (int)(lp.scale_.has_scaling ? num_col : 0));
  ok = legal_scale_num_col && ok;
  if (!legal_scale_num_row)
    highsLogUser(
        log_options, HighsLogType::kError,
        "LP dimension validation (%s) fails on scale_.num_row = %d != %d\n",
        message.c_str(), (int)lp.scale_.num_row,
        (int)(lp.scale_.has_scaling ? num_row : 0));
  ok = legal_scale_num_row && ok;
  if (!legal_scale_col_size)
    highsLogUser(
        log_options, HighsLogType::kError,
        "LP dimension validation (%s) fails on scale_.col.size() = %d %s %d\n",
        message.c_str(), (int)scale_col_size,
        lp.scale_.has_scaling ? ">=" : "==",
        (int)(lp.scale_.has_scaling ? num_col : 0));
  ok = legal_scale_col_size && ok;
  if (!legal_scale_row_size)
    highsLogUser(
        log_options, HighsLogType::kError,
        "LP dimension validation (%s) fails on scale_.row.size() = %d %s %d\n",
        message.c_str(), (int)scale_row_size,
        lp.scale_.has_scaling ? ">=" : "==",
        (int)(lp.scale_.has_scaling ? num_row : 0));
  ok = legal_scale_row_size && ok;
  if (!ok) {
    highsLogUser(log_options, HighsLogType::kError,
                 "LP dimension validation (%s) fails\n", message.c_str());
  }

  return ok;
}

HighsStatus assessCosts(const HighsOptions& options, const HighsInt ml_col_os,
                        const HighsIndexCollection& index_collection,
                        vector<double>& cost, bool& has_infinite_cost,
                        const double infinite_cost) {
  HighsStatus return_status = HighsStatus::kOk;
  assert(ok(index_collection));
  HighsInt from_k;
  HighsInt to_k;
  limits(index_collection, from_k, to_k);
  if (from_k > to_k) return return_status;

  return_status = HighsStatus::kOk;
  // Work through the data to be assessed.
  //
  // Loop is k \in [from_k...to_k) covering the entries in the
  // interval, set or mask to be considered.
  //
  // For an interval or mask, these values of k are the columns to be
  // considered in a local sense, as well as the entries in the
  // cost data to be assessed
  //
  // For a set, these values of k are the indices in the set, from
  // which the columns to be considered in a local sense are
  // drawn. The entries in the cost data to be assessed correspond
  // to the values of k
  //
  // Adding the value of ml_col_os to local_col yields the value of
  // ml_col, being the column in a global (whole-model) sense. This is
  // necessary when assessing the costs of columns being added to a
  // model, since they are specified using an interval
  // [0...num_new_col) which must be offset by the current number of
  // columns in the model.
  //
  HighsInt local_col;
  HighsInt usr_col = -1;
  HighsInt num_infinite_cost = 0;
  for (HighsInt k = from_k; k < to_k + 1; k++) {
    if (index_collection.is_interval_ || index_collection.is_mask_) {
      local_col = k;
    } else {
      local_col = index_collection.set_[k];
    }
    if (index_collection.is_interval_) {
      usr_col++;
    } else {
      usr_col = k;
    }
    if (index_collection.is_mask_ && !index_collection.mask_[local_col])
      continue;
    if (cost[usr_col] >= infinite_cost) {
      num_infinite_cost++;
      cost[usr_col] = kHighsInf;
    } else if (cost[usr_col] <= -infinite_cost) {
      num_infinite_cost++;
      cost[usr_col] = -kHighsInf;
    }
  }
  if (num_infinite_cost > 0) {
    has_infinite_cost = true;
    highsLogUser(options.log_options, HighsLogType::kInfo,
                 "%" HIGHSINT_FORMAT
                 " |cost| values greater than or equal to %12g are treated as "
                 "Infinity\n",
                 num_infinite_cost, infinite_cost);
  }
  return return_status;
}

HighsStatus assessBounds(const HighsOptions& options, const char* type,
                         const HighsInt ml_ix_os,
                         const HighsIndexCollection& index_collection,
                         vector<double>& lower, vector<double>& upper,
                         const double infinite_bound,
                         const HighsVarType* integrality) {
  HighsStatus return_status = HighsStatus::kOk;
  assert(ok(index_collection));
  HighsInt from_k;
  HighsInt to_k;
  limits(index_collection, from_k, to_k);
  if (from_k > to_k) return HighsStatus::kOk;

  return_status = HighsStatus::kOk;
  bool error_found = false;
  bool warning_found = false;
  // Work through the data to be assessed.
  //
  // Loop is k \in [from_k...to_k) covering the entries in the
  // interval, set or mask to be considered.
  //
  // For an interval or mask, these values of k are the row/column
  // indices to be considered in a local sense, as well as the entries
  // in the lower and upper bound data to be assessed
  //
  // For a set, these values of k are the indices in the set, from
  // which the indices to be considered in a local sense are
  // drawn. The entries in the lower and
  // upper bound data to be assessed correspond to the values of
  // k.
  //
  // Adding the value of ml_ix_os to local_ix yields the value of
  // ml_ix, being the index in a global (whole-model) sense. This is
  // necessary when assessing the bounds of rows/columns being added
  // to a model, since they are specified using an interval
  // [0...num_new_row/col) which must be offset by the current number
  // of rows/columns (generically indices) in the model.
  //
  HighsInt num_infinite_lower_bound = 0;
  HighsInt num_infinite_upper_bound = 0;
  HighsInt local_ix;
  HighsInt ml_ix;
  HighsInt usr_ix = -1;
  for (HighsInt k = from_k; k < to_k + 1; k++) {
    if (index_collection.is_interval_ || index_collection.is_mask_) {
      local_ix = k;
    } else {
      local_ix = index_collection.set_[k];
    }
    if (index_collection.is_interval_) {
      usr_ix++;
    } else {
      usr_ix = k;
    }
    ml_ix = ml_ix_os + local_ix;
    if (index_collection.is_mask_ && !index_collection.mask_[local_ix])
      continue;

    if (!highs_isInfinity(-lower[usr_ix])) {
      // Check whether a finite lower bound will be treated as -Infinity
      bool infinite_lower_bound = lower[usr_ix] <= -infinite_bound;
      if (infinite_lower_bound) {
        lower[usr_ix] = -kHighsInf;
        num_infinite_lower_bound++;
      }
    }
    if (!highs_isInfinity(upper[usr_ix])) {
      // Check whether a finite upper bound will be treated as Infinity
      bool infinite_upper_bound = upper[usr_ix] >= infinite_bound;
      if (infinite_upper_bound) {
        upper[usr_ix] = kHighsInf;
        num_infinite_upper_bound++;
      }
    }
    // Check that the lower bound does not exceed the upper bound
    bool legalLowerUpperBound = lower[usr_ix] <= upper[usr_ix];
    if (integrality) {
      // Legal for semi-variables to have inconsistent bounds
      if (integrality[usr_ix] == HighsVarType::kSemiContinuous ||
          integrality[usr_ix] == HighsVarType::kSemiInteger)
        legalLowerUpperBound = true;
    }
    if (!legalLowerUpperBound) {
      // Leave inconsistent bounds to be used to deduce infeasibility
      highsLogUser(options.log_options, HighsLogType::kWarning,
                   "%3s  %12" HIGHSINT_FORMAT
                   " has inconsistent bounds [%12g, %12g]\n",
                   type, ml_ix, lower[usr_ix], upper[usr_ix]);
      warning_found = true;
    }
    // Check that the lower bound is not as much as +Infinity
    bool legalLowerBound = lower[usr_ix] < infinite_bound;
    if (!legalLowerBound) {
      highsLogUser(options.log_options, HighsLogType::kError,
                   "%3s  %12" HIGHSINT_FORMAT
                   " has lower bound of %12g >= %12g\n",
                   type, ml_ix, lower[usr_ix], infinite_bound);
      error_found = true;
    }
    // Check that the upper bound is not as little as -Infinity
    bool legalUpperBound = upper[usr_ix] > -infinite_bound;
    if (!legalUpperBound) {
      highsLogUser(options.log_options, HighsLogType::kError,
                   "%3s  %12" HIGHSINT_FORMAT
                   " has upper bound of %12g <= %12g\n",
                   type, ml_ix, upper[usr_ix], -infinite_bound);
      error_found = true;
    }
  }
  if (num_infinite_lower_bound) {
    highsLogUser(options.log_options, HighsLogType::kInfo,
                 "%3ss:%12" HIGHSINT_FORMAT
                 " lower bounds    less than or equal to %12g are treated as "
                 "-Infinity\n",
                 type, num_infinite_lower_bound, -infinite_bound);
  }
  if (num_infinite_upper_bound) {
    highsLogUser(options.log_options, HighsLogType::kInfo,
                 "%3ss:%12" HIGHSINT_FORMAT
                 " upper bounds greater than or equal to %12g are treated as "
                 "+Infinity\n",
                 type, num_infinite_upper_bound, infinite_bound);
  }

  if (error_found)
    return_status = HighsStatus::kError;
  else if (warning_found)
    return_status = HighsStatus::kWarning;
  else
    return_status = HighsStatus::kOk;

  return return_status;
}

HighsStatus assessSemiVariables(HighsLp& lp, const HighsOptions& options,
                                bool& made_semi_variable_mods) {
  made_semi_variable_mods = false;
  HighsStatus return_status = HighsStatus::kOk;
  if (!lp.integrality_.size()) return return_status;
  assert((HighsInt)lp.integrality_.size() == lp.num_col_);
  HighsInt num_illegal_lower = 0;
  HighsInt num_illegal_upper = 0;
  HighsInt num_modified_upper = 0;
  HighsInt num_inconsistent_semi = 0;
  HighsInt num_non_semi = 0;
  HighsInt num_non_continuous_variables = 0;
  const double kLowerBoundMu = 10.0;
  std::vector<HighsInt>& inconsistent_semi_variable_index =
      lp.mods_.save_inconsistent_semi_variable_index;
  std::vector<double>& inconsistent_semi_variable_lower_bound_value =
      lp.mods_.save_inconsistent_semi_variable_lower_bound_value;
  std::vector<double>& inconsistent_semi_variable_upper_bound_value =
      lp.mods_.save_inconsistent_semi_variable_upper_bound_value;
  std::vector<HighsVarType>& inconsistent_semi_variable_type =
      lp.mods_.save_inconsistent_semi_variable_type;

  std::vector<HighsInt>& tightened_semi_variable_upper_bound_index =
      lp.mods_.save_tightened_semi_variable_upper_bound_index;
  std::vector<double>& tightened_semi_variable_upper_bound_value =
      lp.mods_.save_tightened_semi_variable_upper_bound_value;

  assert(int(lp.mods_.save_inconsistent_semi_variable_index.size()) == 0);
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    if (lp.integrality_[iCol] == HighsVarType::kSemiContinuous ||
        lp.integrality_[iCol] == HighsVarType::kSemiInteger) {
      if (lp.col_lower_[iCol] > lp.col_upper_[iCol]) {
        // Semi-variables with inconsistent bounds become continuous
        // and fixed at zero
        num_inconsistent_semi++;
        inconsistent_semi_variable_index.push_back(iCol);
        inconsistent_semi_variable_lower_bound_value.push_back(
            lp.col_lower_[iCol]);
        inconsistent_semi_variable_upper_bound_value.push_back(
            lp.col_upper_[iCol]);
        inconsistent_semi_variable_type.push_back(lp.integrality_[iCol]);
        continue;
      }
      // Semi-variables with zero lower bound are not semi
      if (lp.col_lower_[iCol] == 0) {
        num_non_semi++;
        lp.mods_.save_non_semi_variable_index.push_back(iCol);
        // Semi-integer become integer so still have a non-continuous variable
        if (lp.integrality_[iCol] == HighsVarType::kSemiInteger)
          num_non_continuous_variables++;
        continue;
      }
      if (lp.col_lower_[iCol] < 0) {
        // Semi-variables must have a positive lower bound
        num_illegal_lower++;
      } else if (lp.col_upper_[iCol] > kMaxSemiVariableUpper) {
        // Semi-variables must have upper bound that's not too large,
        // so see whether the limiting value is sufficiently larger than the
        // lower bound
        if (kLowerBoundMu * lp.col_lower_[iCol] > kMaxSemiVariableUpper) {
          num_illegal_upper++;
        } else {
          // Record the upper bound change to be made later
          tightened_semi_variable_upper_bound_index.push_back(iCol);
          tightened_semi_variable_upper_bound_value.push_back(
              kMaxSemiVariableUpper);
          num_modified_upper++;
        }
      }
      num_non_continuous_variables++;
    } else if (lp.integrality_[iCol] == HighsVarType::kInteger) {
      num_non_continuous_variables++;
    }
  }
  if (num_inconsistent_semi) {
    highsLogUser(
        options.log_options, HighsLogType::kWarning,
        "%" HIGHSINT_FORMAT
        " semi-continuous/integer variable(s) have inconsistent bounds "
        "so are fixed at zero\n",
        num_inconsistent_semi);
    return_status = HighsStatus::kWarning;
  }
  if (num_non_semi) {
    highsLogUser(options.log_options, HighsLogType::kWarning,
                 "%" HIGHSINT_FORMAT
                 " semi-continuous/integer variable(s) have zero lower bound "
                 "so are continuous/integer\n",
                 num_non_semi);
    return_status = HighsStatus::kWarning;
  }
  if (!num_non_continuous_variables) {
    highsLogUser(options.log_options, HighsLogType::kWarning,
                 "No semi-integer/integer variables in model with non-empty "
                 "integrality\n");
    return_status = HighsStatus::kWarning;
  }
  const bool has_illegal_bounds = num_illegal_lower || num_illegal_upper;
  if (num_modified_upper) {
    highsLogUser(options.log_options, HighsLogType::kWarning,
                 "%" HIGHSINT_FORMAT
                 " semi-continuous/integer variable(s) have upper bounds "
                 "exceeding %g that can be modified to %g > %g*lower)\n",
                 num_modified_upper, kMaxSemiVariableUpper,
                 kMaxSemiVariableUpper, kLowerBoundMu);
    return_status = HighsStatus::kWarning;
    if (has_illegal_bounds) {
      // Don't apply upper bound tightenings if there are illegal bounds
      assert(num_illegal_lower || num_illegal_upper);
      tightened_semi_variable_upper_bound_index.clear();
      tightened_semi_variable_upper_bound_value.clear();
    } else {
      // Apply the upper bound tightenings, saving the over-written
      // values
      for (HighsInt k = 0; k < num_modified_upper; k++) {
        const double use_upper_bound =
            tightened_semi_variable_upper_bound_value[k];
        const HighsInt iCol = tightened_semi_variable_upper_bound_index[k];
        tightened_semi_variable_upper_bound_value[k] = lp.col_upper_[iCol];
        lp.col_upper_[iCol] = use_upper_bound;
      }
    }
  }
  if (num_inconsistent_semi) {
    if (has_illegal_bounds) {
      // Don't apply bound changes if there are illegal bounds
      inconsistent_semi_variable_index.clear();
      inconsistent_semi_variable_lower_bound_value.clear();
      inconsistent_semi_variable_upper_bound_value.clear();
      inconsistent_semi_variable_type.clear();
    } else {
      for (HighsInt k = 0; k < num_inconsistent_semi; k++) {
        const HighsInt iCol = inconsistent_semi_variable_index[k];
        lp.col_lower_[iCol] = 0;
        lp.col_upper_[iCol] = 0;
        lp.integrality_[iCol] = HighsVarType::kContinuous;
      }
    }
  }
  if (num_non_semi) {
    if (has_illegal_bounds) {
      // Don't apply type changes if there are illegal bounds
      lp.mods_.save_non_semi_variable_index.clear();
    } else {
      for (HighsInt k = 0; k < num_non_semi; k++) {
        const HighsInt iCol = lp.mods_.save_non_semi_variable_index[k];
        if (lp.integrality_[iCol] == HighsVarType::kSemiContinuous) {
          // Semi-continuous become continuous
          lp.integrality_[iCol] = HighsVarType::kContinuous;
        } else {
          // Semi-integer become integer
          lp.integrality_[iCol] = HighsVarType::kInteger;
        }
      }
    }
  }
  if (num_illegal_lower) {
    highsLogUser(
        options.log_options, HighsLogType::kError,
        "%" HIGHSINT_FORMAT
        " semi-continuous/integer variable(s) have negative lower bounds\n",
        num_illegal_lower);
    return_status = HighsStatus::kError;
  }
  if (num_illegal_upper) {
    highsLogUser(
        options.log_options, HighsLogType::kError,
        "%" HIGHSINT_FORMAT
        " semi-continuous/integer variables have upper bounds "
        "exceeding %g that cannot be modified due to large lower bounds\n",
        num_illegal_upper, kMaxSemiVariableUpper);
    return_status = HighsStatus::kError;
  }
  made_semi_variable_mods =
      lp.mods_.save_non_semi_variable_index.size() > 0 ||
      inconsistent_semi_variable_index.size() > 0 ||
      tightened_semi_variable_upper_bound_index.size() > 0;
  return return_status;
}

void relaxSemiVariables(HighsLp& lp, bool& made_semi_variable_mods) {
  // When solving relaxation, semi-variables are continuous between 0
  // and their upper bound, so have to modify the lower bound to be
  // zero
  made_semi_variable_mods = false;
  if (!lp.integrality_.size()) return;
  assert((HighsInt)lp.integrality_.size() == lp.num_col_);
  std::vector<HighsInt>& relaxed_semi_variable_lower_index =
      lp.mods_.save_relaxed_semi_variable_lower_bound_index;
  std::vector<double>& relaxed_semi_variable_lower_value =
      lp.mods_.save_relaxed_semi_variable_lower_bound_value;
  assert(relaxed_semi_variable_lower_index.size() == 0);
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    if (lp.integrality_[iCol] == HighsVarType::kSemiContinuous ||
        lp.integrality_[iCol] == HighsVarType::kSemiInteger) {
      relaxed_semi_variable_lower_index.push_back(iCol);
      relaxed_semi_variable_lower_value.push_back(lp.col_lower_[iCol]);
      lp.col_lower_[iCol] = 0;
    }
  }
  made_semi_variable_mods = relaxed_semi_variable_lower_index.size() > 0;
}

bool activeModifiedUpperBounds(const HighsOptions& options, const HighsLp& lp,
                               const std::vector<double> col_value) {
  const std::vector<HighsInt>& tightened_semi_variable_upper_bound_index =
      lp.mods_.save_tightened_semi_variable_upper_bound_index;
  const HighsInt num_modified_upper =
      tightened_semi_variable_upper_bound_index.size();
  HighsInt num_active_modified_upper = 0;
  double min_semi_variable_margin = kHighsInf;
  for (HighsInt k = 0; k < num_modified_upper; k++) {
    const double value =
        col_value[tightened_semi_variable_upper_bound_index[k]];
    const double upper =
        lp.col_upper_[tightened_semi_variable_upper_bound_index[k]];
    double semi_variable_margin = upper - value;
    if (value > upper - options.primal_feasibility_tolerance) {
      min_semi_variable_margin = 0;
      num_active_modified_upper++;
    } else {
      min_semi_variable_margin =
          std::min(semi_variable_margin, min_semi_variable_margin);
    }
  }
  if (num_active_modified_upper) {
    highsLogUser(options.log_options, HighsLogType::kError,
                 "%" HIGHSINT_FORMAT
                 " semi-variables are active at modified upper bounds\n",
                 num_active_modified_upper);
  } else if (num_modified_upper) {
    highsLogUser(options.log_options, HighsLogType::kWarning,
                 "No semi-variables are active at modified upper bounds:"
                 " a large minimum margin (%g) suggests optimality,"
                 " but there is no guarantee\n",
                 min_semi_variable_margin);
  }
  return (num_active_modified_upper != 0);
}

HighsStatus cleanBounds(const HighsOptions& options, HighsLp& lp) {
  double max_residual = 0;
  HighsInt num_change = 0;
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    double residual = lp.col_lower_[iCol] - lp.col_upper_[iCol];
    if (residual > options.primal_feasibility_tolerance) {
      highsLogUser(options.log_options, HighsLogType::kError,
                   "Column %" HIGHSINT_FORMAT
                   " has inconsistent bounds [%g, %g] (residual = "
                   "%g) after presolve\n",
                   iCol, lp.col_lower_[iCol], lp.col_upper_[iCol], residual);
      return HighsStatus::kError;
    } else if (residual > 0) {
      num_change++;
      max_residual = std::max(residual, max_residual);
      double mid = 0.5 * (lp.col_lower_[iCol] + lp.col_upper_[iCol]);
      lp.col_lower_[iCol] = mid;
      lp.col_upper_[iCol] = mid;
    }
  }
  for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
    double residual = lp.row_lower_[iRow] - lp.row_upper_[iRow];
    if (residual > options.primal_feasibility_tolerance) {
      highsLogUser(options.log_options, HighsLogType::kError,
                   "Row %" HIGHSINT_FORMAT
                   " has inconsistent bounds [%g, %g] (residual = %g) "
                   "after presolve\n",
                   iRow, lp.row_lower_[iRow], lp.row_upper_[iRow], residual);
      return HighsStatus::kError;
    } else if (residual > 0) {
      num_change++;
      max_residual = std::max(residual, max_residual);
      double mid = 0.5 * (lp.row_lower_[iRow] + lp.row_upper_[iRow]);
      lp.row_lower_[iRow] = mid;
      lp.row_upper_[iRow] = mid;
    }
  }
  if (num_change) {
    highsLogUser(options.log_options, HighsLogType::kWarning,
                 "Resolved %" HIGHSINT_FORMAT
                 " inconsistent bounds (maximum residual = "
                 "%9.4g) after presolve\n",
                 num_change, max_residual);
    return HighsStatus::kWarning;
  }
  return HighsStatus::kOk;
}

bool boundScaleOk(const vector<double>& lower, const vector<double>& upper,
                  const HighsInt bound_scale, const double infinite_bound) {
  if (!bound_scale) return true;
  double bound_scale_value = std::pow(2, bound_scale);
  for (HighsInt iCol = 0; iCol < HighsInt(lower.size()); iCol++) {
    if (lower[iCol] > -kHighsInf &&
        std::abs(lower[iCol] * bound_scale_value) > infinite_bound)
      return false;
    if (upper[iCol] < kHighsInf &&
        std::abs(upper[iCol] * bound_scale_value) > infinite_bound)
      return false;
  }
  return true;
}

bool costScaleOk(const vector<double>& cost, const HighsInt cost_scale,
                 const double infinite_cost) {
  if (!cost_scale) return true;
  double cost_scale_value = std::pow(2, cost_scale);
  for (HighsInt iCol = 0; iCol < HighsInt(cost.size()); iCol++)
    if (std::abs(cost[iCol]) < kHighsInf &&
        std::abs(cost[iCol] * cost_scale_value) > infinite_cost)
      return false;
  return true;
}

bool considerScaling(const HighsOptions& options, HighsLp& lp) {
  // Indicate whether new scaling has been determined in the return value.
  bool new_scaling = false;
  // Consider scaling the LP - either by finding new factors or by
  // applying any existing factors
  const bool allow_scaling =
      lp.num_col_ > 0 &&
      options.simplex_scale_strategy != kSimplexScaleStrategyOff;
  if (lp.scale_.has_scaling && !allow_scaling) {
    // LP had scaling before, but now it is not permitted, clear any
    // scaling. Return true as scaling position has changed
    lp.clearScale();
    return true;
  }
  const bool scaling_not_tried = lp.scale_.strategy == kSimplexScaleStrategyOff;
  const bool new_scaling_strategy =
      options.simplex_scale_strategy != lp.scale_.strategy &&
      options.simplex_scale_strategy != kSimplexScaleStrategyChoose;
  const bool try_scaling =
      allow_scaling && (scaling_not_tried || new_scaling_strategy);
  if (try_scaling) {
    // Scaling will be tried, so ensure that any previous scaling is not applied
    lp.unapplyScale();
    const bool analyse_lp_data =
        kHighsAnalysisLevelModelData & options.highs_analysis_level;
    if (analyse_lp_data) analyseLp(options.log_options, lp);
    scaleLp(options, lp);
    // If the LP is now scaled, then the scaling is new
    new_scaling = lp.is_scaled_;
    if (analyse_lp_data && lp.is_scaled_) analyseLp(options.log_options, lp);
  } else if (lp.scale_.has_scaling) {
    // Scaling factors are known, so ensure that they are applied
    lp.applyScale();
  }
  // Ensure that either the LP has scale factors and is scaled, or
  // it doesn't have scale factors and isn't scaled
  assert(lp.scale_.has_scaling == lp.is_scaled_);
  return new_scaling;
}

void scaleLp(const HighsOptions& options, HighsLp& lp,
             const bool force_scaling) {
  lp.clearScaling();
  HighsInt numCol = lp.num_col_;
  HighsInt numRow = lp.num_row_;
  // Scaling not well defined for models with no columns
  assert(numCol > 0);
  vector<double>& colCost = lp.col_cost_;
  vector<double>& colLower = lp.col_lower_;
  vector<double>& colUpper = lp.col_upper_;
  vector<double>& rowLower = lp.row_lower_;
  vector<double>& rowUpper = lp.row_upper_;

  // Save the simplex_scale_strategy so that the option can be
  // modified for the course of this method
  HighsInt simplex_scale_strategy = options.simplex_scale_strategy;
  // Determine the actual strategy to use
  HighsInt use_scale_strategy = simplex_scale_strategy;
  if (use_scale_strategy == kSimplexScaleStrategyChoose) {
    // HiGHS is left to choose: currently use forced equilibration, but maybe do
    // something more intelligent
    use_scale_strategy = kSimplexScaleStrategyForcedEquilibration;
  }
  // Find out range of matrix values and skip matrix scaling if all
  // |values| are in [0.2, 5]
  const double no_scaling_original_matrix_min_value = 0.2;
  const double no_scaling_original_matrix_max_value = 5.0;
  double original_matrix_min_value = kHighsInf;
  double original_matrix_max_value = 0;
  lp.a_matrix_.range(original_matrix_min_value, original_matrix_max_value);
  // Possibly force scaling, otherwise base the decision on the range
  // of values in the matrix, values that will be used later for
  // reporting
  const bool no_scaling = force_scaling
                              ? false
                              : (original_matrix_min_value >=
                                 no_scaling_original_matrix_min_value) &&
                                    (original_matrix_max_value <=
                                     no_scaling_original_matrix_max_value);
  bool scaled_matrix = false;
  if (no_scaling) {
    // No matrix scaling, but possible cost scaling
    if (options.highs_analysis_level)
      highsLogDev(options.log_options, HighsLogType::kInfo,
                  "Scaling: Matrix has [min, max] values of [%g, %g] within "
                  "[%g, %g] so no scaling performed\n",
                  original_matrix_min_value, original_matrix_max_value,
                  no_scaling_original_matrix_min_value,
                  no_scaling_original_matrix_max_value);
  } else {
    // Try scaling, so assign unit factors - partly because initial
    // factors may be assumed by the scaling method, but also because
    // scaling factors may not be computed for empty rows/columns
    HighsScale& scale = lp.scale_;
    scale.col.assign(numCol, 1);
    scale.row.assign(numRow, 1);
    const bool equilibration_scaling =
        use_scale_strategy == kSimplexScaleStrategyEquilibration ||
        use_scale_strategy == kSimplexScaleStrategyForcedEquilibration;
    // Try scaling. Value of scaled_matrix indicates whether scaling
    // was considered valuable (and performed). If it's not valuable
    // then the matrix remains unscaled
    if (equilibration_scaling) {
      scaled_matrix = equilibrationScaleMatrix(options, lp, use_scale_strategy);
    } else {
      scaled_matrix = maxValueScaleMatrix(options, lp, use_scale_strategy);
    }
    if (scaled_matrix) {
      // Matrix is scaled, so scale the bounds and costs
      for (HighsInt iCol = 0; iCol < numCol; iCol++) {
        colLower[iCol] /= scale.col[iCol];
        colUpper[iCol] /= scale.col[iCol];
        colCost[iCol] *= scale.col[iCol];
      }
      for (HighsInt iRow = 0; iRow < numRow; iRow++) {
        rowLower[iRow] *= scale.row[iRow];
        rowUpper[iRow] *= scale.row[iRow];
      }
      scale.has_scaling = true;
      scale.num_col = numCol;
      scale.num_row = numRow;
      scale.cost = 1.0;
      lp.is_scaled_ = true;
    } else {
      // Matrix is not scaled, so clear the scaling
      lp.clearScaling();
    }
  }
  // Record the scaling strategy used
  lp.scale_.strategy = use_scale_strategy;
  // Possibly scale the costs
  //  if (allow_cost_scaling) scaleSimplexCost(options, lp, scale.cost);

  // If matrix is unscaled, then LP is only scaled if there is a cost scaling
  // factor
  //  if (!scaled_matrix) lp.is_scaled_ = scale.cost != 1;
}

bool equilibrationScaleMatrix(const HighsOptions& options, HighsLp& lp,
                              const HighsInt use_scale_strategy) {
  HighsInt numCol = lp.num_col_;
  HighsInt numRow = lp.num_row_;
  HighsScale& scale = lp.scale_;
  vector<double>& colScale = scale.col;
  vector<double>& rowScale = scale.row;
  vector<HighsInt>& Astart = lp.a_matrix_.start_;
  vector<HighsInt>& Aindex = lp.a_matrix_.index_;
  vector<double>& Avalue = lp.a_matrix_.value_;
  vector<double>& colCost = lp.col_cost_;

  HighsInt simplex_scale_strategy = use_scale_strategy;

  double original_matrix_min_value = kHighsInf;
  double original_matrix_max_value = 0;
  for (HighsInt k = 0, AnX = Astart[numCol]; k < AnX; k++) {
    double value = fabs(Avalue[k]);
    original_matrix_min_value = min(original_matrix_min_value, value);
    original_matrix_max_value = max(original_matrix_max_value, value);
  }

  // Include cost in scaling if minimum nonzero cost is less than 0.1
  double min_nonzero_cost = kHighsInf;
  for (HighsInt i = 0; i < numCol; i++) {
    if (colCost[i]) min_nonzero_cost = min(fabs(colCost[i]), min_nonzero_cost);
  }
  bool include_cost_in_scaling = false;
  include_cost_in_scaling = min_nonzero_cost < 0.1;

  // Limits on scaling factors
  double max_allow_scale;
  double min_allow_scale;
  // Now that kHighsInf =
  // std::numeric_limits<double>::infinity(), this Qi-trick doesn't
  // work so, in recognition, use the old value of kHighsInf
  const double finite_infinity = 1e200;
  max_allow_scale = pow(2.0, options.allowed_matrix_scale_factor);
  min_allow_scale = 1 / max_allow_scale;

  double min_allow_col_scale = min_allow_scale;
  double max_allow_col_scale = max_allow_scale;
  double min_allow_row_scale = min_allow_scale;
  double max_allow_row_scale = max_allow_scale;

  // Search up to 6 times
  vector<double> row_min_value(numRow, finite_infinity);
  vector<double> row_max_value(numRow, 1 / finite_infinity);
  for (HighsInt search_count = 0; search_count < 6; search_count++) {
    // Find column scale, prepare row data
    for (HighsInt iCol = 0; iCol < numCol; iCol++) {
      // For column scale (find)
      double col_min_value = finite_infinity;
      double col_max_value = 1 / finite_infinity;
      double abs_col_cost = fabs(colCost[iCol]);
      if (include_cost_in_scaling && abs_col_cost != 0) {
        col_min_value = min(col_min_value, abs_col_cost);
        col_max_value = max(col_max_value, abs_col_cost);
      }
      for (HighsInt k = Astart[iCol]; k < Astart[iCol + 1]; k++) {
        double value = fabs(Avalue[k]) * rowScale[Aindex[k]];
        col_min_value = min(col_min_value, value);
        col_max_value = max(col_max_value, value);
      }
      double col_equilibration = 1 / sqrt(col_min_value * col_max_value);
      // Ensure that column scale factor is not excessively large or small
      colScale[iCol] =
          min(max(min_allow_col_scale, col_equilibration), max_allow_col_scale);
      // For row scale (only collect)
      for (HighsInt k = Astart[iCol]; k < Astart[iCol + 1]; k++) {
        HighsInt iRow = Aindex[k];
        double value = fabs(Avalue[k]) * colScale[iCol];
        row_min_value[iRow] = min(row_min_value[iRow], value);
        row_max_value[iRow] = max(row_max_value[iRow], value);
      }
    }
    // For row scale (find)
    for (HighsInt iRow = 0; iRow < numRow; iRow++) {
      double row_equilibration =
          1 / sqrt(row_min_value[iRow] * row_max_value[iRow]);
      // Ensure that row scale factor is not excessively large or small
      rowScale[iRow] =
          min(max(min_allow_row_scale, row_equilibration), max_allow_row_scale);
    }
    row_min_value.assign(numRow, finite_infinity);
    row_max_value.assign(numRow, 1 / finite_infinity);
  }
  // Make it numerically better
  // Also determine the max and min row and column scaling factors
  double min_col_scale = finite_infinity;
  double max_col_scale = 1 / finite_infinity;
  double min_row_scale = finite_infinity;
  double max_row_scale = 1 / finite_infinity;
  const double log2 = log(2.0);
  for (HighsInt iCol = 0; iCol < numCol; iCol++) {
    colScale[iCol] = pow(2.0, floor(log(colScale[iCol]) / log2 + 0.5));
    min_col_scale = min(colScale[iCol], min_col_scale);
    max_col_scale = max(colScale[iCol], max_col_scale);
  }
  for (HighsInt iRow = 0; iRow < numRow; iRow++) {
    rowScale[iRow] = pow(2.0, floor(log(rowScale[iRow]) / log2 + 0.5));
    min_row_scale = min(rowScale[iRow], min_row_scale);
    max_row_scale = max(rowScale[iRow], max_row_scale);
  }
  // Apply scaling to matrix and bounds
  double matrix_min_value = finite_infinity;
  double matrix_max_value = 0;
  double min_original_col_equilibration = finite_infinity;
  double sum_original_log_col_equilibration = 0;
  double max_original_col_equilibration = 0;
  double min_original_row_equilibration = finite_infinity;
  double sum_original_log_row_equilibration = 0;
  double max_original_row_equilibration = 0;
  double min_col_equilibration = finite_infinity;
  double sum_log_col_equilibration = 0;
  double max_col_equilibration = 0;
  double min_row_equilibration = finite_infinity;
  double sum_log_row_equilibration = 0;
  double max_row_equilibration = 0;
  vector<double> original_row_min_value(numRow, finite_infinity);
  vector<double> original_row_max_value(numRow, 1 / finite_infinity);
  row_min_value.assign(numRow, finite_infinity);
  row_max_value.assign(numRow, 1 / finite_infinity);
  for (HighsInt iCol = 0; iCol < numCol; iCol++) {
    double original_col_min_value = finite_infinity;
    double original_col_max_value = 1 / finite_infinity;
    double col_min_value = finite_infinity;
    double col_max_value = 1 / finite_infinity;
    for (HighsInt k = Astart[iCol]; k < Astart[iCol + 1]; k++) {
      HighsInt iRow = Aindex[k];
      const double original_value = fabs(Avalue[k]);
      original_col_min_value = min(original_value, original_col_min_value);
      original_col_max_value = max(original_value, original_col_max_value);
      original_row_min_value[iRow] =
          min(original_row_min_value[iRow], original_value);
      original_row_max_value[iRow] =
          max(original_row_max_value[iRow], original_value);
      Avalue[k] *= (colScale[iCol] * rowScale[iRow]);
      const double value = fabs(Avalue[k]);
      col_min_value = min(value, col_min_value);
      col_max_value = max(value, col_max_value);
      row_min_value[iRow] = min(row_min_value[iRow], value);
      row_max_value[iRow] = max(row_max_value[iRow], value);
    }
    matrix_min_value = min(matrix_min_value, col_min_value);
    matrix_max_value = max(matrix_max_value, col_max_value);

    const double original_col_equilibration =
        1 / sqrt(original_col_min_value * original_col_max_value);
    min_original_col_equilibration =
        min(original_col_equilibration, min_original_col_equilibration);
    sum_original_log_col_equilibration += log(original_col_equilibration);
    max_original_col_equilibration =
        max(original_col_equilibration, max_original_col_equilibration);
    const double col_equilibration = 1 / sqrt(col_min_value * col_max_value);
    min_col_equilibration = min(col_equilibration, min_col_equilibration);
    sum_log_col_equilibration += log(col_equilibration);
    max_col_equilibration = max(col_equilibration, max_col_equilibration);
  }

  for (HighsInt iRow = 0; iRow < numRow; iRow++) {
    const double original_row_equilibration =
        1 / sqrt(original_row_min_value[iRow] * original_row_max_value[iRow]);
    min_original_row_equilibration =
        min(original_row_equilibration, min_original_row_equilibration);
    sum_original_log_row_equilibration += log(original_row_equilibration);
    max_original_row_equilibration =
        max(original_row_equilibration, max_original_row_equilibration);
    const double row_equilibration =
        1 / sqrt(row_min_value[iRow] * row_max_value[iRow]);
    min_row_equilibration = min(row_equilibration, min_row_equilibration);
    sum_log_row_equilibration += log(row_equilibration);
    max_row_equilibration = max(row_equilibration, max_row_equilibration);
  }
  const double geomean_original_col_equilibration =
      exp(sum_original_log_col_equilibration / numCol);
  const double geomean_original_row_equilibration =
      exp(sum_original_log_row_equilibration / numRow);
  const double geomean_col_equilibration =
      exp(sum_log_col_equilibration / numCol);
  const double geomean_row_equilibration =
      exp(sum_log_row_equilibration / numRow);
  if (options.log_dev_level) {
    highsLogDev(
        options.log_options, HighsLogType::kInfo,
        "Scaling: Original equilibration: min/mean/max %11.4g/%11.4g/%11.4g "
        "(cols); min/mean/max %11.4g/%11.4g/%11.4g (rows)\n",
        min_original_col_equilibration, geomean_original_col_equilibration,
        max_original_col_equilibration, min_original_row_equilibration,
        geomean_original_row_equilibration, max_original_row_equilibration);
    highsLogDev(
        options.log_options, HighsLogType::kInfo,
        "Scaling: Final    equilibration: min/mean/max %11.4g/%11.4g/%11.4g "
        "(cols); min/mean/max %11.4g/%11.4g/%11.4g (rows)\n",
        min_col_equilibration, geomean_col_equilibration, max_col_equilibration,
        min_row_equilibration, geomean_row_equilibration,
        max_row_equilibration);
  }

  // Compute the mean equilibration improvement
  const double geomean_original_col =
      max(geomean_original_col_equilibration,
          1 / geomean_original_col_equilibration);
  const double geomean_original_row =
      max(geomean_original_row_equilibration,
          1 / geomean_original_row_equilibration);
  const double geomean_col =
      max(geomean_col_equilibration, 1 / geomean_col_equilibration);
  const double geomean_row =
      max(geomean_row_equilibration, 1 / geomean_row_equilibration);
  const double mean_equilibration_improvement =
      sqrt((geomean_original_col * geomean_original_row) /
           (geomean_col * geomean_row));
  // Compute the extreme equilibration improvement
  const double original_col_ratio =
      max_original_col_equilibration / min_original_col_equilibration;
  const double original_row_ratio =
      max_original_row_equilibration / min_original_row_equilibration;
  const double col_ratio = max_col_equilibration / min_col_equilibration;
  const double row_ratio = max_row_equilibration / min_row_equilibration;
  const double extreme_equilibration_improvement =
      (original_col_ratio + original_row_ratio) / (col_ratio + row_ratio);
  // Compute the max/min matrix value improvement
  const double matrix_value_ratio = matrix_max_value / matrix_min_value;
  const double original_matrix_value_ratio =
      original_matrix_max_value / original_matrix_min_value;
  const double matrix_value_ratio_improvement =
      original_matrix_value_ratio / matrix_value_ratio;
  if (options.log_dev_level) {
    highsLogDev(
        options.log_options, HighsLogType::kInfo,
        "Scaling: Extreme equilibration improvement =      ( %11.4g + "
        "%11.4g) / ( %11.4g + %11.4g)  =      %11.4g / %11.4g  = %11.4g\n",
        original_col_ratio, original_row_ratio, col_ratio, row_ratio,
        (original_col_ratio + original_row_ratio), (col_ratio + row_ratio),
        extreme_equilibration_improvement);
    highsLogDev(
        options.log_options, HighsLogType::kInfo,
        "Scaling: Mean    equilibration improvement = sqrt(( %11.4g * "
        "%11.4g) / ( %11.4g * %11.4g)) = sqrt(%11.4g / %11.4g) = %11.4g\n",
        geomean_original_col, geomean_original_row, geomean_col, geomean_row,
        (geomean_original_col * geomean_original_row),
        (geomean_col * geomean_row), mean_equilibration_improvement);
    highsLogDev(
        options.log_options, HighsLogType::kInfo,
        "Scaling: Yields [min, max, ratio] matrix values of [%0.4g, %0.4g, "
        "%0.4g]; Originally [%0.4g, %0.4g, %0.4g]: Improvement of %0.4g\n",
        matrix_min_value, matrix_max_value, matrix_value_ratio,
        original_matrix_min_value, original_matrix_max_value,
        original_matrix_value_ratio, matrix_value_ratio_improvement);
    highsLogDev(options.log_options, HighsLogType::kInfo,
                "Scaling: Improves    mean equilibration by a factor %0.4g\n",
                mean_equilibration_improvement);
    highsLogDev(options.log_options, HighsLogType::kInfo,
                "Scaling: Improves extreme equilibration by a factor %0.4g\n",
                extreme_equilibration_improvement);
    highsLogDev(options.log_options, HighsLogType::kInfo,
                "Scaling: Improves max/min matrix values by a factor %0.4g\n",
                matrix_value_ratio_improvement);
  }
  const bool possibly_abandon_scaling =
      simplex_scale_strategy != kSimplexScaleStrategyForcedEquilibration;
  const double improvement_factor = extreme_equilibration_improvement *
                                    mean_equilibration_improvement *
                                    matrix_value_ratio_improvement;

  const double improvement_factor_required = 1.0;
  const bool poor_improvement =
      improvement_factor < improvement_factor_required;

  // Possibly abandon scaling if it's not improved equilibration significantly
  if (possibly_abandon_scaling && poor_improvement) {
    // Unscale the matrix
    for (HighsInt iCol = 0; iCol < numCol; iCol++) {
      for (HighsInt k = Astart[iCol]; k < Astart[iCol + 1]; k++) {
        HighsInt iRow = Aindex[k];
        Avalue[k] /= (colScale[iCol] * rowScale[iRow]);
      }
    }
    if (options.log_dev_level)
      highsLogDev(options.log_options, HighsLogType::kInfo,
                  "Scaling: Improvement factor %0.4g < %0.4g required, so no "
                  "scaling applied\n",
                  improvement_factor, improvement_factor_required);
    return false;
  } else {
    if (options.log_dev_level) {
      highsLogDev(options.log_options, HighsLogType::kInfo,
                  "Scaling: Factors are in [%0.4g, %0.4g] for columns and in "
                  "[%0.4g, %0.4g] for rows\n",
                  min_col_scale, max_col_scale, min_row_scale, max_row_scale);
      highsLogDev(options.log_options, HighsLogType::kInfo,
                  "Scaling: Improvement factor is %0.4g >= %0.4g so scale LP\n",
                  improvement_factor, improvement_factor_required);
      if (extreme_equilibration_improvement < 1.0) {
        highsLogDev(
            options.log_options, HighsLogType::kWarning,
            "Scaling: Applying scaling with extreme improvement of %0.4g\n",
            extreme_equilibration_improvement);
      }
      if (mean_equilibration_improvement < 1.0) {
        highsLogDev(
            options.log_options, HighsLogType::kWarning,
            "Scaling: Applying scaling with mean improvement of %0.4g\n",
            mean_equilibration_improvement);
      }
      if (matrix_value_ratio_improvement < 1.0) {
        highsLogDev(options.log_options, HighsLogType::kWarning,
                    "Scaling: Applying scaling with matrix value ratio "
                    "improvement of %0.4g\n",
                    matrix_value_ratio_improvement);
      }
      if (improvement_factor < 10 * improvement_factor_required) {
        highsLogDev(options.log_options, HighsLogType::kWarning,
                    "Scaling: Applying scaling with improvement factor %0.4g "
                    "< 10*(%0.4g) improvement\n",
                    improvement_factor, improvement_factor_required);
      }
    }
  }
  return true;
}

bool maxValueScaleMatrix(const HighsOptions& options, HighsLp& lp,
                         const HighsInt use_scale_strategy) {
  HighsInt numCol = lp.num_col_;
  HighsInt numRow = lp.num_row_;
  HighsScale& scale = lp.scale_;
  vector<double>& colScale = scale.col;
  vector<double>& rowScale = scale.row;
  vector<HighsInt>& Astart = lp.a_matrix_.start_;
  vector<HighsInt>& Aindex = lp.a_matrix_.index_;
  vector<double>& Avalue = lp.a_matrix_.value_;

  assert(options.simplex_scale_strategy == kSimplexScaleStrategyMaxValue015 ||
         options.simplex_scale_strategy == kSimplexScaleStrategyMaxValue0157);

  // The 015(7) values refer to bit settings in FICO's scaling options.
  // Specifically
  //
  // 0: Row scaling
  //
  // 1: Column scaling
  //
  // 5: Scale by maximum element
  //
  // 7: Scale objective function for the simplex method
  //
  // Note that 7 is not yet implemented, so
  // kSimplexScaleStrategyMaxValue015 and
  // kSimplexScaleStrategyMaxValue0157 are equivalent. However, cost
  // scaling could be well worth adding, now that the unscaled problem
  // can be solved using scaled NLA

  const double log2 = log(2.0);
  const double max_allow_scale = pow(2.0, options.allowed_matrix_scale_factor);
  const double min_allow_scale = 1 / max_allow_scale;

  const double min_allow_col_scale = min_allow_scale;
  const double max_allow_col_scale = max_allow_scale;
  const double min_allow_row_scale = min_allow_scale;
  const double max_allow_row_scale = max_allow_scale;

  double min_row_scale = kHighsInf;
  double max_row_scale = 0;
  double original_matrix_min_value = kHighsInf;
  double original_matrix_max_value = 0;
  // Determine the row scaling. Also determine the max/min row scaling
  // factors, and max/min original matrix values
  vector<double> row_max_value(numRow, 0);
  for (HighsInt iCol = 0; iCol < numCol; iCol++) {
    for (HighsInt k = Astart[iCol]; k < Astart[iCol + 1]; k++) {
      const HighsInt iRow = Aindex[k];
      const double value = fabs(Avalue[k]);
      row_max_value[iRow] = max(row_max_value[iRow], value);
      original_matrix_min_value = min(original_matrix_min_value, value);
      original_matrix_max_value = max(original_matrix_max_value, value);
    }
  }
  for (HighsInt iRow = 0; iRow < numRow; iRow++) {
    if (row_max_value[iRow]) {
      double row_scale_value = 1 / row_max_value[iRow];
      // Convert the row scale factor to the nearest power of two, and
      // ensure that it is not excessively large or small
      row_scale_value = pow(2.0, floor(log(row_scale_value) / log2 + 0.5));
      row_scale_value =
          min(max(min_allow_row_scale, row_scale_value), max_allow_row_scale);
      min_row_scale = min(row_scale_value, min_row_scale);
      max_row_scale = max(row_scale_value, max_row_scale);
      rowScale[iRow] = row_scale_value;
    }
  }
  // Determine the column scaling, whilst applying the row scaling
  // Also determine the max/min column scaling factors, and max/min
  // matrix values
  double min_col_scale = kHighsInf;
  double max_col_scale = 0;
  double matrix_min_value = kHighsInf;
  double matrix_max_value = 0;
  for (HighsInt iCol = 0; iCol < numCol; iCol++) {
    double col_max_value = 0;
    for (HighsInt k = Astart[iCol]; k < Astart[iCol + 1]; k++) {
      const HighsInt iRow = Aindex[k];
      Avalue[k] *= rowScale[iRow];
      const double value = fabs(Avalue[k]);
      col_max_value = max(col_max_value, value);
    }
    if (col_max_value) {
      double col_scale_value = 1 / col_max_value;
      // Convert the col scale factor to the nearest power of two, and
      // ensure that it is not excessively large or small
      col_scale_value = pow(2.0, floor(log(col_scale_value) / log2 + 0.5));
      col_scale_value =
          min(max(min_allow_col_scale, col_scale_value), max_allow_col_scale);
      min_col_scale = min(col_scale_value, min_col_scale);
      max_col_scale = max(col_scale_value, max_col_scale);
      colScale[iCol] = col_scale_value;
      for (HighsInt k = Astart[iCol]; k < Astart[iCol + 1]; k++) {
        Avalue[k] *= colScale[iCol];
        const double value = fabs(Avalue[k]);
        matrix_min_value = min(matrix_min_value, value);
        matrix_max_value = max(matrix_max_value, value);
      }
    }
  }
  const double matrix_value_ratio = matrix_max_value / matrix_min_value;
  const double original_matrix_value_ratio =
      original_matrix_max_value / original_matrix_min_value;
  const double matrix_value_ratio_improvement =
      original_matrix_value_ratio / matrix_value_ratio;

  const double improvement_factor = matrix_value_ratio_improvement;

  const double improvement_factor_required = 1.0;
  const bool poor_improvement =
      improvement_factor <= improvement_factor_required;

  if (poor_improvement) {
    // Unscale the matrix
    for (HighsInt iCol = 0; iCol < numCol; iCol++) {
      for (HighsInt k = Astart[iCol]; k < Astart[iCol + 1]; k++) {
        HighsInt iRow = Aindex[k];
        Avalue[k] /= (colScale[iCol] * rowScale[iRow]);
      }
    }
    if (options.log_dev_level)
      highsLogDev(options.log_options, HighsLogType::kInfo,
                  "Scaling: Improvement factor %0.4g < %0.4g required, so no "
                  "scaling applied\n",
                  improvement_factor, improvement_factor_required);
    return false;
  } else {
    if (options.log_dev_level) {
      highsLogDev(options.log_options, HighsLogType::kInfo,
                  "Scaling: Factors are in [%0.4g, %0.4g] for columns and in "
                  "[%0.4g, %0.4g] for rows\n",
                  min_col_scale, max_col_scale, min_row_scale, max_row_scale);
      highsLogDev(
          options.log_options, HighsLogType::kInfo,
          "Scaling: Yields [min, max, ratio] matrix values of [%0.4g, %0.4g, "
          "%0.4g]; Originally [%0.4g, %0.4g, %0.4g]: Improvement of %0.4g\n",
          matrix_min_value, matrix_max_value, matrix_value_ratio,
          original_matrix_min_value, original_matrix_max_value,
          original_matrix_value_ratio, matrix_value_ratio_improvement);
    }
    return true;
  }
}

HighsStatus applyScalingToLpCol(HighsLp& lp, const HighsInt col,
                                const double colScale) {
  if (col < 0) return HighsStatus::kError;
  if (col >= lp.num_col_) return HighsStatus::kError;
  if (!colScale) return HighsStatus::kError;

  lp.a_matrix_.scaleCol(col, colScale);
  lp.col_cost_[col] *= colScale;
  if (colScale > 0) {
    lp.col_lower_[col] /= colScale;
    lp.col_upper_[col] /= colScale;
  } else {
    const double new_upper = lp.col_lower_[col] / colScale;
    lp.col_lower_[col] = lp.col_upper_[col] / colScale;
    lp.col_upper_[col] = new_upper;
  }
  return HighsStatus::kOk;
}

HighsStatus applyScalingToLpRow(HighsLp& lp, const HighsInt row,
                                const double rowScale) {
  if (row < 0) return HighsStatus::kError;
  if (row >= lp.num_row_) return HighsStatus::kError;
  if (!rowScale) return HighsStatus::kError;

  lp.a_matrix_.scaleRow(row, rowScale);
  if (rowScale > 0) {
    lp.row_lower_[row] *= rowScale;
    lp.row_upper_[row] *= rowScale;
  } else {
    const double new_upper = lp.row_lower_[row] * rowScale;
    lp.row_lower_[row] = lp.row_upper_[row] * rowScale;
    lp.row_upper_[row] = new_upper;
  }
  return HighsStatus::kOk;
}

void unscaleSolution(HighsSolution& solution, const HighsScale& scale) {
  assert(solution.col_value.size() == static_cast<size_t>(scale.num_col));
  assert(solution.col_dual.size() == static_cast<size_t>(scale.num_col));
  assert(solution.row_value.size() == static_cast<size_t>(scale.num_row));
  assert(solution.row_dual.size() == static_cast<size_t>(scale.num_row));

  for (HighsInt iCol = 0; iCol < scale.num_col; iCol++) {
    solution.col_value[iCol] *= scale.col[iCol];
    solution.col_dual[iCol] /= (scale.col[iCol] / scale.cost);
  }
  for (HighsInt iRow = 0; iRow < scale.num_row; iRow++) {
    solution.row_value[iRow] /= scale.row[iRow];
    solution.row_dual[iRow] *= (scale.row[iRow] * scale.cost);
  }
}

void appendColsToLpVectors(HighsLp& lp, const HighsInt num_new_col,
                           const vector<double>& colCost,
                           const vector<double>& colLower,
                           const vector<double>& colUpper) {
  assert(num_new_col >= 0);
  if (num_new_col == 0) return;
  HighsInt new_num_col = lp.num_col_ + num_new_col;
  lp.col_cost_.resize(new_num_col);
  lp.col_lower_.resize(new_num_col);
  lp.col_upper_.resize(new_num_col);
  const bool have_integrality = (lp.integrality_.size() != 0);
  if (have_integrality) {
    assert(HighsInt(lp.integrality_.size()) == lp.num_col_);
    lp.integrality_.resize(new_num_col);
  }
  bool have_names = (lp.col_names_.size() != 0);
  if (have_names) lp.col_names_.resize(new_num_col);
  for (HighsInt new_col = 0; new_col < num_new_col; new_col++) {
    HighsInt iCol = lp.num_col_ + new_col;
    lp.col_cost_[iCol] = colCost[new_col];
    lp.col_lower_[iCol] = colLower[new_col];
    lp.col_upper_[iCol] = colUpper[new_col];
    // Cannot guarantee to create unique names, so name is blank
    if (have_names) lp.col_names_[iCol] = "";
    if (have_integrality) lp.integrality_[iCol] = HighsVarType::kContinuous;
  }
}

void appendRowsToLpVectors(HighsLp& lp, const HighsInt num_new_row,
                           const vector<double>& rowLower,
                           const vector<double>& rowUpper) {
  assert(num_new_row >= 0);
  if (num_new_row == 0) return;
  HighsInt new_num_row = lp.num_row_ + num_new_row;
  lp.row_lower_.resize(new_num_row);
  lp.row_upper_.resize(new_num_row);
  bool have_names = (lp.row_names_.size() != 0);
  if (have_names) lp.row_names_.resize(new_num_row);

  for (HighsInt new_row = 0; new_row < num_new_row; new_row++) {
    HighsInt iRow = lp.num_row_ + new_row;
    lp.row_lower_[iRow] = rowLower[new_row];
    lp.row_upper_[iRow] = rowUpper[new_row];
    // Cannot guarantee to create unique names, so name is blank
    if (have_names) lp.row_names_[iRow] = "";
  }
}

void deleteScale(vector<double>& scale,
                 const HighsIndexCollection& index_collection) {
  assert(ok(index_collection));
  HighsInt from_k;
  HighsInt to_k;
  limits(index_collection, from_k, to_k);
  if (from_k > to_k) return;

  HighsInt delete_from_col;
  HighsInt delete_to_col;
  HighsInt keep_from_col;
  HighsInt keep_to_col = -1;
  HighsInt current_set_entry = 0;

  HighsInt col_dim = index_collection.dimension_;
  HighsInt new_num_col = 0;
  for (HighsInt k = from_k; k <= to_k; k++) {
    updateOutInIndex(index_collection, delete_from_col, delete_to_col,
                     keep_from_col, keep_to_col, current_set_entry);
    // Account for the initial columns being kept
    if (k == from_k) new_num_col = delete_from_col;
    if (delete_to_col >= col_dim - 1) break;
    assert(delete_to_col < col_dim);
    for (HighsInt col = keep_from_col; col <= keep_to_col; col++) {
      assert((HighsInt)scale.size() > new_num_col);
      scale[new_num_col] = scale[col];
      new_num_col++;
    }
    if (keep_to_col >= col_dim - 1) break;
  }
}

void changeLpMatrixCoefficient(HighsLp& lp, const HighsInt row,
                               const HighsInt col, const double new_value,
                               const bool zero_new_value) {
  assert(0 <= row && row < lp.num_row_);
  assert(0 <= col && col < lp.num_col_);

  // Determine whether the coefficient corresponds to an existing
  // nonzero
  HighsInt change_el = -1;
  for (HighsInt el = lp.a_matrix_.start_[col];
       el < lp.a_matrix_.start_[col + 1]; el++) {
    if (lp.a_matrix_.index_[el] == row) {
      change_el = el;
      break;
    }
  }
  if (change_el < 0) {
    // Coefficient doesn't correspond to an existing nonzero
    //
    // If coefficient is small, then just ignore it
    if (zero_new_value) return;
    // New nonzero goes at the end of column "col", so have to shift
    // all index and value entries forward by 1 to accommodate it
    change_el = lp.a_matrix_.start_[col + 1];
    HighsInt new_num_nz = lp.a_matrix_.start_[lp.num_col_] + 1;
    lp.a_matrix_.index_.resize(new_num_nz);
    lp.a_matrix_.value_.resize(new_num_nz);
    for (HighsInt i = col + 1; i <= lp.num_col_; i++) lp.a_matrix_.start_[i]++;
    for (HighsInt el = new_num_nz - 1; el > change_el; el--) {
      lp.a_matrix_.index_[el] = lp.a_matrix_.index_[el - 1];
      lp.a_matrix_.value_[el] = lp.a_matrix_.value_[el - 1];
    }
  } else if (zero_new_value) {
    // Coefficient zeroes an existing nonzero, so shift all index and
    // value entries backward by 1 to eliminate it
    HighsInt new_num_nz = lp.a_matrix_.start_[lp.num_col_] - 1;
    for (HighsInt i = col + 1; i <= lp.num_col_; i++) lp.a_matrix_.start_[i]--;
    for (HighsInt el = change_el; el < new_num_nz; el++) {
      lp.a_matrix_.index_[el] = lp.a_matrix_.index_[el + 1];
      lp.a_matrix_.value_[el] = lp.a_matrix_.value_[el + 1];
    }
    return;
  }
  lp.a_matrix_.index_[change_el] = row;
  lp.a_matrix_.value_[change_el] = new_value;
}

void changeLpIntegrality(HighsLp& lp,
                         const HighsIndexCollection& index_collection,
                         const vector<HighsVarType>& new_integrality) {
  assert(ok(index_collection));
  HighsInt from_k;
  HighsInt to_k;
  limits(index_collection, from_k, to_k);
  if (from_k > to_k) return;

  const bool& interval = index_collection.is_interval_;
  const bool& mask = index_collection.is_mask_;
  const vector<HighsInt>& col_set = index_collection.set_;
  const vector<HighsInt>& col_mask = index_collection.mask_;

  // Change the integrality to the user-supplied integrality, according to the
  // technique
  HighsInt lp_col;
  HighsInt usr_col = -1;
  // If changing integrality for a problem without an integrality
  // vector (ie an LP), have to create it for the incumbent columns -
  // which are naturally continuous
  if (lp.integrality_.size() == 0)
    lp.integrality_.assign(lp.num_col_, HighsVarType::kContinuous);
  assert(HighsInt(lp.integrality_.size()) == lp.num_col_);
  for (HighsInt k = from_k; k < to_k + 1; k++) {
    if (interval || mask) {
      lp_col = k;
    } else {
      lp_col = col_set[k];
    }
    HighsInt col = lp_col;
    if (interval) {
      usr_col++;
    } else {
      usr_col = k;
    }
    if (mask && !col_mask[col]) continue;
    lp.integrality_[col] = new_integrality[usr_col];
  }
  // If integrality_ contains only HighsVarType::kContinuous then
  // clear it
  if (!lp.isMip()) lp.integrality_.clear();
}

void changeLpCosts(HighsLp& lp, const HighsIndexCollection& index_collection,
                   const vector<double>& new_col_cost,
                   const double infinite_cost) {
  assert(ok(index_collection));
  HighsInt from_k;
  HighsInt to_k;
  limits(index_collection, from_k, to_k);
  if (from_k > to_k) return;

  const bool& interval = index_collection.is_interval_;
  const bool& mask = index_collection.is_mask_;
  const vector<HighsInt>& col_set = index_collection.set_;
  const vector<HighsInt>& col_mask = index_collection.mask_;

  // Change the costs to the user-supplied costs, according to the technique
  HighsInt lp_col;
  HighsInt usr_col = -1;
  for (HighsInt k = from_k; k < to_k + 1; k++) {
    if (interval || mask) {
      lp_col = k;
    } else {
      lp_col = col_set[k];
    }
    HighsInt col = lp_col;
    if (interval) {
      usr_col++;
    } else {
      usr_col = k;
    }
    if (mask && !col_mask[col]) continue;
    lp.col_cost_[col] = new_col_cost[usr_col];
  }
  // Check whether the LP still has an infinite cost
  if (lp.has_infinite_cost_)
    lp.has_infinite_cost_ = lp.hasInfiniteCost(infinite_cost);
}

void changeLpColBounds(HighsLp& lp,
                       const HighsIndexCollection& index_collection,
                       const vector<double>& new_col_lower,
                       const vector<double>& new_col_upper) {
  changeBounds(lp.col_lower_, lp.col_upper_, index_collection, new_col_lower,
               new_col_upper);
}

void changeLpRowBounds(HighsLp& lp,
                       const HighsIndexCollection& index_collection,
                       const vector<double>& new_row_lower,
                       const vector<double>& new_row_upper) {
  changeBounds(lp.row_lower_, lp.row_upper_, index_collection, new_row_lower,
               new_row_upper);
}

void changeBounds(vector<double>& lower, vector<double>& upper,
                  const HighsIndexCollection& index_collection,
                  const vector<double>& new_lower,
                  const vector<double>& new_upper) {
  assert(ok(index_collection));
  HighsInt from_k;
  HighsInt to_k;
  limits(index_collection, from_k, to_k);
  if (from_k > to_k) return;

  const bool& interval = index_collection.is_interval_;
  const bool& mask = index_collection.is_mask_;
  const vector<HighsInt>& ix_set = index_collection.set_;
  const vector<HighsInt>& ix_mask = index_collection.mask_;

  // Change the bounds to the user-supplied bounds, according to the technique
  HighsInt lp_ix;
  HighsInt usr_ix = -1;
  for (HighsInt k = from_k; k < to_k + 1; k++) {
    if (interval || mask) {
      lp_ix = k;
    } else {
      lp_ix = ix_set[k];
    }
    HighsInt ix = lp_ix;
    if (interval) {
      usr_ix++;
    } else {
      usr_ix = k;
    }
    if (mask && !ix_mask[ix]) continue;
    lower[ix] = new_lower[usr_ix];
    upper[ix] = new_upper[usr_ix];
  }
}

HighsInt getNumInt(const HighsLp& lp) {
  HighsInt num_int = 0;
  if (lp.integrality_.size()) {
    for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++)
      if (lp.integrality_[iCol] == HighsVarType::kInteger) num_int++;
  }
  return num_int;
}

void getLpCosts(const HighsLp& lp, const HighsInt from_col,
                const HighsInt to_col, double* XcolCost) {
  assert(0 <= from_col && from_col < lp.num_col_);
  assert(0 <= to_col && to_col < lp.num_col_);
  if (from_col > to_col) return;
  for (HighsInt col = from_col; col < to_col + 1; col++)
    XcolCost[col - from_col] = lp.col_cost_[col];
}

void getLpColBounds(const HighsLp& lp, const HighsInt from_col,
                    const HighsInt to_col, double* XcolLower,
                    double* XcolUpper) {
  assert(0 <= from_col && from_col < lp.num_col_);
  assert(0 <= to_col && to_col < lp.num_col_);
  if (from_col > to_col) return;
  for (HighsInt col = from_col; col < to_col + 1; col++) {
    if (XcolLower != nullptr) XcolLower[col - from_col] = lp.col_lower_[col];
    if (XcolUpper != nullptr) XcolUpper[col - from_col] = lp.col_upper_[col];
  }
}

void getLpRowBounds(const HighsLp& lp, const HighsInt from_row,
                    const HighsInt to_row, double* XrowLower,
                    double* XrowUpper) {
  assert(0 <= from_row && from_row < lp.num_row_);
  assert(0 <= to_row && to_row < lp.num_row_);
  if (from_row > to_row) return;
  for (HighsInt row = from_row; row < to_row + 1; row++) {
    if (XrowLower != nullptr) XrowLower[row - from_row] = lp.row_lower_[row];
    if (XrowUpper != nullptr) XrowUpper[row - from_row] = lp.row_upper_[row];
  }
}

// Get a single coefficient from the matrix
void getLpMatrixCoefficient(const HighsLp& lp, const HighsInt Xrow,
                            const HighsInt Xcol, double* val) {
  assert(0 <= Xrow && Xrow < lp.num_row_);
  assert(0 <= Xcol && Xcol < lp.num_col_);

  HighsInt get_el = -1;
  for (HighsInt el = lp.a_matrix_.start_[Xcol];
       el < lp.a_matrix_.start_[Xcol + 1]; el++) {
    if (lp.a_matrix_.index_[el] == Xrow) {
      get_el = el;
      break;
    }
  }
  if (get_el < 0) {
    *val = 0;
  } else {
    *val = lp.a_matrix_.value_[get_el];
  }
}

// Methods for reporting an LP, including its row and column data and matrix
//
// Report the whole LP
void reportLp(const HighsLogOptions& log_options, const HighsLp& lp,
              const HighsLogType report_level) {
  reportLpBrief(log_options, lp);
  if ((HighsInt)report_level >= (HighsInt)HighsLogType::kDetailed) {
    reportLpColVectors(log_options, lp);
    reportLpRowVectors(log_options, lp);
    if ((HighsInt)report_level >= (HighsInt)HighsLogType::kVerbose)
      reportLpColMatrix(log_options, lp);
  }
}

// Report the LP briefly
void reportLpBrief(const HighsLogOptions& log_options, const HighsLp& lp) {
  reportLpDimensions(log_options, lp);
  reportLpObjSense(log_options, lp);
}

// Report the LP dimensions
void reportLpDimensions(const HighsLogOptions& log_options, const HighsLp& lp) {
  HighsInt lp_num_nz;
  if (lp.num_col_ == 0)
    lp_num_nz = 0;
  else
    lp_num_nz = lp.a_matrix_.start_[lp.num_col_];
  highsLogUser(log_options, HighsLogType::kInfo,
               "LP has %" HIGHSINT_FORMAT " columns, %" HIGHSINT_FORMAT " rows",
               lp.num_col_, lp.num_row_);
  HighsInt num_int = getNumInt(lp);
  if (num_int) {
    highsLogUser(log_options, HighsLogType::kInfo,
                 ", %" HIGHSINT_FORMAT " nonzeros and %" HIGHSINT_FORMAT
                 " integer columns\n",
                 lp_num_nz, num_int);
  } else {
    highsLogUser(log_options, HighsLogType::kInfo,
                 " and %" HIGHSINT_FORMAT " nonzeros\n", lp_num_nz, num_int);
  }
}

// Report the LP objective sense
void reportLpObjSense(const HighsLogOptions& log_options, const HighsLp& lp) {
  if (lp.sense_ == ObjSense::kMinimize)
    highsLogUser(log_options, HighsLogType::kInfo,
                 "Objective sense is minimize\n");
  else if (lp.sense_ == ObjSense::kMaximize)
    highsLogUser(log_options, HighsLogType::kInfo,
                 "Objective sense is maximize\n");
  else
    highsLogUser(log_options, HighsLogType::kInfo,
                 "Objective sense is ill-defined as %" HIGHSINT_FORMAT "\n",
                 lp.sense_);
}

static std::string getBoundType(const double lower, const double upper) {
  std::string type;
  if (highs_isInfinity(-lower)) {
    if (highs_isInfinity(upper)) {
      type = "FR";
    } else {
      type = "UB";
    }
  } else {
    if (highs_isInfinity(upper)) {
      type = "LB";
    } else {
      if (lower < upper) {
        type = "BX";
      } else {
        type = "FX";
      }
    }
  }
  return type;
}

// Report the vectors of LP column data
void reportLpColVectors(const HighsLogOptions& log_options, const HighsLp& lp) {
  if (lp.num_col_ <= 0) return;
  std::string type;
  HighsInt count;
  bool have_integer_columns = (getNumInt(lp) != 0);
  bool have_col_names = (lp.col_names_.size() != 0);

  highsLogUser(log_options, HighsLogType::kInfo,
               "  Column        Lower        Upper         Cost       "
               "Type        Count");
  if (have_integer_columns)
    highsLogUser(log_options, HighsLogType::kInfo, "  Discrete");
  if (have_col_names) highsLogUser(log_options, HighsLogType::kInfo, "  Name");
  highsLogUser(log_options, HighsLogType::kInfo, "\n");

  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    type = getBoundType(lp.col_lower_[iCol], lp.col_upper_[iCol]);
    count = lp.a_matrix_.start_[iCol + 1] - lp.a_matrix_.start_[iCol];
    highsLogUser(log_options, HighsLogType::kInfo,
                 "%8" HIGHSINT_FORMAT
                 " %12g %12g %12g         %2s %12" HIGHSINT_FORMAT "",
                 iCol, lp.col_lower_[iCol], lp.col_upper_[iCol],
                 lp.col_cost_[iCol], type.c_str(), count);
    if (have_integer_columns) {
      std::string integer_column = "";
      if (lp.integrality_[iCol] == HighsVarType::kInteger) {
        if (lp.col_lower_[iCol] == 0 && lp.col_upper_[iCol] == 1) {
          integer_column = "Binary";
        } else {
          integer_column = "Integer";
        }
      }
      highsLogUser(log_options, HighsLogType::kInfo, "  %-8s",
                   integer_column.c_str());
    }
    if (have_col_names)
      highsLogUser(log_options, HighsLogType::kInfo, "  %-s",
                   lp.col_names_[iCol].c_str());
    highsLogUser(log_options, HighsLogType::kInfo, "\n");
  }
}

// Report the vectors of LP row data
void reportLpRowVectors(const HighsLogOptions& log_options, const HighsLp& lp) {
  if (lp.num_row_ <= 0) return;
  std::string type;
  vector<HighsInt> count;
  bool have_row_names = (lp.row_names_.size() != 0);

  count.resize(lp.num_row_, 0);
  if (lp.num_col_ > 0) {
    for (HighsInt el = 0; el < lp.a_matrix_.start_[lp.num_col_]; el++)
      count[lp.a_matrix_.index_[el]]++;
  }

  highsLogUser(log_options, HighsLogType::kInfo,
               "     Row        Lower        Upper       Type        Count");
  if (have_row_names) highsLogUser(log_options, HighsLogType::kInfo, "  Name");
  highsLogUser(log_options, HighsLogType::kInfo, "\n");

  for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
    type = getBoundType(lp.row_lower_[iRow], lp.row_upper_[iRow]);
    std::string name = "";
    highsLogUser(log_options, HighsLogType::kInfo,
                 "%8" HIGHSINT_FORMAT
                 " %12g %12g         %2s %12" HIGHSINT_FORMAT "",
                 iRow, lp.row_lower_[iRow], lp.row_upper_[iRow], type.c_str(),
                 count[iRow]);
    if (have_row_names)
      highsLogUser(log_options, HighsLogType::kInfo, "  %-s",
                   lp.row_names_[iRow].c_str());
    highsLogUser(log_options, HighsLogType::kInfo, "\n");
  }
}

// Report the LP column-wise matrix
void reportLpColMatrix(const HighsLogOptions& log_options, const HighsLp& lp) {
  if (lp.num_col_ <= 0) return;
  if (lp.num_row_) {
    // With positive number of rows, can assume that there are index and value
    // vectors to pass
    reportMatrix(log_options, "Column", lp.num_col_,
                 lp.a_matrix_.start_[lp.num_col_], lp.a_matrix_.start_.data(),
                 lp.a_matrix_.index_.data(), lp.a_matrix_.value_.data());
  } else {
    // With no rows, can's assume that there are index and value vectors to pass
    reportMatrix(log_options, "Column", lp.num_col_,
                 lp.a_matrix_.start_[lp.num_col_], lp.a_matrix_.start_.data(),
                 NULL, NULL);
  }
}

void reportMatrix(const HighsLogOptions& log_options, const std::string message,
                  const HighsInt num_col, const HighsInt num_nz,
                  const HighsInt* start, const HighsInt* index,
                  const double* value) {
  if (num_col <= 0) return;
  highsLogUser(log_options, HighsLogType::kInfo,
               "%-7s Index              Value\n", message.c_str());
  for (HighsInt col = 0; col < num_col; col++) {
    highsLogUser(log_options, HighsLogType::kInfo,
                 "    %8" HIGHSINT_FORMAT " Start   %10" HIGHSINT_FORMAT "\n",
                 col, start[col]);
    HighsInt to_el = (col < num_col - 1 ? start[col + 1] : num_nz);
    for (HighsInt el = start[col]; el < to_el; el++)
      highsLogUser(log_options, HighsLogType::kInfo,
                   "          %8" HIGHSINT_FORMAT " %12g\n", index[el],
                   value[el]);
  }
  highsLogUser(log_options, HighsLogType::kInfo,
               "             Start   %10" HIGHSINT_FORMAT "\n", num_nz);
}

void analyseLp(const HighsLogOptions& log_options, const HighsLp& lp) {
  /*
  vector<double> min_colBound;
  vector<double> min_rowBound;
  vector<double> colRange;
  vector<double> rowRange;
  min_colBound.resize(lp.num_col_);
  min_rowBound.resize(lp.num_row_);
  colRange.resize(lp.num_col_);
  rowRange.resize(lp.num_row_);
  for (HighsInt col = 0; col < lp.num_col_; col++)
    min_colBound[col] = min(fabs(lp.col_lower_[col]), fabs(lp.col_upper_[col]));
  for (HighsInt row = 0; row < lp.num_row_; row++)
    min_rowBound[row] = min(fabs(lp.row_lower_[row]), fabs(lp.row_upper_[row]));
  for (HighsInt col = 0; col < lp.num_col_; col++)
    colRange[col] = lp.col_upper_[col] - lp.col_lower_[col];
  for (HighsInt row = 0; row < lp.num_row_; row++)
    rowRange[row] = lp.row_upper_[row] - lp.row_lower_[row];
  */
  std::string message;
  if (lp.is_scaled_) {
    message = "Scaled";
  } else {
    message = "Unscaled";
  }
  highsLogDev(log_options, HighsLogType::kInfo, "\n%s model data: Analysis\n",
              message.c_str());
  if (lp.is_scaled_) {
    const HighsScale& scale = lp.scale_;
    analyseVectorValues(&log_options, "Column scaling factors", lp.num_col_,
                        scale.col, true, lp.model_name_);
    analyseVectorValues(&log_options, "Row    scaling factors", lp.num_row_,
                        scale.row, true, lp.model_name_);
  }
  analyseVectorValues(&log_options, "Column costs", lp.num_col_, lp.col_cost_,
                      true, lp.model_name_);
  analyseVectorValues(&log_options, "Column lower bounds", lp.num_col_,
                      lp.col_lower_, true, lp.model_name_);
  analyseVectorValues(&log_options, "Column upper bounds", lp.num_col_,
                      lp.col_upper_, true, lp.model_name_);
  //  analyseVectorValues(&log_options, "Column min abs bound", lp.num_col_,
  //                      min_colBound, true, lp.model_name_);
  //  analyseVectorValues(&log_options, "Column range", lp.num_col_, colRange,
  //  true,
  //                      lp.model_name_);
  analyseVectorValues(&log_options, "Row lower bounds", lp.num_row_,
                      lp.row_lower_, true, lp.model_name_);
  analyseVectorValues(&log_options, "Row upper bounds", lp.num_row_,
                      lp.row_upper_, true, lp.model_name_);
  //  analyseVectorValues(&log_options, "Row min abs bound", lp.num_row_,
  //                      min_rowBound, true, lp.model_name_);
  //  analyseVectorValues(&log_options, "Row range", lp.num_row_, rowRange,
  //  true,
  //                      lp.model_name_);
  analyseVectorValues(&log_options, "Matrix sparsity",
                      lp.a_matrix_.start_[lp.num_col_], lp.a_matrix_.value_,
                      true, lp.model_name_);
  analyseMatrixSparsity(log_options, "Constraint matrix", lp.num_col_,
                        lp.num_row_, lp.a_matrix_.start_, lp.a_matrix_.index_);
  analyseModelBounds(log_options, "Column", lp.num_col_, lp.col_lower_,
                     lp.col_upper_);
  analyseModelBounds(log_options, "Row", lp.num_row_, lp.row_lower_,
                     lp.row_upper_);
}

HighsStatus readSolutionFile(const std::string filename,
                             const HighsOptions& options, HighsLp& lp,
                             HighsBasis& basis, HighsSolution& solution,
                             const HighsInt style) {
  const HighsLogOptions& log_options = options.log_options;
  if (style != kSolutionStyleRaw && style != kSolutionStyleSparse) {
    highsLogUser(log_options, HighsLogType::kError,
                 "readSolutionFile: Cannot read file of style %d\n",
                 (int)style);
    return HighsStatus::kError;
  }
  std::ifstream in_file(filename);
  if (in_file.fail()) {
    highsLogUser(log_options, HighsLogType::kError,
                 "readSolutionFile: Cannot open readable file \"%s\"\n",
                 filename.c_str());
    return HighsStatus::kError;
  }
  std::string keyword;
  std::string name;
  double value;
  HighsInt num_col;
  HighsInt num_row;
  const HighsInt lp_num_col = lp.num_col_;
  const HighsInt lp_num_row = lp.num_row_;
  // Define identifiers for reading in
  HighsSolution read_solution = solution;
  HighsBasis read_basis = basis;
  read_solution.clear();
  read_basis.clear();
  read_solution.col_value.resize(lp_num_col);
  read_solution.row_value.resize(lp_num_row);
  read_solution.col_dual.resize(lp_num_col);
  read_solution.row_dual.resize(lp_num_row);
  read_basis.col_status.resize(lp_num_col);
  read_basis.row_status.resize(lp_num_row);
  std::string section_name;
  if (!readSolutionFileIdIgnoreLineOk(section_name, in_file))
    return readSolutionFileErrorReturn(
        in_file);  // Model (status) or =obj= (value)
  const bool miplib_sol = section_name == "=obj=";
  if (miplib_sol) {
    // A MIPLIB solution file has nonzero solution values for a subset
    // of the variables identified by name, so there must be column
    // names
    if (!lp.col_names_.size()) {
      highsLogUser(log_options, HighsLogType::kError,
                   "readSolutionFile: Cannot read a MIPLIB solution file "
                   "without column names in the model\n");
      return HighsStatus::kError;
    }
    // Ensure that the col name hash table has been formed
    if (!lp.col_hash_.name2index.size()) lp.col_hash_.form(lp.col_names_);
  }
  bool sparse = false;
  if (!miplib_sol) {
    if (!readSolutionFileIgnoreLineOk(in_file))
      return readSolutionFileErrorReturn(in_file);  // Optimal
    if (!readSolutionFileIgnoreLineOk(in_file))
      return readSolutionFileErrorReturn(in_file);  //
    if (!readSolutionFileIgnoreLineOk(in_file))
      return readSolutionFileErrorReturn(in_file);  // # Primal solution values
    if (!readSolutionFileKeywordLineOk(keyword, in_file))
      return readSolutionFileErrorReturn(in_file);
    // Read in the primal solution values: return warning if there is none
    if (keyword == "None")
      return readSolutionFileReturn(HighsStatus::kWarning, solution, basis,
                                    read_solution, read_basis, in_file);
    // If there are primal solution values then keyword is the status
    // and the next line is objective
    if (!readSolutionFileIgnoreLineOk(in_file))
      return readSolutionFileErrorReturn(in_file);  // EOL
    if (!readSolutionFileIgnoreLineOk(in_file))
      return readSolutionFileErrorReturn(in_file);  // Objective
    // Next line should be "Columns" and correct number
    if (!readSolutionFileHashKeywordIntLineOk(keyword, num_col, in_file))
      return readSolutionFileErrorReturn(in_file);
    assert(keyword == "Columns");
    // The default style parameter is kSolutionStyleRaw, and this still
    // allows sparse files to be read. Recognise the latter from num_col
    // <= 0. Doesn't matter if num_col = 0, since there's nothing to
    // read either way
    sparse = num_col <= 0;
    if (style == kSolutionStyleSparse) assert(sparse);
    if (sparse) {
      num_col = -num_col;
      assert(num_col <= lp_num_col);
    } else {
      if (num_col != lp_num_col) {
        highsLogUser(log_options, HighsLogType::kError,
                     "readSolutionFile: Solution file is for %" HIGHSINT_FORMAT
                     " columns, not %" HIGHSINT_FORMAT "\n",
                     num_col, lp_num_col);
        return readSolutionFileErrorReturn(in_file);
      }
    }
  }
  if (miplib_sol) {
    HighsInt num_value = 0;
    read_solution.col_value.assign(lp_num_col, 0);
    for (;;) {
      // Only false return is for encountering EOF
      if (!readSolutionFileIdDoubleLineOk(name, value, in_file)) break;
      auto search = lp.col_hash_.name2index.find(name);
      if (search == lp.col_hash_.name2index.end()) {
        highsLogUser(log_options, HighsLogType::kError,
                     "readSolutionFile: name %s is not found\n", name.c_str());
        return HighsStatus::kError;
      } else if (search->second == kHashIsDuplicate) {
        highsLogUser(log_options, HighsLogType::kError,
                     "readSolutionFile: name %s is duplicated\n", name.c_str());
        return HighsStatus::kError;
      }
      HighsInt iCol = search->second;
      assert(lp.col_names_[iCol] == name);
      read_solution.col_value[iCol] = value;
      num_value++;
      if (in_file.eof()) break;
    }
  } else if (sparse) {
    read_solution.col_value.assign(lp_num_col, 0);
    HighsInt iCol;
    for (HighsInt iX = 0; iX < num_col; iX++) {
      if (!readSolutionFileIdDoubleIntLineOk(value, iCol, in_file))
        return readSolutionFileErrorReturn(in_file);
      read_solution.col_value[iCol] = value;
    }
  } else {
    for (HighsInt iCol = 0; iCol < num_col; iCol++) {
      if (!readSolutionFileIdDoubleLineOk(name, value, in_file))
        return readSolutionFileErrorReturn(in_file);
      read_solution.col_value[iCol] = value;
    }
  }
  read_solution.value_valid = true;
  if (sparse) {
    if (calculateRowValuesQuad(lp, read_solution.col_value,
                               read_solution.row_value) != HighsStatus::kOk)
      return readSolutionFileErrorReturn(in_file);
    return readSolutionFileReturn(HighsStatus::kOk, solution, basis,
                                  read_solution, read_basis, in_file);
  }
  // Read in the col values: OK to have none, otherwise next line
  // should be "Rows" and correct number
  if (!readSolutionFileHashKeywordIntLineOk(keyword, num_row, in_file)) {
    // Compute the row values since there are none to read
    if (calculateRowValuesQuad(lp, read_solution.col_value,
                               read_solution.row_value) != HighsStatus::kOk)
      return readSolutionFileErrorReturn(in_file);
    return readSolutionFileReturn(HighsStatus::kOk, solution, basis,
                                  read_solution, read_basis, in_file);
  }
  assert(keyword == "Rows");
  // OK to read from a file with different number of rows, since the
  // primal solution is all that's important. For example, see #1284,
  // where the user is solving a sequence of MIPs with the same number
  // of variables, but increasing numbers of constraints, and wants to
  // used the solution from one MIP as the starting solution for the
  // next.
  const bool num_row_ok = num_row == lp_num_row;
  for (HighsInt iRow = 0; iRow < num_row; iRow++) {
    if (!readSolutionFileIdDoubleLineOk(name, value, in_file))
      return readSolutionFileErrorReturn(in_file);
    if (num_row_ok) read_solution.row_value[iRow] = value;
  }
  if (!num_row_ok) {
    highsLogUser(log_options, HighsLogType::kWarning,
                 "readSolutionFile: Solution file is for %" HIGHSINT_FORMAT
                 " rows, not %" HIGHSINT_FORMAT ": row values ignored\n",
                 num_row, lp_num_row);
    // Calculate the row values
    if (calculateRowValuesQuad(lp, read_solution.col_value,
                               read_solution.row_value) != HighsStatus::kOk)
      return readSolutionFileErrorReturn(in_file);
  }
  // OK to have no EOL
  if (!readSolutionFileIgnoreLineOk(in_file))
    return readSolutionFileReturn(HighsStatus::kOk, solution, basis,
                                  read_solution, read_basis,
                                  in_file);  // EOL
  // OK to have no blank line
  if (!readSolutionFileIgnoreLineOk(in_file))
    return readSolutionFileReturn(HighsStatus::kOk, solution, basis,
                                  read_solution, read_basis,
                                  in_file);  //
  // OK to have no reference to dual solution values
  if (!readSolutionFileIgnoreLineOk(in_file))
    return readSolutionFileReturn(HighsStatus::kOk, solution, basis,
                                  read_solution, read_basis,
                                  in_file);  // # Dual solution values
  // If there's a reference to dual solution values, there's a keyword
  // to indicate their status
  if (!readSolutionFileKeywordLineOk(keyword, in_file))
    return readSolutionFileErrorReturn(in_file);
  if (keyword != "None") {
    if (!readSolutionFileIgnoreLineOk(in_file))
      return readSolutionFileErrorReturn(in_file);  // EOL
    // Next line should be "Columns" and correct number
    if (!readSolutionFileHashKeywordIntLineOk(keyword, num_col, in_file))
      return readSolutionFileReturn(HighsStatus::kOk, solution, basis,
                                    read_solution, read_basis, in_file);
    assert(keyword == "Columns");
    double dual;
    for (HighsInt iCol = 0; iCol < num_col; iCol++) {
      if (!readSolutionFileIdDoubleLineOk(name, dual, in_file))
        return readSolutionFileErrorReturn(in_file);
      read_solution.col_dual[iCol] = dual;
    }
    // Read in the col values: next line should be "Rows" and correct
    // number
    if (!readSolutionFileHashKeywordIntLineOk(keyword, num_row, in_file))
      return readSolutionFileReturn(HighsStatus::kOk, solution, basis,
                                    read_solution, read_basis, in_file);
    assert(keyword == "Rows");
    for (HighsInt iRow = 0; iRow < num_row; iRow++) {
      if (!readSolutionFileIdDoubleLineOk(name, dual, in_file))
        return readSolutionFileErrorReturn(in_file);
      read_solution.row_dual[iRow] = dual;
    }
  }
  // OK to have no EOL
  if (!readSolutionFileIgnoreLineOk(in_file))
    return readSolutionFileReturn(HighsStatus::kOk, solution, basis,
                                  read_solution, read_basis,
                                  in_file);  // EOL
  // OK to have no blank line
  if (!readSolutionFileIgnoreLineOk(in_file))
    return readSolutionFileReturn(HighsStatus::kOk, solution, basis,
                                  read_solution, read_basis,
                                  in_file);  //
  // OK to have no reference to basis
  if (!readSolutionFileIgnoreLineOk(in_file))
    return readSolutionFileReturn(HighsStatus::kOk, solution, basis,
                                  read_solution, read_basis,
                                  in_file);  // # Basis
  HighsStatus basis_read_status =
      readBasisStream(log_options, read_basis, in_file);
  // Return with basis read status
  return readSolutionFileReturn(basis_read_status, solution, basis,
                                read_solution, read_basis, in_file);
}

HighsStatus readSolutionFileErrorReturn(std::ifstream& in_file) {
  in_file.close();
  return HighsStatus::kError;
}

HighsStatus readSolutionFileReturn(const HighsStatus status,
                                   HighsSolution& solution, HighsBasis& basis,
                                   const HighsSolution& read_solution,
                                   const HighsBasis& read_basis,
                                   std::ifstream& in_file) {
  in_file.close();
  if (status != HighsStatus::kOk) {
    return status;
  }
  solution = read_solution;
  basis = read_basis;
  return HighsStatus::kOk;
}

bool readSolutionFileIgnoreLineOk(std::ifstream& in_file) {
  if (in_file.eof()) return false;
  in_file.ignore(kMaxLineLength, '\n');
  return true;
}

bool readSolutionFileKeywordLineOk(std::string& keyword,
                                   std::ifstream& in_file) {
  if (in_file.eof()) return false;
  in_file >> keyword;
  return true;
}

bool readSolutionFileHashKeywordIntLineOk(std::string& keyword, HighsInt& value,
                                          std::ifstream& in_file) {
  if (in_file.eof()) return false;
  in_file >> keyword;  // #
  if (in_file.eof()) return false;
  in_file >> keyword;  // keyword
  if (in_file.eof()) return false;
  in_file >> value;  // integer value
  return true;
}

bool readSolutionFileIdIgnoreLineOk(std::string& id, std::ifstream& in_file) {
  if (in_file.eof()) return false;
  in_file >> id;  // Id
  in_file.ignore(kMaxLineLength, '\n');
  return true;
}

bool readSolutionFileIdDoubleLineOk(std::string& id, double& value,
                                    std::ifstream& in_file) {
  if (in_file.eof()) return false;
  in_file >> id;  // Id
  if (in_file.eof()) return false;
  in_file >> value;  // double value
  return true;
}

bool readSolutionFileIdDoubleIntLineOk(double& value, HighsInt& index,
                                       std::ifstream& in_file) {
  std::string id;
  if (in_file.eof()) return false;
  in_file >> id;  // Id
  if (in_file.eof()) return false;
  in_file >> value;  // double value
  if (in_file.eof()) return false;
  in_file >> index;  // double value
  return true;
}

void assessColPrimalSolution(const HighsOptions& options, const double primal,
                             const double lower, const double upper,
                             const HighsVarType type, double& col_infeasibility,
                             double& integer_infeasibility) {
  // @primal_infeasibility calculation
  col_infeasibility = 0;
  if (primal < lower - options.primal_feasibility_tolerance) {
    col_infeasibility = lower - primal;
  } else if (primal > upper + options.primal_feasibility_tolerance) {
    col_infeasibility = primal - upper;
  }
  integer_infeasibility = 0;
  if (type == HighsVarType::kInteger || type == HighsVarType::kSemiInteger) {
    integer_infeasibility = fractionality(primal);
  }
  if (col_infeasibility > 0 && (type == HighsVarType::kSemiContinuous ||
                                type == HighsVarType::kSemiInteger)) {
    // Semi-variables at zero will have positive col
    // infeasibility, so possibly zero this
    if (std::fabs(primal) <= options.mip_feasibility_tolerance)
      col_infeasibility = 0;
    // If there is (still) column infeasibility due to value being
    // off zero or below lower bound, then this counts as an integer
    // infeasibility
    if (col_infeasibility && primal < upper)
      integer_infeasibility =
          std::max(col_infeasibility, integer_infeasibility);
  }
}

// Determine validity, primal feasibility and (when relevant) integer
// feasibility of a solution
HighsStatus assessLpPrimalSolution(const std::string message,
                                   const HighsOptions& options,
                                   const HighsLp& lp,
                                   const HighsSolution& solution, bool& valid,
                                   bool& integral, bool& feasible) {
  valid = false;
  integral = false;
  feasible = false;
  HighsInt num_col_infeasibilities = 0;
  double max_col_infeasibility = 0;
  double sum_col_infeasibilities = 0;
  HighsInt num_integer_infeasibilities = 0;
  double max_integer_infeasibility = 0;
  double sum_integer_infeasibilities = 0;
  HighsInt num_row_infeasibilities = 0;
  double max_row_infeasibility = 0;
  double sum_row_infeasibilities = 0;
  HighsInt num_row_residuals = 0;
  double max_row_residual = 0;
  double sum_row_residuals = 0;
  const double kRowResidualTolerance =
      options.primal_feasibility_tolerance;  // 1e-12;
  const double kPrimalFeasibilityTolerance =
      lp.isMip() ? options.mip_feasibility_tolerance
                 : options.primal_feasibility_tolerance;
  highsLogUser(options.log_options, HighsLogType::kInfo,
               "%sAssessing feasibility of %s tolerance of %11.4g\n",
               message.c_str(),
               lp.isMip() ? "MIP using primal feasibility and integrality"
                          : "LP using primal feasibility",
               kPrimalFeasibilityTolerance);
  vector<double> row_value;
  row_value.assign(lp.num_row_, 0);
  const bool have_integrality = (lp.integrality_.size() != 0);
  if (!solution.value_valid) return HighsStatus::kError;
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    const double primal = solution.col_value[iCol];
    const double lower = lp.col_lower_[iCol];
    const double upper = lp.col_upper_[iCol];
    const HighsVarType type =
        have_integrality ? lp.integrality_[iCol] : HighsVarType::kContinuous;

    double col_infeasibility = 0;
    double integer_infeasibility = 0;

    assessColPrimalSolution(options, primal, lower, upper, type,
                            col_infeasibility, integer_infeasibility);
    if (col_infeasibility > 0) {
      if (col_infeasibility > kPrimalFeasibilityTolerance) {
        if (col_infeasibility > 2 * max_col_infeasibility)
          highsLogUser(options.log_options, HighsLogType::kWarning,
                       "Col %6d has         infeasibility of %11.4g from "
                       "[lower, value, upper] = [%15.8g; %15.8g; %15.8g]\n",
                       (int)iCol, col_infeasibility, lower, primal, upper);
        num_col_infeasibilities++;
      }
      max_col_infeasibility =
          std::max(col_infeasibility, max_col_infeasibility);
      sum_col_infeasibilities += col_infeasibility;
    }
    if (integer_infeasibility > 0) {
      if (integer_infeasibility > options.mip_feasibility_tolerance) {
        if (integer_infeasibility > 2 * max_integer_infeasibility)
          highsLogUser(options.log_options, HighsLogType::kWarning,
                       "Col %6d has integer infeasibility of %11.4g\n",
                       (int)iCol, integer_infeasibility);
        num_integer_infeasibilities++;
      }
      max_integer_infeasibility =
          std::max(integer_infeasibility, max_integer_infeasibility);
      sum_integer_infeasibilities += integer_infeasibility;
    }
  }
  HighsStatus return_status =
      calculateRowValuesQuad(lp, solution.col_value, row_value);
  if (return_status != HighsStatus::kOk) return return_status;
  for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
    const double primal = solution.row_value[iRow];
    const double lower = lp.row_lower_[iRow];
    const double upper = lp.row_upper_[iRow];
    // @primal_infeasibility calculation
    double row_infeasibility = 0;
    if (primal < lower - kPrimalFeasibilityTolerance) {
      row_infeasibility = lower - primal;
    } else if (primal > upper + kPrimalFeasibilityTolerance) {
      row_infeasibility = primal - upper;
    }
    if (row_infeasibility > 0) {
      if (row_infeasibility > kPrimalFeasibilityTolerance) {
        if (row_infeasibility > 2 * max_row_infeasibility)
          highsLogUser(options.log_options, HighsLogType::kWarning,
                       "Row %6d has         infeasibility of %11.4g from "
                       "[lower, value, upper] = [%15.8g; %15.8g; %15.8g]\n",
                       (int)iRow, row_infeasibility, lower, primal, upper);
        num_row_infeasibilities++;
      }
      max_row_infeasibility =
          std::max(row_infeasibility, max_row_infeasibility);
      sum_row_infeasibilities += row_infeasibility;
    }
    double row_residual = fabs(primal - row_value[iRow]);
    if (row_residual > kRowResidualTolerance) {
      if (row_residual > 2 * max_row_residual) {
        highsLogUser(options.log_options, HighsLogType::kWarning,
                     "Row %6d has         residual      of %11.4g\n", (int)iRow,
                     row_residual);
      }
      num_row_residuals++;
    }
    max_row_residual = std::max(row_residual, max_row_residual);
    sum_row_residuals += row_residual;
  }
  highsLogUser(options.log_options, HighsLogType::kInfo,
               "Solution has               num          max          sum\n");
  highsLogUser(options.log_options, HighsLogType::kInfo,
               "Col     infeasibilities %6d  %11.4g  %11.4g\n",
               (int)num_col_infeasibilities, max_col_infeasibility,
               sum_col_infeasibilities);
  if (lp.isMip())
    highsLogUser(options.log_options, HighsLogType::kInfo,
                 "Integer infeasibilities %6d  %11.4g  %11.4g\n",
                 (int)num_integer_infeasibilities, max_integer_infeasibility,
                 sum_integer_infeasibilities);
  highsLogUser(options.log_options, HighsLogType::kInfo,
               "Row     infeasibilities %6d  %11.4g  %11.4g\n",
               (int)num_row_infeasibilities, max_row_infeasibility,
               sum_row_infeasibilities);
  highsLogUser(options.log_options, HighsLogType::kInfo,
               "Row     residuals       %6d  %11.4g  %11.4g\n",
               (int)num_row_residuals, max_row_residual, sum_row_residuals);
  valid = num_row_residuals == 0;
  integral = valid && num_integer_infeasibilities == 0;
  feasible = valid && num_col_infeasibilities == 0 &&
             num_integer_infeasibilities == 0 && num_row_infeasibilities == 0;
  if (!(integral && feasible)) return HighsStatus::kWarning;
  return HighsStatus::kOk;
}

void writeBasisFile(FILE*& file, const HighsBasis& basis) {
  fprintf(file, "HiGHS v%d\n", (int)HIGHS_VERSION_MAJOR);
  if (basis.valid == false) {
    fprintf(file, "None\n");
    return;
  }
  fprintf(file, "Valid\n");
  fprintf(file, "# Columns %d\n", (int)basis.col_status.size());
  for (const auto& status : basis.col_status) fprintf(file, "%d ", (int)status);
  fprintf(file, "\n");
  fprintf(file, "# Rows %d\n", (int)basis.row_status.size());
  for (const auto& status : basis.row_status) fprintf(file, "%d ", (int)status);
  fprintf(file, "\n");
}

HighsStatus readBasisFile(const HighsLogOptions& log_options, HighsBasis& basis,
                          const std::string filename) {
  // Opens a basis file as an ifstream
  HighsStatus return_status = HighsStatus::kOk;
  std::ifstream in_file;
  in_file.open(filename.c_str(), std::ios::in);
  if (in_file.is_open()) {
    return_status = readBasisStream(log_options, basis, in_file);
    in_file.close();
  } else {
    highsLogUser(log_options, HighsLogType::kError,
                 "readBasisFile: Cannot open readable file \"%s\"\n",
                 filename.c_str());
    return_status = HighsStatus::kError;
  }
  return return_status;
}

HighsStatus readBasisStream(const HighsLogOptions& log_options,
                            HighsBasis& basis, std::ifstream& in_file) {
  // Reads a basis as an ifstream, returning an error if what's read is
  // inconsistent with the sizes of the HighsBasis passed in
  HighsStatus return_status = HighsStatus::kOk;
  std::string string_highs, string_version;
  in_file >> string_highs >> string_version;
  if (string_version == "v1") {
    std::string keyword;
    in_file >> keyword;
    if (keyword == "None") {
      basis.valid = false;
      return HighsStatus::kOk;
    }
    const HighsInt basis_num_col = (HighsInt)basis.col_status.size();
    const HighsInt basis_num_row = (HighsInt)basis.row_status.size();
    HighsInt int_status;
    assert(keyword == "Valid");
    HighsInt num_col, num_row;
    // Read in the columns section
    in_file >> keyword >> keyword;
    assert(keyword == "Columns");
    in_file >> num_col;
    if (num_col != basis_num_col) {
      highsLogUser(log_options, HighsLogType::kError,
                   "readBasisFile: Basis file is for %" HIGHSINT_FORMAT
                   " columns, not %" HIGHSINT_FORMAT "\n",
                   num_col, basis_num_col);
      return HighsStatus::kError;
    }
    for (HighsInt iCol = 0; iCol < num_col; iCol++) {
      in_file >> int_status;
      basis.col_status[iCol] = (HighsBasisStatus)int_status;
    }
    // Read in the rows section
    in_file >> keyword >> keyword;
    assert(keyword == "Rows");
    in_file >> num_row;
    if (num_row != basis_num_row) {
      highsLogUser(log_options, HighsLogType::kError,
                   "readBasisFile: Basis file is for %" HIGHSINT_FORMAT
                   " rows, not %" HIGHSINT_FORMAT "\n",
                   num_row, basis_num_row);
      return HighsStatus::kError;
    }
    for (HighsInt iRow = 0; iRow < num_row; iRow++) {
      in_file >> int_status;
      basis.row_status[iRow] = (HighsBasisStatus)int_status;
    }
  } else {
    highsLogUser(log_options, HighsLogType::kError,
                 "readBasisFile: Cannot read basis file for HiGHS %s\n",
                 string_version.c_str());
    return_status = HighsStatus::kError;
  }
  return return_status;
}

HighsStatus calculateColDualsQuad(const HighsLp& lp, HighsSolution& solution) {
  const bool correct_size = int(solution.row_dual.size()) == lp.num_row_;
  const bool is_colwise = lp.a_matrix_.isColwise();
  const bool data_error = !correct_size || !is_colwise;
  assert(!data_error);
  if (data_error) return HighsStatus::kError;

  std::vector<HighsCDouble> col_dual_quad;
  col_dual_quad.assign(lp.num_col_, HighsCDouble{0.0});

  for (HighsInt col = 0; col < lp.num_col_; col++) {
    for (HighsInt i = lp.a_matrix_.start_[col];
         i < lp.a_matrix_.start_[col + 1]; i++) {
      const HighsInt row = lp.a_matrix_.index_[i];
      assert(row >= 0);
      assert(row < lp.num_row_);
      col_dual_quad[col] += solution.row_dual[row] * lp.a_matrix_.value_[i];
    }
    col_dual_quad[col] += lp.col_cost_[col];
  }

  // assign quad values to double vector
  solution.col_dual.resize(lp.num_col_);
  std::transform(col_dual_quad.begin(), col_dual_quad.end(),
                 solution.col_dual.begin(),
                 [](HighsCDouble x) { return double(x); });

  return HighsStatus::kOk;
}

HighsStatus calculateRowValuesQuad(const HighsLp& lp,
                                   const std::vector<double>& col_value,
                                   std::vector<double>& row_value,
                                   const HighsInt report_row) {
  const bool correct_size = int(col_value.size()) == lp.num_col_;
  const bool is_colwise = lp.a_matrix_.isColwise();
  const bool data_error = !correct_size || !is_colwise;
  assert(!data_error);
  if (data_error) return HighsStatus::kError;

  std::vector<HighsCDouble> row_value_quad;
  row_value_quad.assign(lp.num_row_, HighsCDouble{0.0});

  for (HighsInt col = 0; col < lp.num_col_; col++) {
    for (HighsInt i = lp.a_matrix_.start_[col];
         i < lp.a_matrix_.start_[col + 1]; i++) {
      const HighsInt row = lp.a_matrix_.index_[i];
      assert(row >= 0);
      assert(row < lp.num_row_);
      row_value_quad[row] += col_value[col] * lp.a_matrix_.value_[i];
      if (row == report_row) {
        printf(
            "calculateRowValuesQuad: Row %d becomes %g due to contribution of "
            ".col_value[%d] = %g\n",
            int(row), double(row_value_quad[row]), int(col), col_value[col]);
      }
    }
  }

  // assign quad values to double vector
  row_value.resize(lp.num_row_);
  std::transform(row_value_quad.begin(), row_value_quad.end(),
                 row_value.begin(), [](HighsCDouble x) { return double(x); });

  return HighsStatus::kOk;
}

HighsStatus calculateRowValuesQuad(const HighsLp& lp, HighsSolution& solution,
                                   const HighsInt report_row) {
  return calculateRowValuesQuad(lp, solution.col_value, solution.row_value,
                                report_row);
}

bool isColDataNull(const HighsLogOptions& log_options,
                   const double* usr_col_cost, const double* usr_col_lower,
                   const double* usr_col_upper) {
  bool null_data = false;
  null_data =
      doubleUserDataNotNull(log_options, usr_col_cost, "column costs") ||
      null_data;
  null_data = doubleUserDataNotNull(log_options, usr_col_lower,
                                    "column lower bounds") ||
              null_data;
  null_data = doubleUserDataNotNull(log_options, usr_col_upper,
                                    "column upper bounds") ||
              null_data;
  return null_data;
}

bool isRowDataNull(const HighsLogOptions& log_options,
                   const double* usr_row_lower, const double* usr_row_upper) {
  bool null_data = false;
  null_data =
      doubleUserDataNotNull(log_options, usr_row_lower, "row lower bounds") ||
      null_data;
  null_data =
      doubleUserDataNotNull(log_options, usr_row_upper, "row upper bounds") ||
      null_data;
  return null_data;
}

bool isMatrixDataNull(const HighsLogOptions& log_options,
                      const HighsInt* usr_matrix_start,
                      const HighsInt* usr_matrix_index,
                      const double* usr_matrix_value) {
  bool null_data = false;
  null_data =
      intUserDataNotNull(log_options, usr_matrix_start, "matrix starts") ||
      null_data;
  null_data =
      intUserDataNotNull(log_options, usr_matrix_index, "matrix indices") ||
      null_data;
  null_data =
      doubleUserDataNotNull(log_options, usr_matrix_value, "matrix values") ||
      null_data;
  return null_data;
}

void reportPresolveReductions(const HighsLogOptions& log_options,
                              const HighsLp& lp, const HighsLp& presolve_lp) {
  HighsInt num_col_from = lp.num_col_;
  HighsInt num_row_from = lp.num_row_;
  HighsInt num_els_from = lp.a_matrix_.start_[num_col_from];
  HighsInt num_col_to = presolve_lp.num_col_;
  HighsInt num_row_to = presolve_lp.num_row_;
  HighsInt num_els_to;
  if (num_col_to) {
    num_els_to = presolve_lp.a_matrix_.start_[num_col_to];
  } else {
    num_els_to = 0;
  }
  char elemsignchar = '-';
  HighsInt elemdelta = num_els_from - num_els_to;
  if (num_els_from < num_els_to) {
    elemdelta = -elemdelta;
    elemsignchar = '+';
  }
  highsLogUser(
      log_options, HighsLogType::kInfo,
      "Presolve : Reductions: rows %" HIGHSINT_FORMAT "(-%" HIGHSINT_FORMAT
      "); columns %" HIGHSINT_FORMAT "(-%" HIGHSINT_FORMAT
      "); "
      "elements %" HIGHSINT_FORMAT "(%c%" HIGHSINT_FORMAT ")\n",
      num_row_to, (num_row_from - num_row_to), num_col_to,
      (num_col_from - num_col_to), num_els_to, elemsignchar, elemdelta);
}

void reportPresolveReductions(const HighsLogOptions& log_options,
                              const HighsLp& lp, const bool presolve_to_empty) {
  HighsInt num_col_from = lp.num_col_;
  HighsInt num_row_from = lp.num_row_;
  HighsInt num_els_from = lp.a_matrix_.start_[num_col_from];
  HighsInt num_col_to;
  HighsInt num_row_to;
  HighsInt num_els_to;
  std::string message;
  if (presolve_to_empty) {
    num_col_to = 0;
    num_row_to = 0;
    num_els_to = 0;
    message = "- Reduced to empty";
  } else {
    num_col_to = num_col_from;
    num_row_to = num_row_from;
    num_els_to = num_els_from;
    message = "- Not reduced";
  }
  highsLogUser(log_options, HighsLogType::kInfo,
               "Presolve : Reductions: rows %" HIGHSINT_FORMAT
               "(-%" HIGHSINT_FORMAT "); columns %" HIGHSINT_FORMAT
               "(-%" HIGHSINT_FORMAT
               "); "
               "elements %" HIGHSINT_FORMAT "(-%" HIGHSINT_FORMAT ") %s\n",
               num_row_to, (num_row_from - num_row_to), num_col_to,
               (num_col_from - num_col_to), num_els_to,
               (num_els_from - num_els_to), message.c_str());
}

bool isLessInfeasibleDSECandidate(const HighsLogOptions& log_options,
                                  const HighsLp& lp) {
  HighsInt max_col_num_en = -1;
  const HighsInt max_allowed_col_num_en = 24;
  const HighsInt max_assess_col_num_en =
      std::max(HighsInt{9}, max_allowed_col_num_en);
  const HighsInt max_average_col_num_en = 6;
  vector<HighsInt> col_length_k;
  col_length_k.resize(1 + max_assess_col_num_en, 0);
  bool LiDSE_candidate = true;
  for (HighsInt col = 0; col < lp.num_col_; col++) {
    // Check limit on number of entries in the column has not been breached
    HighsInt col_num_en =
        lp.a_matrix_.start_[col + 1] - lp.a_matrix_.start_[col];
    max_col_num_en = std::max(col_num_en, max_col_num_en);
    if (col_num_en > max_assess_col_num_en) return false;
    col_length_k[col_num_en]++;
    for (HighsInt en = lp.a_matrix_.start_[col];
         en < lp.a_matrix_.start_[col + 1]; en++) {
      double value = lp.a_matrix_.value_[en];
      // All nonzeros must be +1 or -1
      if (fabs(value) != 1) return false;
    }
  }
  double average_col_num_en = lp.a_matrix_.start_[lp.num_col_];
  average_col_num_en = average_col_num_en / lp.num_col_;
  LiDSE_candidate =
      LiDSE_candidate && average_col_num_en <= max_average_col_num_en;
  highsLogDev(log_options, HighsLogType::kInfo,
              "LP %s has all |entries|=1; max column count = %" HIGHSINT_FORMAT
              " (limit %" HIGHSINT_FORMAT
              "); average "
              "column count = %0.2g (limit %" HIGHSINT_FORMAT
              "): LP is %s a candidate for LiDSE\n",
              lp.model_name_.c_str(), max_col_num_en, max_allowed_col_num_en,
              average_col_num_en, max_average_col_num_en,
              LiDSE_candidate ? "is" : "is not");
  return LiDSE_candidate;
}

HighsLp withoutSemiVariables(const HighsLp& lp_, HighsSolution& solution,
                             const double primal_feasibility_tolerance) {
  HighsLp lp = lp_;
  HighsInt num_col = lp.num_col_;
  HighsInt num_row = lp.num_row_;
  HighsInt num_semi_variables = 0;
  for (HighsInt iCol = 0; iCol < num_col; iCol++) {
    if (lp.integrality_[iCol] == HighsVarType::kSemiContinuous ||
        lp.integrality_[iCol] == HighsVarType::kSemiInteger)
      num_semi_variables++;
  }
  assert(num_semi_variables);
  // Insert spaces for index/value of new coefficients for
  // semi-variables
  vector<HighsInt>& start = lp.a_matrix_.start_;
  vector<HighsInt>& index = lp.a_matrix_.index_;
  vector<double>& value = lp.a_matrix_.value_;
  HighsInt num_nz = start[num_col];
  HighsInt new_num_nz = num_nz + 2 * num_semi_variables;
  HighsInt new_el = new_num_nz;
  index.resize(new_num_nz);
  value.resize(new_num_nz);
  for (HighsInt iCol = num_col - 1; iCol >= 0; iCol--) {
    HighsInt from_el = start[iCol + 1] - 1;
    start[iCol + 1] = new_el;
    if (lp.integrality_[iCol] == HighsVarType::kSemiContinuous ||
        lp.integrality_[iCol] == HighsVarType::kSemiInteger)
      new_el -= 2;
    for (HighsInt iEl = from_el; iEl >= start[iCol]; iEl--) {
      new_el--;
      index[new_el] = index[iEl];
      value[new_el] = value[iEl];
    }
  }
  assert(new_el == 0);
  // Insert the new coefficients for semi-variables
  HighsInt row_num = num_row;
  for (HighsInt iCol = 0; iCol < num_col; iCol++) {
    if (lp.integrality_[iCol] == HighsVarType::kSemiContinuous ||
        lp.integrality_[iCol] == HighsVarType::kSemiInteger) {
      HighsInt iEl = start[iCol + 1] - 2;
      index[iEl] = row_num++;
      value[iEl] = 1;
      iEl++;
      index[iEl] = row_num++;
      value[iEl] = 1;
    }
  }
  num_nz = start[num_col];
  new_num_nz = num_nz + 2 * num_semi_variables;
  row_num = num_row;
  HighsInt semi_col_num = 0;
  HighsInt semi_row_num = 0;
  // Insert the new variables and their coefficients
  std::stringstream ss;
  const bool has_col_names = (lp.col_names_.size() != 0);
  const bool has_row_names = (lp.row_names_.size() != 0);
  const bool has_solution = solution.value_valid;
  if (has_solution) {
    // Create zeroed row values for the new rows
    assert((HighsInt)solution.row_value.size() == lp_.num_row_);
    for (HighsInt iCol = 0; iCol < 2 * num_semi_variables; iCol++)
      solution.row_value.push_back(0);
    assert((HighsInt)solution.col_value.size() == lp_.num_col_);
    assert((HighsInt)solution.row_value.size() ==
           lp_.num_row_ + 2 * num_semi_variables);
  }
  for (HighsInt iCol = 0; iCol < num_col; iCol++) {
    if (lp.integrality_[iCol] == HighsVarType::kSemiContinuous ||
        lp.integrality_[iCol] == HighsVarType::kSemiInteger) {
      // Add a binary variable with zero cost
      lp.col_cost_.push_back(0);
      lp.col_lower_.push_back(0);
      lp.col_upper_.push_back(1);
      // Complete x - l*y >= 0
      lp.row_lower_.push_back(0);
      lp.row_upper_.push_back(kHighsInf);
      if (has_col_names) {
        // Create a column name
        ss.str(std::string());
        ss << "semi_binary_" << semi_col_num++;
        lp.col_names_.push_back(ss.str());
      }
      if (has_row_names) {
        // Create a row name
        ss.str(std::string());
        ss << "semi_lb_" << semi_row_num;
        lp.row_names_.push_back(ss.str());
      }
      index.push_back(row_num++);
      value.push_back(-lp.col_lower_[iCol]);
      // Accommodate any primal solution
      if (has_solution) {
        // Record the previous solution value so any change can be
        // determined
        const double prev_primal = solution.col_value[iCol];
        if (solution.col_value[iCol] <= primal_feasibility_tolerance) {
          // Currently at or below zero, so binary is 0
          solution.col_value[iCol] = 0;
          solution.col_value.push_back(0);
        } else {
          // Otherwise, solution is at least lower bound, and binary
          // is 1
          solution.col_value[iCol] =
              std::max(lp.col_lower_[iCol], solution.col_value[iCol]);
          solution.col_value.push_back(1);
        }
        const double dl_primal = solution.col_value[iCol] - prev_primal;
        if (dl_primal) {
          // Change in primal value, so update row values. NB start
          // has been extended to incorporate the values in this
          // column for the new rows. Their solution values are zero,
          // but will be set later
          for (HighsInt iEl = start[iCol]; iEl < start[iCol + 1]; iEl++)
            solution.row_value[index[iEl]] += dl_primal * value[iEl];
        }
        const HighsInt new_col = lp.col_cost_.size() - 1;
        const double binary_value = solution.col_value[new_col];
        solution.row_value[row_num - 1] =
            solution.col_value[iCol] - lp.col_lower_[iCol] * binary_value;
        solution.row_value[row_num] =
            solution.col_value[iCol] - lp.col_upper_[iCol] * binary_value;
      }
      // Complete x - u*y <= 0
      lp.row_lower_.push_back(-kHighsInf);
      lp.row_upper_.push_back(0);
      if (has_row_names) {
        // Create a row name
        ss.str(std::string());
        ss << "semi_ub_" << semi_row_num++;
        lp.row_names_.push_back(ss.str());
      }
      index.push_back(row_num++);
      value.push_back(-lp.col_upper_[iCol]);
      // Add the next start
      start.push_back(index.size());
      lp.integrality_.push_back(HighsVarType::kInteger);
      if (lp.integrality_[iCol] == HighsVarType::kSemiContinuous) {
        lp.integrality_[iCol] = HighsVarType::kContinuous;
      } else if (lp.integrality_[iCol] == HighsVarType::kSemiInteger) {
        lp.integrality_[iCol] = HighsVarType::kInteger;
      }
      // Change the lower bound on the semi-variable to zero. Cannot
      // do this earlier, as its original value is used in constraint
      // 0 <= x-l*y
      lp.col_lower_[iCol] = 0;
    }
  }
  num_col += num_semi_variables;
  lp.num_col_ += num_semi_variables;
  lp.num_row_ += 2 * num_semi_variables;
  assert((HighsInt)index.size() == new_num_nz);
  // Clear any modifications inherited from lp_
  lp.mods_.clear();
  return lp;
}

void removeRowsOfCountOne(const HighsLogOptions& log_options, HighsLp& lp) {
  HighsLp row_wise_lp = lp;
  vector<HighsInt>& a_start = lp.a_matrix_.start_;
  vector<HighsInt>& a_index = lp.a_matrix_.index_;
  vector<double>& a_value = lp.a_matrix_.value_;
  vector<HighsInt> a_count;
  vector<HighsInt> ar_count;
  vector<HighsInt> ar_start;
  vector<HighsInt> ar_index;
  vector<double> ar_value;
  const bool has_name = lp.row_names_.size() > 0;
  HighsInt num_nz = a_start[lp.num_col_];
  const HighsInt original_num_nz = num_nz;
  const HighsInt original_num_row = lp.num_row_;
  HighsInt num_row_count_1 = 0;
  ar_count.assign(lp.num_row_, 0);
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    for (HighsInt iEl = a_start[iCol]; iEl < a_start[iCol + 1]; iEl++)
      ar_count[a_index[iEl]]++;
  }
  ar_start.push_back(0);
  for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
    ar_start.push_back(ar_start[iRow] + ar_count[iRow]);
    ar_count[iRow] = ar_start[iRow];
  }
  ar_index.resize(num_nz);
  ar_value.resize(num_nz);
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    for (HighsInt iEl = a_start[iCol]; iEl < a_start[iCol + 1]; iEl++) {
      HighsInt iRow = a_index[iEl];
      ar_index[ar_count[iRow]] = iCol;
      ar_value[ar_count[iRow]] = a_value[iEl];
      ar_count[iRow]++;
    }
  }
  HighsInt newRow = 0;
  HighsInt newEl = 0;
  for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
    const HighsInt row_count = ar_start[iRow + 1] - ar_start[iRow];
    if (row_count == 1) {
      HighsInt iCol = ar_index[ar_start[iRow]];
      double value = ar_value[ar_start[iRow]];
      assert(value);
      if (value > 0) {
        if (lp.row_lower_[iRow] > -kHighsInf)
          lp.col_lower_[iCol] =
              std::max(lp.row_lower_[iRow] / value, lp.col_lower_[iCol]);
        if (lp.row_upper_[iRow] < kHighsInf)
          lp.col_upper_[iCol] =
              std::min(lp.row_upper_[iRow] / value, lp.col_upper_[iCol]);
      } else {
        if (lp.row_lower_[iRow] > -kHighsInf)
          lp.col_upper_[iCol] =
              std::min(lp.row_lower_[iRow] / value, lp.col_upper_[iCol]);
        if (lp.row_upper_[iRow] < kHighsInf)
          lp.col_lower_[iCol] =
              std::max(lp.row_upper_[iRow] / value, lp.col_lower_[iCol]);
      }
      num_row_count_1++;
      continue;
    }
    lp.row_lower_[newRow] = lp.row_lower_[iRow];
    lp.row_upper_[newRow] = lp.row_upper_[iRow];
    if (has_name) lp.row_names_[newRow] = lp.row_names_[iRow];
    ar_start[newRow] = newEl;
    for (HighsInt iEl = ar_start[iRow]; iEl < ar_start[iRow + 1]; iEl++) {
      ar_index[newEl] = ar_index[iEl];
      ar_value[newEl] = ar_value[iEl];
      newEl++;
    }
    newRow++;
  }
  ar_start[newRow] = newEl;
  lp.num_row_ = newRow;
  lp.row_lower_.resize(newRow);
  lp.row_upper_.resize(newRow);
  if (has_name) lp.row_names_.resize(newRow);

  num_nz = ar_start[lp.num_row_];
  a_count.assign(lp.num_col_, 0);
  for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
    for (HighsInt iEl = ar_start[iRow]; iEl < ar_start[iRow + 1]; iEl++)
      a_count[ar_index[iEl]]++;
  }
  a_start[0] = 0;
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    a_start[iCol + 1] = a_start[iCol] + a_count[iCol];
    a_count[iCol] = a_start[iCol];
  }
  a_index.resize(num_nz);
  a_value.resize(num_nz);
  for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
    for (HighsInt iEl = ar_start[iRow]; iEl < ar_start[iRow + 1]; iEl++) {
      HighsInt iCol = ar_index[iEl];
      a_index[a_count[iCol]] = iRow;
      a_value[a_count[iCol]] = ar_value[iEl];
      a_count[iCol]++;
    }
  }
  assert(original_num_row - lp.num_row_ == num_row_count_1);
  assert(original_num_nz - num_nz == num_row_count_1);
  highsLogUser(log_options, HighsLogType::kWarning,
               "Removed %d rows of count 1\n", (int)num_row_count_1);
}
