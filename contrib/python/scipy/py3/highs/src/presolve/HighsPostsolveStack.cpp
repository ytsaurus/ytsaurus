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
#include "presolve/HighsPostsolveStack.h"

#include <numeric>

#include "lp_data/HConst.h"
#include "lp_data/HighsModelUtils.h"  // For debugging #2001
#include "lp_data/HighsOptions.h"
#include "util/HighsCDouble.h"
#include "util/HighsUtils.h"

namespace presolve {

void HighsPostsolveStack::initializeIndexMaps(HighsInt numRow,
                                              HighsInt numCol) {
  origNumRow = numRow;
  origNumCol = numCol;

  origRowIndex.resize(numRow);
  std::iota(origRowIndex.begin(), origRowIndex.end(), 0);

  origColIndex.resize(numCol);
  std::iota(origColIndex.begin(), origColIndex.end(), 0);

  linearlyTransformable.resize(numCol, true);
}

void HighsPostsolveStack::compressIndexMaps(
    const std::vector<HighsInt>& newRowIndex,
    const std::vector<HighsInt>& newColIndex) {
  // loop over rows, decrease row counter for deleted rows (marked with -1),
  // store original index at new index position otherwise
  HighsInt numRow = origRowIndex.size();
  for (size_t i = 0; i != newRowIndex.size(); ++i) {
    if (newRowIndex[i] == -1)
      --numRow;
    else
      origRowIndex[newRowIndex[i]] = origRowIndex[i];
  }
  // resize original index array to new size
  origRowIndex.resize(numRow);

  // now compress the column array
  HighsInt numCol = origColIndex.size();
  for (size_t i = 0; i != newColIndex.size(); ++i) {
    if (newColIndex[i] == -1)
      --numCol;
    else
      origColIndex[newColIndex[i]] = origColIndex[i];
  }
  origColIndex.resize(numCol);
}

void HighsPostsolveStack::LinearTransform::undo(const HighsOptions& options,
                                                HighsSolution& solution) const {
  solution.col_value[col] *= scale;
  solution.col_value[col] += constant;

  if (solution.dual_valid) solution.col_dual[col] /= scale;
}

void HighsPostsolveStack::LinearTransform::transformToPresolvedSpace(
    std::vector<double>& primalSol) const {
  primalSol[col] -= constant;
  primalSol[col] /= scale;
}

static HighsBasisStatus computeRowStatus(double dual,
                                         HighsPostsolveStack::RowType rowType) {
  if (rowType == HighsPostsolveStack::RowType::kEq)
    return dual < 0 ? HighsBasisStatus::kUpper : HighsBasisStatus::kLower;
  else if (rowType == HighsPostsolveStack::RowType::kGeq)
    return HighsBasisStatus::kLower;
  else
    return HighsBasisStatus::kUpper;
}

void HighsPostsolveStack::FreeColSubstitution::undo(
    const HighsOptions& options, const std::vector<Nonzero>& rowValues,
    const std::vector<Nonzero>& colValues, HighsSolution& solution,
    HighsBasis& basis) {
  // a (removed) cut may have been used in this reduction.
  bool isModelRow = static_cast<size_t>(row) < solution.row_value.size();

  // compute primal values
  double colCoef = 0;
  HighsCDouble rowValue = 0;
  for (const auto& rowVal : rowValues) {
    if (rowVal.index == col)
      colCoef = rowVal.value;
    else
      rowValue += rowVal.value * solution.col_value[rowVal.index];
  }

  assert(colCoef != 0);
  // Row values aren't fully postsolved, so why do this?
  if (isModelRow)
    solution.row_value[row] =
        double(rowValue + colCoef * solution.col_value[col]);
  solution.col_value[col] = double((rhs - rowValue) / colCoef);

  // if no dual values requested, return here
  if (!solution.dual_valid) return;

  // compute the row dual value such that reduced cost of basic column is 0
  if (isModelRow) {
    solution.row_dual[row] = 0;
    HighsCDouble dualval = colCost;
    for (const auto& colVal : colValues) {
      if (static_cast<size_t>(colVal.index) < solution.row_dual.size())
        dualval -= colVal.value * solution.row_dual[colVal.index];
    }
    solution.row_dual[row] = double(dualval / colCoef);
  }

  solution.col_dual[col] = 0;

  // set basis status if necessary
  if (!basis.valid) return;

  basis.col_status[col] = HighsBasisStatus::kBasic;
  if (isModelRow)
    basis.row_status[row] = computeRowStatus(solution.row_dual[row], rowType);
}

static HighsBasisStatus computeStatus(double dual, HighsBasisStatus& status,
                                      double dual_feasibility_tolerance) {
  if (dual > dual_feasibility_tolerance)
    status = HighsBasisStatus::kLower;
  else if (dual < -dual_feasibility_tolerance)
    status = HighsBasisStatus::kUpper;

  return status;
}

static HighsBasisStatus computeStatus(double dual,
                                      double dual_feasibility_tolerance) {
  if (dual > dual_feasibility_tolerance)
    return HighsBasisStatus::kLower;
  else if (dual < -dual_feasibility_tolerance)
    return HighsBasisStatus::kUpper;
  else
    return HighsBasisStatus::kBasic;
}

void HighsPostsolveStack::DoubletonEquation::undo(
    const HighsOptions& options, const std::vector<Nonzero>& colValues,
    HighsSolution& solution, HighsBasis& basis) const {
  // retrieve the row and column index, the row side and the two
  // coefficients then compute the primal values
  solution.col_value[colSubst] =
      double((rhs - HighsCDouble(coef) * solution.col_value[col]) / coefSubst);

  // can only do primal postsolve, stop here
  if (row == -1 || !solution.dual_valid) return;

  const HighsBasisStatus colStatus =
      !basis.valid
          ? computeStatus(solution.col_dual[col],
                          options.dual_feasibility_tolerance)
          : computeStatus(solution.col_dual[col], basis.col_status[col],
                          options.dual_feasibility_tolerance);

  // a (removed) cut may have been used in this reduction.
  bool isModelRow = static_cast<size_t>(row) < solution.row_value.size();

  // compute the current dual values of the row and the substituted column
  // before deciding on which column becomes basic
  // for each entry in a row i of the substituted column we added the
  // doubleton equation row with scale -a_i/substCoef. Therefore the dual
  // multiplier of this row i implicitly increases the dual multiplier of this
  // doubleton equation row with that scale.
  HighsCDouble rowDual = 0.0;
  if (isModelRow) {
    solution.row_dual[row] = 0;
    for (const auto& colVal : colValues) {
      if (static_cast<size_t>(colVal.index) < solution.row_dual.size())
        rowDual -= colVal.value * solution.row_dual[colVal.index];
    }
    rowDual /= coefSubst;
    solution.row_dual[row] = double(rowDual);
  }
  // the equation was also added to the objective, so the current values need
  // to be changed
  solution.col_dual[colSubst] = substCost;
  solution.col_dual[col] += substCost * coef / coefSubst;

  if ((upperTightened && colStatus == HighsBasisStatus::kUpper) ||
      (lowerTightened && colStatus == HighsBasisStatus::kLower)) {
    // column must get zero reduced cost as the current bound cannot be used
    // so alter the dual multiplier of the row to make the dual multiplier of
    // column zero
    double rowDualDelta = solution.col_dual[col] / coef;
    if (isModelRow) solution.row_dual[row] = double(rowDual + rowDualDelta);
    solution.col_dual[col] = 0.0;
    solution.col_dual[colSubst] = double(
        HighsCDouble(solution.col_dual[colSubst]) - rowDualDelta * coefSubst);

    if (basis.valid) {
      if ((std::signbit(coef) == std::signbit(coefSubst) &&
           basis.col_status[col] == HighsBasisStatus::kUpper) ||
          (std::signbit(coef) != std::signbit(coefSubst) &&
           basis.col_status[col] == HighsBasisStatus::kLower))
        basis.col_status[colSubst] = HighsBasisStatus::kLower;
      else
        basis.col_status[colSubst] = HighsBasisStatus::kUpper;
      basis.col_status[col] = HighsBasisStatus::kBasic;
    }
  } else {
    // otherwise make the reduced cost of the substituted column zero and make
    // that column basic
    double rowDualDelta = solution.col_dual[colSubst] / coefSubst;
    if (isModelRow) solution.row_dual[row] = double(rowDual + rowDualDelta);
    solution.col_dual[colSubst] = 0.0;
    solution.col_dual[col] =
        double(HighsCDouble(solution.col_dual[col]) - rowDualDelta * coef);
    if (basis.valid) basis.col_status[colSubst] = HighsBasisStatus::kBasic;
  }

  if (!basis.valid) return;

  if (isModelRow)
    basis.row_status[row] = computeRowStatus(solution.row_dual[row], rowType);
}

void HighsPostsolveStack::EqualityRowAddition::undo(
    const HighsOptions& options, const std::vector<Nonzero>& eqRowValues,
    HighsSolution& solution, HighsBasis& basis) const {
  // (removed) cuts may have been used in this reduction.
  if (static_cast<size_t>(row) >= solution.row_value.size() ||
      static_cast<size_t>(addedEqRow) >= solution.row_value.size())
    return;

  // nothing more to do if the row is zero in the dual solution or there is
  // no dual solution
  if (!solution.dual_valid || solution.row_dual[row] == 0.0) return;

  // the dual multiplier of the row implicitly increases the dual multiplier
  // of the equation with the scale the equation was added with
  solution.row_dual[addedEqRow] =
      double(HighsCDouble(eqRowScale) * solution.row_dual[row] +
             solution.row_dual[addedEqRow]);

  assert(!basis.valid);
}

void HighsPostsolveStack::EqualityRowAdditions::undo(
    const HighsOptions& options, const std::vector<Nonzero>& eqRowValues,
    const std::vector<Nonzero>& targetRows, HighsSolution& solution,
    HighsBasis& basis) const {
  // a (removed) cut may have been used in this reduction.
  if (static_cast<size_t>(addedEqRow) >= solution.row_value.size()) return;

  // nothing more to do if the row is zero in the dual solution or there is
  // no dual solution
  if (!solution.dual_valid) return;

  // the dual multiplier of the rows where the eq row was added implicitly
  // increases the dual multiplier of the equation with the scale that was
  // used for adding the equation
  HighsCDouble eqRowDual = solution.row_dual[addedEqRow];
  for (const auto& targetRow : targetRows) {
    if (static_cast<size_t>(targetRow.index) < solution.row_dual.size())
      eqRowDual +=
          HighsCDouble(targetRow.value) * solution.row_dual[targetRow.index];
  }
  solution.row_dual[addedEqRow] = double(eqRowDual);

  assert(!basis.valid);
}

void HighsPostsolveStack::ForcingColumn::undo(
    const HighsOptions& options, const std::vector<Nonzero>& colValues,
    HighsSolution& solution, HighsBasis& basis) const {
  HighsInt nonbasicRow = -1;
  HighsBasisStatus nonbasicRowStatus = HighsBasisStatus::kNonbasic;
  double colValFromNonbasicRow = colBound;

  HighsInt debug_num_use_row_value = 0;
  const bool debug_report = false;

  auto computeColVal = [&](HighsInt direction) {
    // choose solution value that makes all rows feasible
    for (const auto& colVal : colValues) {
      // Row values aren't fully postsolved, so how can this work?
      debug_num_use_row_value++;
      if (static_cast<size_t>(colVal.index) < solution.row_value.size()) {
        double colValFromRow = solution.row_value[colVal.index] / colVal.value;
        if (direction * colValFromRow > direction * colValFromNonbasicRow) {
          nonbasicRow = colVal.index;
          colValFromNonbasicRow = colValFromRow;
          nonbasicRowStatus = direction * colVal.value > 0
                                  ? HighsBasisStatus::kLower
                                  : HighsBasisStatus::kUpper;
        }
      }
    }
    // round solution value if column is integer-constrained
    if (nonbasicRow != -1 && colIntegral)
      colValFromNonbasicRow =
          direction * std::ceil(direction * colValFromNonbasicRow -
                                options.mip_feasibility_tolerance);
  };

  if (atInfiniteUpper) {
    // choose largest value as then all rows are feasible
    computeColVal(HighsInt{1});
  } else {
    // choose smallest value, as then all rows are feasible
    computeColVal(HighsInt{-1});
  }
  if (debug_num_use_row_value && debug_report) {
    printf(
        "HighsPostsolveStack::ForcingColumn::undo Using %d unknown row "
        "activit%s\n",
        int(debug_num_use_row_value),
        debug_num_use_row_value > 1 ? "ies" : "y");
  }

  solution.col_value[col] = colValFromNonbasicRow;

  if (!solution.dual_valid) return;

  solution.col_dual[col] = 0.0;

  if (!basis.valid) return;
  if (nonbasicRow == -1) {
    basis.col_status[col] =
        atInfiniteUpper ? HighsBasisStatus::kLower : HighsBasisStatus::kUpper;
  } else {
    basis.col_status[col] = HighsBasisStatus::kBasic;
    basis.row_status[nonbasicRow] = nonbasicRowStatus;
  }
}

void HighsPostsolveStack::ForcingColumnRemovedRow::undo(
    const HighsOptions& options, const std::vector<Nonzero>& rowValues,
    HighsSolution& solution, HighsBasis& basis) const {
  // a (removed) cut may have been used in this reduction.
  if (static_cast<size_t>(row) >= solution.row_value.size()) return;

  // we use the row value as storage for the scaled value implied on the
  // column dual
  HighsCDouble val = rhs;
  for (const auto& rowVal : rowValues)
    val -= rowVal.value * solution.col_value[rowVal.index];

  // Row values aren't fully postsolved, so why do this?
  solution.row_value[row] = double(val);

  if (solution.dual_valid) solution.row_dual[row] = 0.0;
  if (basis.valid) basis.row_status[row] = HighsBasisStatus::kBasic;
}

void HighsPostsolveStack::SingletonRow::undo(const HighsOptions& options,
                                             HighsSolution& solution,
                                             HighsBasis& basis) const {
  // a (removed) cut may have been used in this reduction.
  bool isModelRow = static_cast<size_t>(row) < solution.row_value.size();

  // nothing to do if the rows dual value is zero in the dual solution or
  // there is no dual solution
  if (!solution.dual_valid) return;

  const HighsBasisStatus colStatus =
      !basis.valid
          ? computeStatus(solution.col_dual[col],
                          options.dual_feasibility_tolerance)
          : computeStatus(solution.col_dual[col], basis.col_status[col],
                          options.dual_feasibility_tolerance);

  if ((!colLowerTightened || colStatus != HighsBasisStatus::kLower) &&
      (!colUpperTightened || colStatus != HighsBasisStatus::kUpper)) {
    // the tightened bound is not used in the basic solution
    // hence we simply make the row basic and give it a dual multiplier of 0
    if (isModelRow) {
      if (basis.valid) basis.row_status[row] = HighsBasisStatus::kBasic;
      solution.row_dual[row] = 0;
    }
    return;
  }

  // choose the row dual value such that the columns reduced cost becomes
  // zero
  if (isModelRow) solution.row_dual[row] = solution.col_dual[col] / coef;
  solution.col_dual[col] = 0;

  if (!basis.valid) return;

  if (isModelRow) {
    switch (colStatus) {
      case HighsBasisStatus::kLower:
        assert(colLowerTightened);
        if (coef > 0)
          // tightened lower bound comes from row lower bound
          basis.row_status[row] = HighsBasisStatus::kLower;
        else
          // tightened lower bound comes from row upper bound
          basis.row_status[row] = HighsBasisStatus::kUpper;

        break;
      case HighsBasisStatus::kUpper:
        if (coef > 0)
          // tightened upper bound comes from row lower bound
          basis.row_status[row] = HighsBasisStatus::kUpper;
        else
          // tightened lower bound comes from row upper bound
          basis.row_status[row] = HighsBasisStatus::kLower;
        break;
      default:
        assert(false);
    }
  }

  // column becomes basic
  basis.col_status[col] = HighsBasisStatus::kBasic;
}

// column fixed to lower or upper bound
void HighsPostsolveStack::FixedCol::undo(const HighsOptions& options,
                                         const std::vector<Nonzero>& colValues,
                                         HighsSolution& solution,
                                         HighsBasis& basis) const {
  // set solution value
  solution.col_value[col] = fixValue;

  if (!solution.dual_valid) return;

  // compute reduced cost

  HighsCDouble reducedCost = colCost;
  for (const auto& colVal : colValues) {
    if (static_cast<size_t>(colVal.index) < solution.row_dual.size())
      reducedCost -= colVal.value * solution.row_dual[colVal.index];
  }

  solution.col_dual[col] = double(reducedCost);

  // set basis status
  if (basis.valid) {
    basis.col_status[col] = fixType;
    if (basis.col_status[col] == HighsBasisStatus::kNonbasic)
      basis.col_status[col] = solution.col_dual[col] >= 0
                                  ? HighsBasisStatus::kLower
                                  : HighsBasisStatus::kUpper;
  }
}

void HighsPostsolveStack::RedundantRow::undo(const HighsOptions& options,
                                             HighsSolution& solution,
                                             HighsBasis& basis) const {
  // a (removed) cut may have been used in this reduction.
  if (static_cast<size_t>(row) >= solution.row_value.size()) return;

  // set row dual to zero if dual solution requested
  if (!solution.dual_valid) return;

  solution.row_dual[row] = 0.0;

  if (basis.valid) basis.row_status[row] = HighsBasisStatus::kBasic;
}

void HighsPostsolveStack::ForcingRow::undo(
    const HighsOptions& options, const std::vector<Nonzero>& rowValues,
    HighsSolution& solution, HighsBasis& basis) const {
  if (!solution.dual_valid) return;

  // compute the row dual multiplier and determine the new basic column
  HighsInt basicCol = -1;
  double dualDelta = 0;
  HighsInt direction = rowType == RowType::kLeq ? 1 : -1;
  for (const auto& rowVal : rowValues) {
    double colDual = solution.col_dual[rowVal.index] - rowVal.value * dualDelta;
    if (direction * colDual * rowVal.value < 0) {
      // column is dual infeasible, decrease the row dual such that its
      // reduced cost become zero and remember this column as the new basic
      // column for this row
      dualDelta = solution.col_dual[rowVal.index] / rowVal.value;
      basicCol = rowVal.index;
    }
  }

  if (basicCol != -1) {
    bool isModelRow = static_cast<size_t>(row) < solution.row_dual.size();
    if (isModelRow) solution.row_dual[row] = solution.row_dual[row] + dualDelta;
    for (const auto& rowVal : rowValues) {
      solution.col_dual[rowVal.index] =
          double(solution.col_dual[rowVal.index] -
                 HighsCDouble(dualDelta) * rowVal.value);
    }
    solution.col_dual[basicCol] = 0;

    if (basis.valid) {
      if (isModelRow)
        basis.row_status[row] =
            (rowType == RowType::kGeq ? HighsBasisStatus::kLower
                                      : HighsBasisStatus::kUpper);

      basis.col_status[basicCol] = HighsBasisStatus::kBasic;
    }
  }
}

void HighsPostsolveStack::DuplicateRow::undo(const HighsOptions& options,
                                             HighsSolution& solution,
                                             HighsBasis& basis) const {
  // (removed) cuts may have been used in this reduction.
  if (static_cast<size_t>(row) >= solution.row_value.size()) return;
  bool duplicateIsModelRow =
      static_cast<size_t>(duplicateRow) < solution.row_value.size();

  if (!solution.dual_valid) return;
  if (!rowUpperTightened && !rowLowerTightened) {
    // simple case of row2 being redundant, in which case it just gets a
    // dual multiplier of 0 and is made basic
    if (duplicateIsModelRow) {
      solution.row_dual[duplicateRow] = 0.0;
      if (basis.valid)
        basis.row_status[duplicateRow] = HighsBasisStatus::kBasic;
    }
    return;
  }

  const HighsBasisStatus rowStatus =
      !basis.valid
          ? computeStatus(solution.row_dual[row],
                          options.dual_feasibility_tolerance)
          : computeStatus(solution.row_dual[row], basis.row_status[row],
                          options.dual_feasibility_tolerance);

  auto computeRowDualAndStatus = [&](bool tighened) {
    if (tighened) {
      if (duplicateIsModelRow) {
        solution.row_dual[duplicateRow] =
            solution.row_dual[row] / duplicateRowScale;
        if (basis.valid) {
          if (duplicateRowScale > 0)
            basis.row_status[duplicateRow] = HighsBasisStatus::kUpper;
          else
            basis.row_status[duplicateRow] = HighsBasisStatus::kLower;
        }
      }
      solution.row_dual[row] = 0.0;
      if (basis.valid) basis.row_status[row] = HighsBasisStatus::kBasic;
    } else if (duplicateIsModelRow) {
      solution.row_dual[duplicateRow] = 0.0;
      if (basis.valid)
        basis.row_status[duplicateRow] = HighsBasisStatus::kBasic;
    }
  };

  // at least one bound of the row was tightened by using the bound of the
  // scaled parallel row, hence we might need to make the parallel row
  // nonbasic and the row basic

  switch (rowStatus) {
    case HighsBasisStatus::kBasic:
      // if row is basic the parallel row is also basic
      if (duplicateIsModelRow) {
        solution.row_dual[duplicateRow] = 0.0;
        if (basis.valid)
          basis.row_status[duplicateRow] = HighsBasisStatus::kBasic;
      }
      break;
    case HighsBasisStatus::kUpper:
      // if row sits on its upper bound, and the row upper bound was
      // tightened using the parallel row we make the row basic and
      // transfer its dual value to the parallel row with the proper scale
      computeRowDualAndStatus(rowUpperTightened);
      break;
    case HighsBasisStatus::kLower:
      computeRowDualAndStatus(rowLowerTightened);
      break;
    default:
      assert(false);
  }
}

void HighsPostsolveStack::DuplicateColumn::undo(const HighsOptions& options,
                                                HighsSolution& solution,
                                                HighsBasis& basis) const {
  const bool debug_report = false;
  const double mergeVal = solution.col_value[col];

  auto okResidual = [&](const double x, const double y) {
    const double check_mergeVal = x + colScale * y;
    const double residual = std::fabs(check_mergeVal - mergeVal);
    const bool ok_residual = residual <= options.primal_feasibility_tolerance;
    if (!ok_residual && debug_report) {
      printf(
          "HighsPostsolveStack::DuplicateColumn::undo %g + %g.%g = %g != %g: "
          "residual = %g\n",
          x, colScale, y, check_mergeVal, mergeVal, residual);
    }
    return ok_residual;
  };

  auto isAtBound = [&](const double value, const double bound) {
    if (value < bound - options.primal_feasibility_tolerance) return false;
    if (value <= bound + options.primal_feasibility_tolerance) return true;
    return false;
  };

  //  const bool ok_merge = okMerge(options.mip_feasibility_tolerance);
  //  assert(ok_merge);
  //
  // the column dual of the duplicate column is easily computed by scaling
  // since col * colScale yields the coefficient values and cost of the
  // duplicate column.
  if (solution.dual_valid)
    solution.col_dual[duplicateCol] = solution.col_dual[col] * colScale;

  if (basis.valid) {
    // do postsolve using basis status if a basis is available: if the
    // merged column is nonbasic, we can just set both columns to
    // appropriate nonbasic status and value
    //
    // Undoing z = x + a.y
    //
    // Since x became z, its basis status is unchanged
    //
    // For a > 0, z\in [x_l + a.y_l, x_u + a.y_u]
    //
    // If z is nonbasic at its lower (upper) bound, set y to be
    // nonbasic at its lower (upper) bound
    //
    // For a < 0, z\in [x_l + a.y_u, x_u + a.y_l]
    //
    // If z is nonbasic at lower (upper) bound, set y to be nonbasic
    // at its upper (lower) bounds
    //
    // Check for perturbations
    switch (basis.col_status[col]) {
      case HighsBasisStatus::kLower: {
        solution.col_value[col] = colLower;
        if (colScale > 0) {
          basis.col_status[duplicateCol] = HighsBasisStatus::kLower;
          solution.col_value[duplicateCol] = duplicateColLower;
        } else {
          basis.col_status[duplicateCol] = HighsBasisStatus::kUpper;
          solution.col_value[duplicateCol] = duplicateColUpper;
        }
        // nothing else to do
        assert(okResidual(solution.col_value[col],
                          solution.col_value[duplicateCol]));
        return;
      }
      case HighsBasisStatus::kUpper: {
        solution.col_value[col] = colUpper;
        if (colScale > 0) {
          basis.col_status[duplicateCol] = HighsBasisStatus::kUpper;
          solution.col_value[duplicateCol] = duplicateColUpper;
        } else {
          basis.col_status[duplicateCol] = HighsBasisStatus::kLower;
          solution.col_value[duplicateCol] = duplicateColLower;
        }
        // nothing else to do
        assert(okResidual(solution.col_value[col],
                          solution.col_value[duplicateCol]));
        return;
      }
      case HighsBasisStatus::kZero: {
        solution.col_value[col] = 0.0;
        basis.col_status[duplicateCol] = HighsBasisStatus::kZero;
        solution.col_value[duplicateCol] = 0.0;
        // nothing else to do
        assert(okResidual(solution.col_value[col],
                          solution.col_value[duplicateCol]));
        return;
      }
      case HighsBasisStatus::kBasic:
      case HighsBasisStatus::kNonbasic:;
    }
    // Nonbasic cases should have been considered; basic case
    // considered later
    assert(basis.col_status[col] == HighsBasisStatus::kBasic);
  }

  // either no basis for postsolve, or column status is basic. One of
  // the two columns must become nonbasic. In case of integrality it is
  // simpler to choose col, since it has a coefficient of +1 in the equation z
  // = col + colScale * duplicateCol where the merged column is z and is
  // currently using the index of col. The duplicateCol can have a positive or
  // negative coefficient. So for postsolve, we first start out with col
  // sitting at the lower bound and compute the corresponding value for the
  // duplicate column as (z - colLower)/colScale. Then the following things
  // might happen:
  // - case 1: the value computed for duplicateCol is within the bounds
  // - case 1.1: duplicateCol is continuous -> accept value, make col nonbasic
  // at lower and duplicateCol basic
  // - case 1.2: duplicateCol is integer -> accept value if integer feasible,
  // otherwise round down and compute value of col as
  // col = z - colScale * duplicateCol
  // - case 2: the value for duplicateCol violates the column bounds: make it
  // sit at the bound that is violated
  //           and compute the value of col as col = z - colScale *
  //           duplicateCol for basis postsolve col is basic and duplicateCol
  //           nonbasic at lower/upper depending on which bound is violated.

  if (colLower != -kHighsInf)
    solution.col_value[col] = colLower;
  else
    solution.col_value[col] = std::min(0.0, colUpper);
  solution.col_value[duplicateCol] =
      double((HighsCDouble(mergeVal) - solution.col_value[col]) / colScale);

  bool recomputeCol = false;

  // Set any basis status for duplicateCol to kNonbasic to check that
  // it is set
  if (basis.valid) basis.col_status[duplicateCol] = HighsBasisStatus::kNonbasic;

  if (solution.col_value[duplicateCol] > duplicateColUpper) {
    solution.col_value[duplicateCol] = duplicateColUpper;
    recomputeCol = true;
    if (basis.valid) basis.col_status[duplicateCol] = HighsBasisStatus::kUpper;
  } else if (solution.col_value[duplicateCol] < duplicateColLower) {
    solution.col_value[duplicateCol] = duplicateColLower;
    recomputeCol = true;
    if (basis.valid) basis.col_status[duplicateCol] = HighsBasisStatus::kLower;
  } else if (duplicateColIntegral) {
    // Doesn't set basis.col_status[duplicateCol], so assume no basis
    assert(!basis.valid);
    if (fractionality(solution.col_value[duplicateCol]) >
        options.mip_feasibility_tolerance) {
      solution.col_value[duplicateCol] =
          std::floor(solution.col_value[duplicateCol]);
      recomputeCol = true;
    }
  }

  if (recomputeCol) {
    solution.col_value[col] =
        mergeVal - colScale * solution.col_value[duplicateCol];
    if (!duplicateColIntegral && colIntegral) {
      // if column is integral and duplicateCol is not we need to make sure
      // we split the values into an integral one for col
      //
      // Doesn't set basis.col_status[duplicateCol], so assume no basis
      assert(!basis.valid);
      solution.col_value[col] = std::ceil(solution.col_value[col] -
                                          options.mip_feasibility_tolerance);
      solution.col_value[duplicateCol] =
          double((HighsCDouble(mergeVal) - solution.col_value[col]) / colScale);
    }
  } else {
    // setting col to its lower bound yielded a feasible value for
    // duplicateCol - not necessarily!
    if (basis.valid) {
      // This makes duplicateCol basic
      basis.col_status[duplicateCol] = basis.col_status[col];
      basis.col_status[col] = HighsBasisStatus::kLower;
      assert(basis.col_status[duplicateCol] == HighsBasisStatus::kBasic);
    }
  }
  // Check that any basis status for duplicateCol has been set
  if (basis.valid)
    assert(basis.col_status[duplicateCol] != HighsBasisStatus::kNonbasic);

  bool illegal_duplicateCol_lower =
      solution.col_value[duplicateCol] <
      duplicateColLower - options.mip_feasibility_tolerance;
  bool illegal_duplicateCol_upper =
      solution.col_value[duplicateCol] >
      duplicateColUpper + options.mip_feasibility_tolerance;
  bool illegal_col_lower =
      solution.col_value[col] < colLower - options.mip_feasibility_tolerance;
  bool illegal_col_upper =
      solution.col_value[col] > colUpper + options.mip_feasibility_tolerance;
  bool illegal_residual =
      !okResidual(solution.col_value[col], solution.col_value[duplicateCol]);
  bool error = illegal_duplicateCol_lower || illegal_duplicateCol_upper ||
               illegal_col_lower || illegal_col_upper || illegal_residual;
  if (error) {
    if (debug_report)
      printf(
          "DuplicateColumn::undo error: col = %d(%g), duplicateCol = %d(%g)\n"
          "%g\n%g\n%g %g %d\n%g %g %d\n",
          int(col), solution.col_value[col], int(duplicateCol),
          solution.col_value[duplicateCol], mergeVal, colScale, colLower,
          colUpper, colIntegral, duplicateColLower, duplicateColUpper,
          duplicateColIntegral);
    // Fix error due to undo
    undoFix(options, solution);
    illegal_duplicateCol_lower =
        solution.col_value[duplicateCol] <
        duplicateColLower - options.mip_feasibility_tolerance;
    illegal_duplicateCol_upper =
        solution.col_value[duplicateCol] >
        duplicateColUpper + options.mip_feasibility_tolerance;
    illegal_col_lower =
        solution.col_value[col] < colLower - options.mip_feasibility_tolerance;
    illegal_col_upper =
        solution.col_value[col] > colUpper + options.mip_feasibility_tolerance;
    illegal_residual =
        !okResidual(solution.col_value[col], solution.col_value[duplicateCol]);
  } else {
    return;
  }
  const bool allow_assert = false;
  if (allow_assert) {
    assert(!illegal_duplicateCol_lower);
    assert(!illegal_duplicateCol_upper);
    assert(!illegal_col_lower);
    assert(!illegal_col_upper);
    assert(!illegal_residual);
  }
  // Following undoFix, set any basis status, ideally keeping col basic
  if (basis.valid) {
    bool duplicateCol_basic = false;
    if (duplicateColLower <= -kHighsInf && duplicateColUpper >= kHighsInf) {
      // duplicateCol is free, so may be zero
      if (solution.col_value[duplicateCol] == 0) {
        basis.col_status[col] = HighsBasisStatus::kBasic;
        basis.col_status[duplicateCol] = HighsBasisStatus::kZero;
      } else {
        duplicateCol_basic = true;
      }
    } else if (isAtBound(solution.col_value[duplicateCol], duplicateColLower)) {
      basis.col_status[col] = HighsBasisStatus::kBasic;
      basis.col_status[duplicateCol] = HighsBasisStatus::kLower;
    } else if (isAtBound(solution.col_value[duplicateCol], duplicateColUpper)) {
      basis.col_status[col] = HighsBasisStatus::kBasic;
      basis.col_status[duplicateCol] = HighsBasisStatus::kUpper;
    } else {
      // duplicateCol is not free or at a bound, so must be basic
      duplicateCol_basic = true;
    }
    if (duplicateCol_basic) {
      // duplicateCol must be basic
      basis.col_status[duplicateCol] = HighsBasisStatus::kBasic;
      // Hopefully col can be nonbasic
      if (isAtBound(solution.col_value[col], colLower)) {
        basis.col_status[col] = HighsBasisStatus::kLower;
      } else if (isAtBound(solution.col_value[col], colUpper)) {
        basis.col_status[col] = HighsBasisStatus::kUpper;
      } else {
        basis.col_status[col] = HighsBasisStatus::kNonbasic;
        if (debug_report)
          printf(
              "When demerging, neither col nor duplicateCol can be "
              "nonbasic\n");
        if (kAllowDeveloperAssert) assert(666 == 999);
      }
    }
  }
}

bool HighsPostsolveStack::DuplicateColumn::okMerge(
    const double tolerance) const {
  // When merging x and y to x+a.y, not all values of a are permitted,
  // since it must be possible to map back onto feasible values of x
  // and y.
  //
  // Assume WLOG that a > 0, x\in[x_l, x_u], y\in[y_l, y_u]
  //
  // Let z = x + a.y
  //
  // Range for z is [x_l+a.y_l, x_u+a.y_u]
  //
  // * If x and y are both integer:
  //
  // z will be integer and x+a.y must generate all integer values in
  // [x_l+a.y_l, x_u+a.y_u]. Hence a must be an integer. If a >=
  // (x_u-x_l)+2 then, since [a.y_l, a.y_u] contains integer multiples
  // of a, some of the intervening integers don't correspond to a
  // value of x. Hence a must be an integer and a <= (x_u-x_l)+1
  //
  // For example, if x and y are binary, then x+a.y is [0, 1, a,
  // 1+a]. For this to be a continuous sequence of integers, we must
  // have a <= 2.
  //
  // * If x is integer and y is continuous:
  //
  // z will be continuous and x+a.y must generate all values in
  // [x_l+a.y_l, x_u+a.y_u]. Since [x_l, x_u] are integers, [a.y_l,
  // a.y_u] = a[y_l, y_u] must be of length at least 1. Hence a must
  // be at least 1/(y_u-y_l) in magnitude.
  //
  // * If x is continuous and y is integer:
  //
  // z will be continuous and x+a.y must generate all values in
  // [x_l+a.y_l, x_u+a.y_u]. Since [a.y_l, a.y_u] contains integer
  // multiples of a, the gaps between them must not exceed the length
  // of [x_l, x_u]. Hence a must be at most x_u-x_l in
  // magnitude.
  //
  // Observe that this is equivalent to requiring 1/a to be at least
  // 1/(x_u-x_l) in magnitude, the symmetric result corresponding to
  // the merge (1/a)x+y.
  //
  //  * If x and y are both continuous
  //
  // z will be continuous and x+a.y naturally generates all values in
  // [x_l+a.y_l, x_u+a.y_u].

  const double scale = colScale;
  const bool x_int = colIntegral;
  const bool y_int = duplicateColIntegral;
  const double x_lo = x_int ? std::ceil(colLower - tolerance) : colLower;
  const double x_up = x_int ? std::floor(colUpper + tolerance) : colUpper;
  const double y_lo =
      y_int ? std::ceil(duplicateColLower - tolerance) : duplicateColLower;
  const double y_up =
      y_int ? std::floor(duplicateColUpper + tolerance) : duplicateColUpper;
  const double x_len = x_up - x_lo;
  const double y_len = y_up - y_lo;
  std::string newline = "\n";
  bool ok_merge = true;
  const bool debug_report = false;
  if (scale == 0) {
    if (debug_report)
      printf("%sDuplicateColumn::checkMerge: Scale cannot be zero\n",
             newline.c_str());
    newline = "";
    ok_merge = false;
  }
  const double abs_scale = std::fabs(scale);
  if (x_int) {
    if (y_int) {
      // Scale must be integer and not exceed (x_u-x_l)+1 in magnitude
      if (fractionality(scale) > tolerance) {
        if (debug_report)
          printf(
              "%sDuplicateColumn::checkMerge: scale must be integer, but is "
              "%g\n",
              newline.c_str(), scale);
        newline = "";
        ok_merge = false;
      }
      double scale_limit = x_len + 1 + tolerance;
      if (abs_scale > scale_limit) {
        if (debug_report)
          printf(
              "%sDuplicateColumn::checkMerge: scale = %g, but |scale| cannot "
              "exceed %g since x is [%g, %g]\n",
              newline.c_str(), scale, scale_limit, x_lo, x_up);
        newline = "";
        ok_merge = false;
      }
    } else {  // y is continuous
      if (debug_report)
        printf("DuplicateColumn::checkMerge: x-integer; y-continuous\n");
      // Scale must be at least 1/(y_u-y_l) in magnitude
      if (y_len == 0) {
        if (debug_report)
          printf(
              "%sDuplicateColumn::checkMerge: scale = %g is too small in "
              "magnitude, as y is [%g, %g]\n",
              newline.c_str(), scale, y_lo, y_up);
        newline = "";
        ok_merge = false;
      } else {
        double scale_limit = 1 / y_len;
        if (abs_scale < scale_limit) {
          if (debug_report)
            printf(
                "%sDuplicateColumn::checkMerge: scale = %g, but |scale| must "
                "be "
                "at least %g since y is [%g, %g]\n",
                newline.c_str(), scale, scale_limit, y_lo, y_up);
          newline = "";
          ok_merge = false;
        }
      }
    }
  } else {
    if (y_int) {
      if (debug_report)
        printf("DuplicateColumn::checkMerge: x-continuous; y-integer\n");
      // Scale must be at most (x_u-x_l) in magnitude
      double scale_limit = x_len;
      if (abs_scale > scale_limit) {
        if (debug_report)
          printf(
              "%sDuplicateColumn::checkMerge: scale = %g, but |scale| must "
              "be "
              "at "
              "most %g since x is [%g, %g]\n",
              newline.c_str(), scale, scale_limit, x_lo, x_up);
        newline = "";
        ok_merge = false;
      }
    } else {
      // x and y are continuous
      //	if (debug_report) printf("DuplicateColumn::checkMerge:
      // x-continuous ;
      // y-continuous\n");
    }
  }
  return ok_merge;
}

void HighsPostsolveStack::DuplicateColumn::undoFix(
    const HighsOptions& options, HighsSolution& solution) const {
  const double mip_feasibility_tolerance = options.mip_feasibility_tolerance;
  const double primal_feasibility_tolerance =
      options.primal_feasibility_tolerance;
  std::vector<double>& col_value = solution.col_value;
  const bool allow_assert = false;
  const bool debug_report = false;
  //=============================================================================================

  auto isInteger = [&](const double v) {
    return (fractionality(v) <= mip_feasibility_tolerance);
  };

  auto isFeasible = [&](const double l, const double v, const double u) {
    return v >= l - primal_feasibility_tolerance &&
           v <= u + primal_feasibility_tolerance;
  };
  const double merge_value = col_value[col];
  const double value_max = 1000;
  const double eps = 1e-8;
  const double scale = colScale;
  const bool x_int = colIntegral;
  const bool y_int = duplicateColIntegral;
  const int x_ix = col;
  const int y_ix = duplicateCol;
  const double x_lo =
      x_int ? std::ceil(colLower - mip_feasibility_tolerance) : colLower;
  const double x_up =
      x_int ? std::floor(colUpper + mip_feasibility_tolerance) : colUpper;
  const double y_lo =
      y_int ? std::ceil(duplicateColLower - mip_feasibility_tolerance)
            : duplicateColLower;
  const double y_up =
      y_int ? std::floor(duplicateColUpper + mip_feasibility_tolerance)
            : duplicateColUpper;
  if (kAllowDeveloperAssert) assert(scale);
  double x_v = merge_value;
  double y_v;

  //  assert(x_int);
  //  assert(y_int);
  //  assert(scale < 0);
  if (x_int) {
    double x_0 = 0;
    double x_d = 0;
    double x_1 = 0;
    double x_free = false;
    if (x_lo <= -kHighsInf) {
      if (x_up >= kHighsInf) {
        // x is free
        x_free = true;
        x_0 = 0;
        x_d = 1.0;
        x_1 = value_max;
      } else {
        // x is (-int, u]
        x_0 = x_up;
        x_d = -1.0;
        x_1 = -value_max;
      }
    } else {
      if (x_up >= kHighsInf) {
        // x is [l, inf)
        x_0 = x_lo;
        x_d = 1.0;
        x_1 = value_max;
      } else {
        // x is [l, u]
        x_0 = x_lo;
        x_d = 1.0;
        x_1 = x_up;
      }
    }
    // x is integer, so look through its possible values to find a
    // suitable y
    if (x_free && debug_report) printf("DuplicateColumn::undo x is free\n");
    if (debug_report)
      printf("DuplicateColumn::undo Using x (%g; %g; %g)\n", x_0, x_d, x_1);
    bool found_y = false;
    for (x_v = x_0;; x_v += x_d) {
      //      printf("x_v = %g\n", x_v);
      y_v = double((HighsCDouble(merge_value) - x_v) / scale);
      if (isFeasible(y_lo, y_v, y_up)) {
        found_y = !y_int || isInteger(y_v);
        if (found_y) break;
      }
      if (x_d > 0 && x_v + x_d >= x_1 + eps) break;
      if (x_d < 0 && x_v + x_d <= x_1 - eps) break;
    }
    if (allow_assert) assert(found_y);
  } else if (y_int) {
    double y_0 = 0;
    double y_d = 0;
    double y_1 = 0;
    double y_free = false;
    if (y_lo <= -kHighsInf) {
      if (y_up >= kHighsInf) {
        // y is free
        y_free = true;
        y_0 = 0;
        y_d = 1.0;
        y_1 = value_max;
      } else {
        // y is (-int, u]
        y_0 = y_up;
        y_d = -1.0;
        y_1 = -value_max;
      }
    } else {
      if (y_up >= kHighsInf) {
        // y is [l, inf)
        y_0 = y_lo;
        y_d = 1.0;
        y_1 = value_max;
      } else {
        // y is [l, u]
        y_0 = y_lo;
        y_d = 1.0;
        y_1 = y_up;
      }
    }
    // y is integer, so look through its possible values to find a
    // suitable x
    if (y_free && debug_report) printf("DuplicateColumn::undo y is free\n");
    if (debug_report)
      printf("DuplicateColumn::undo Using y (%g; %g; %g)\n", y_0, y_d, y_1);
    bool found_x = false;
    for (y_v = y_0;; y_v += y_d) {
      //      printf("y_v = %g\n", y_v);
      x_v = double((HighsCDouble(merge_value) - HighsCDouble(y_v) * scale));
      if (isFeasible(x_lo, x_v, x_up)) {
        found_x = !x_int || isInteger(x_v);
        if (found_x) break;
      }
      if (y_d > 0 && y_v + y_d >= y_1 + eps) break;
      if (y_d < 0 && y_v + y_d <= y_1 - eps) break;
    }
    if (allow_assert) assert(found_x);
  } else {
    // x and y are both continuous
    double v_m_a_ylo = 0;
    double v_m_a_yup = 0;
    if (y_lo <= -kHighsInf) {
      v_m_a_ylo = scale > 0 ? kHighsInf : -kHighsInf;
    } else {
      v_m_a_ylo =
          double((HighsCDouble(merge_value) - HighsCDouble(y_lo) * scale));
    }
    if (y_up >= kHighsInf) {
      v_m_a_yup = scale > 0 ? -kHighsInf : kHighsInf;
    } else {
      v_m_a_yup =
          double((HighsCDouble(merge_value) - HighsCDouble(y_up) * scale));
    }
    // Need to ensure that y puts x in [x_l, x_u]
    if (scale > 0) {
      if (debug_report)
        printf("DuplicateColumn::undo [V-a(y_u), V-a(y_l)] == [%g, %g]\n",
               v_m_a_yup, v_m_a_ylo);
      // V-ay is in [V-a(y_u), V-a(y_l)] == [v_m_a_yup, v_m_a_ylo]
      if (y_up < kHighsInf) {
        // If v_m_a_yup is right of x_up+eps then [v_m_a_yup, v_m_a_ylo] is
        // right of [x_lo-eps, x_up+eps] so there's no solution. [Could
        // try v_m_a_ylo computed from y_lo-eps.]
        if (kAllowDeveloperAssert)
          assert(x_up + primal_feasibility_tolerance >= v_m_a_yup);
        // This assignment is OK unless x_v < x_lo-eps
        y_v = y_up;
        x_v = v_m_a_yup;
        if (x_v < x_lo - primal_feasibility_tolerance) {
          // Try y_v corresponding to x_lo
          x_v = x_lo;
          y_v = double((HighsCDouble(merge_value) - x_v) / scale);
          if (y_v < y_lo - primal_feasibility_tolerance) {
            // Very tight: use x_v on its margin and hope!
            x_v = x_lo - primal_feasibility_tolerance;
            y_v = double((HighsCDouble(merge_value) - x_v) / scale);
          }
        }
      } else if (y_lo > -kHighsInf) {
        // If v_m_a_ylo is left of x_lo-eps then [v_m_a_yup, v_m_a_ylo] is
        // left of [x_lo-eps, x_up+eps] so there's no solution. [Could
        // try v_m_a_yup computed from y_up+eps.]
        if (kAllowDeveloperAssert)
          assert(x_lo - primal_feasibility_tolerance <= v_m_a_ylo);
        // This assignment is OK unless x_v > x_up-eps
        y_v = y_lo;
        x_v = v_m_a_ylo;
        if (x_v > x_up + primal_feasibility_tolerance) {
          // Try y_v corresponding to x_up
          //	  if (kAllowDeveloperAssert) assert(1==102);
          x_v = x_up;
          y_v = double((HighsCDouble(merge_value) - x_v) / scale);
          if (y_v > y_up + primal_feasibility_tolerance) {
            // Very tight: use x_v on its margin and hope!
            if (debug_report) printf("DuplicateColumn::undoFix 2==102\n");
            if (kAllowDeveloperAssert) assert(2 == 102);
            x_v = x_up + primal_feasibility_tolerance;
            y_v = double((HighsCDouble(merge_value) - x_v) / scale);
          }
        }
      } else {
        // y is free, so use x_v = max(0, x_lo)
        x_v = std::max(0.0, x_lo);
        y_v = double((HighsCDouble(merge_value) - x_v) / scale);
      }
    } else {  // scale < 0
      if (debug_report)
        printf("DuplicateColumn::undo [V-a(y_l), V-a(y_u)] == [%g, %g]\n",
               v_m_a_ylo, v_m_a_yup);
      // V-ay is in [V-a(y_l), V-a(y_u)] == [v_m_a_ylo, v_m_a_yup]
      //
      if (y_lo > -kHighsInf) {
        // If v_m_a_ylo is right of x_up+eps then [v_m_a_ylo, v_m_a_yup] is
        // right of [x_lo-eps, x_up+eps] so there's no solution. [Could
        // try v_m_a_ylo computed from y_up+eps.]
        if (kAllowDeveloperAssert)
          assert(x_up + primal_feasibility_tolerance >= v_m_a_ylo);
        // This assignment is OK unless x_v < x_lo-eps
        y_v = y_lo;
        x_v = v_m_a_ylo;
        if (x_v < x_lo - primal_feasibility_tolerance) {
          // Try y_v corresponding to x_lo
          //	  if (kAllowDeveloperAssert) assert(11==101);
          x_v = x_lo;
          y_v = double((HighsCDouble(merge_value) - x_v) / scale);
          if (y_v > y_up + primal_feasibility_tolerance) {
            // Very tight: use x_v on its margin and hope!
            if (debug_report) printf("DuplicateColumn::undoFix 12==101\n");
            if (kAllowDeveloperAssert) assert(12 == 101);
            x_v = x_lo - primal_feasibility_tolerance;
            y_v = double((HighsCDouble(merge_value) - x_v) / scale);
          }
        }
      } else if (y_up < kHighsInf) {
        // If v_m_a_yup is left of x_lo-eps then [v_m_a_ylo, v_m_a_yup] is
        // left of [x_lo-eps, x_up+eps] so there's no solution. [Could
        // try v_m_a_yup computed from y_lo-eps.]
        if (kAllowDeveloperAssert)
          assert(x_lo - primal_feasibility_tolerance <= v_m_a_yup);
        // This assignment is OK unless x_v < x_lo-eps
        y_v = y_up;
        x_v = v_m_a_yup;
        if (x_v > x_up + primal_feasibility_tolerance) {
          // Try y_v corresponding to x_up
          //	  if (kAllowDeveloperAssert) assert(11==102);
          x_v = x_up;
          y_v = double((HighsCDouble(merge_value) - x_v) / scale);
          if (y_v < y_lo - primal_feasibility_tolerance) {
            // Very tight: use x_v on its margin and hope!
            if (debug_report) printf("DuplicateColumn::undoFix 12==102\n");
            if (kAllowDeveloperAssert) assert(12 == 102);
            x_v = x_up + primal_feasibility_tolerance;
            y_v = double((HighsCDouble(merge_value) - x_v) / scale);
          }
        }
      } else {
        // y is free, so use x_v = max(0, x_lo)
        x_v = std::max(0.0, x_lo);
        y_v = double((HighsCDouble(merge_value) - x_v) / scale);
      }
    }
  }
  const double residual_tolerance = 1e-12;
  double residual =
      std::fabs(double(HighsCDouble(x_v) + HighsCDouble(y_v) * scale -
                       HighsCDouble(merge_value)));
  const bool x_y_ok =
      isFeasible(x_lo, x_v, x_up) && isFeasible(y_lo, y_v, y_up) &&
      (!x_int || isInteger(x_v)) && (!y_int || isInteger(y_v)) &&
      (std::fabs(x_v) < kHighsInf) && (std::fabs(y_v) < kHighsInf) &&
      (residual <= residual_tolerance);

  bool check;
  check = isFeasible(x_lo, x_v, x_up);
  if (!check) {
    if (debug_report)
      printf(
          "DuplicateColumn::undo error: isFeasible(x_lo, x_v, x_up) is "
          "false\n");
    if (allow_assert) assert(check);
  }
  check = isFeasible(y_lo, y_v, y_up);
  if (!check) {
    if (debug_report)
      printf(
          "DuplicateColumn::undo error: isFeasible(y_lo, y_v, y_up) is "
          "false\n");
    if (allow_assert) assert(check);
  }
  check = !x_int || isInteger(x_v);
  if (!check) {
    if (debug_report)
      printf(
          "DuplicateColumn::undo error: !x_int || isInteger(x_v) is false\n");
    if (allow_assert) assert(check);
  }
  check = !y_int || isInteger(y_v);
  if (!check) {
    if (debug_report)
      printf(
          "DuplicateColumn::undo error: !y_int || isInteger(y_v) is false\n");
    if (allow_assert) assert(check);
  }
  check = std::fabs(x_v) < kHighsInf;
  if (!check) {
    if (debug_report)
      printf(
          "DuplicateColumn::undo error: std::fabs(x_v) < kHighsInf is "
          "false\n");
    if (allow_assert) assert(check);
  }
  check = std::fabs(y_v) < kHighsInf;
  if (!check) {
    if (debug_report)
      printf(
          "DuplicateColumn::undo error: std::fabs(y_v) < kHighsInf is "
          "false\n");
    if (allow_assert) assert(check);
  }
  check = residual <= residual_tolerance;
  if (!check) {
    if (debug_report)
      printf(
          "DuplicateColumn::undo error: residual <= residual_tolerance is "
          "false\n");
    if (allow_assert) assert(check);
  }
  check = residual <= residual_tolerance;
  if (debug_report)
    printf("DuplicateColumn::undo%s x = %g; y = %g to give x + (%g)y = %g",
           x_y_ok ? "" : " ERROR", x_v, y_v, scale, merge_value);
  if (x_y_ok) {
    if (debug_report) printf(": FIXED\n");
  } else if (check) {
    if (debug_report) printf("\n");
  } else {
    if (debug_report) printf(": residual = %g\n", residual);
  }
  //=============================================================================================
  if (x_y_ok) {
    col_value[x_ix] = x_v;
    col_value[y_ix] = y_v;
  }
}

void HighsPostsolveStack::DuplicateColumn::transformToPresolvedSpace(
    std::vector<double>& primalSol) const {
  primalSol[col] = primalSol[col] + colScale * primalSol[duplicateCol];
}

void HighsPostsolveStack::SlackColSubstitution::undo(
    const HighsOptions& options, const std::vector<Nonzero>& rowValues,
    HighsSolution& solution, HighsBasis& basis) {
  bool debug_print = false;
  // May have to determine row dual and basis status unless doing
  // primal-only transformation in MIP solver, in which case row may
  // no longer exist if it corresponds to a removed cut, so have to
  // avoid exceeding array bounds of solution.row_value
  bool isModelRow = static_cast<size_t>(row) < solution.row_value.size();

  // compute primal values
  double colCoef = 0;
  HighsCDouble rowValue = 0;
  for (const auto& rowVal : rowValues) {
    if (rowVal.index == col)
      colCoef = rowVal.value;
    else
      rowValue += rowVal.value * solution.col_value[rowVal.index];
  }

  assert(colCoef != 0);
  // Row values aren't fully postsolved, so why do this?
  if (isModelRow)
    solution.row_value[row] =
        double(rowValue + colCoef * solution.col_value[col]);

  solution.col_value[col] = double((rhs - rowValue) / colCoef);

  // If no dual values requested, return here
  if (!solution.dual_valid) return;

  // Row retains its dual value, and column has this dual value scaled by coeff
  if (isModelRow) solution.col_dual[col] = -solution.row_dual[row] / colCoef;

  // Set basis status if necessary
  if (!basis.valid) return;

  // If row is basic, then slack is basic, otherwise row retains its status
  if (isModelRow) {
    HighsBasisStatus save_row_basis_status = basis.row_status[row];
    if (basis.row_status[row] == HighsBasisStatus::kBasic) {
      basis.col_status[col] = HighsBasisStatus::kBasic;
      basis.row_status[row] =
          computeRowStatus(solution.row_dual[row], RowType::kEq);
    } else if (basis.row_status[row] == HighsBasisStatus::kLower) {
      basis.col_status[col] =
          colCoef > 0 ? HighsBasisStatus::kUpper : HighsBasisStatus::kLower;
    } else {
      basis.col_status[col] =
          colCoef > 0 ? HighsBasisStatus::kLower : HighsBasisStatus::kUpper;
    }
    if (debug_print)
      printf(
          "HighsPostsolveStack::SlackColSubstitution::undo OgRowStatus = %s; "
          "RowStatus = %s; ColStatus = %s\n",
          utilBasisStatusToString(save_row_basis_status).c_str(),
          utilBasisStatusToString(basis.row_status[row]).c_str(),
          utilBasisStatusToString(basis.col_status[col]).c_str());
    if (basis.col_status[col] == HighsBasisStatus::kLower) {
      assert(solution.col_dual[col] > -options.dual_feasibility_tolerance);
    } else if (basis.col_status[col] == HighsBasisStatus::kUpper) {
      assert(solution.col_dual[col] < options.dual_feasibility_tolerance);
    }
  } else {
    basis.col_status[col] = HighsBasisStatus::kNonbasic;
  }
}

}  // namespace presolve
