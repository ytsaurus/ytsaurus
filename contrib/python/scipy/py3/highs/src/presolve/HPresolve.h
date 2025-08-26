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
/**@file presolve/HPresolve.h
 * @brief
 */
#ifndef PRESOLVE_HIGHS_PRESOLVE_H_
#define PRESOLVE_HIGHS_PRESOLVE_H_
#include <cassert>
#include <cmath>
#include <list>
#include <queue>
#include <set>
#include <unordered_map>
#include <vector>

#include "lp_data/HConst.h"
#include "lp_data/HStruct.h"
#include "lp_data/HighsLp.h"
#include "lp_data/HighsOptions.h"
#include "mip/HighsMipSolver.h"
#include "presolve/HPresolveAnalysis.h"
#include "util/HighsCDouble.h"
#include "util/HighsHash.h"
#include "util/HighsLinearSumBounds.h"
#include "util/HighsMatrixSlice.h"

namespace presolve {

class HighsPostsolveStack;

class HPresolve {
  // pointer to model and options that where presolved
  HighsLp* model;
  const HighsOptions* options;
  HighsTimer* timer;
  HighsMipSolver* mipsolver = nullptr;
  double primal_feastol;

  // triplet storage
  std::vector<double> Avalue;
  std::vector<HighsInt> Arow;
  std::vector<HighsInt> Acol;

  // linked list links for column based links for each nonzero
  std::vector<HighsInt> colhead;
  std::vector<HighsInt> Anext;
  std::vector<HighsInt> Aprev;

  // splay tree links for row based iteration and nonzero lookup
  std::vector<HighsInt> rowroot;
  std::vector<HighsInt> ARleft;
  std::vector<HighsInt> ARright;

  // length of rows and columns
  std::vector<HighsInt> rowsize;
  std::vector<HighsInt> rowsizeInteger;
  std::vector<HighsInt> rowsizeImplInt;
  std::vector<HighsInt> colsize;

  // vector to store the nonzero positions of a row
  std::vector<HighsInt> rowpositions;

  // stack to reuse free slots
  std::vector<HighsInt> freeslots;

  // vectors holding implied bounds on primal and dual variables as well as
  // their origins
  std::vector<double> implColLower;
  std::vector<double> implColUpper;
  std::vector<HighsInt> colLowerSource;
  std::vector<HighsInt> colUpperSource;
  std::vector<double> rowDualLower;
  std::vector<double> rowDualUpper;
  std::vector<double> implRowDualLower;
  std::vector<double> implRowDualUpper;
  std::vector<HighsInt> rowDualLowerSource;
  std::vector<HighsInt> rowDualUpperSource;
  std::vector<std::set<HighsInt>> colImplSourceByRow;
  std::vector<std::set<HighsInt>> implRowDualSourceByCol;

  // implied bounds on values of primal and dual rows computed from the bounds
  // of primal and dual variables
  HighsLinearSumBounds impliedRowBounds;
  HighsLinearSumBounds impliedDualRowBounds;

  std::vector<HighsInt> changedRowIndices;
  std::vector<uint8_t> changedRowFlag;
  std::vector<HighsInt> changedColIndices;
  std::vector<uint8_t> changedColFlag;

  std::vector<std::pair<HighsInt, HighsInt>> substitutionOpportunities;

  // set with the sizes and indices of equation rows sorted by the size and a
  // vector to access there iterator positions in the set by index for quick
  // removal
  std::set<std::pair<HighsInt, HighsInt>> equations;
  std::vector<std::set<std::pair<HighsInt, HighsInt>>::iterator> eqiters;

  bool shrinkProblemEnabled;
  size_t reductionLimit;

  // vectors storing singleton rows and columns
  std::vector<HighsInt> singletonRows;
  std::vector<HighsInt> singletonColumns;

  // flags to mark rows/columns as deleted
  std::vector<uint8_t> rowDeleted;
  std::vector<uint8_t> colDeleted;

  std::vector<uint16_t> numProbes;

  int64_t probingContingent;
  HighsInt probingNumDelCol;
  HighsInt numProbed;

  // counters for number of deleted rows and columns
  HighsInt numDeletedRows;
  HighsInt numDeletedCols;

  // store old problem sizes to compute percentage reductions in
  // presolve loop
  HighsInt oldNumCol;
  HighsInt oldNumRow;
  bool probingEarlyAbort;

  enum class Result {
    kOk,
    kPrimalInfeasible,
    kDualInfeasible,
    kStopped,
  };
  HighsPresolveStatus presolve_status_;
  HPresolveAnalysis analysis_;

  // private functions for different shared functionality and matrix
  // modification

  void link(HighsInt pos);

  void unlink(HighsInt pos);

  void markChangedRow(HighsInt row);

  void markChangedCol(HighsInt col);

  double getMaxAbsColVal(HighsInt col) const;

  double getMaxAbsRowVal(HighsInt row) const;

  void updateColImpliedBounds(HighsInt row, HighsInt col, double val);

  void recomputeColImpliedBounds(HighsInt row);

  void recomputeRowDualImpliedBounds(HighsInt col);

  void updateRowDualImpliedBounds(HighsInt row, HighsInt col, double val);

  bool rowCoefficientsIntegral(HighsInt row, double scale) const;

  bool isImpliedFree(HighsInt col) const;

  bool isDualImpliedFree(HighsInt row) const;

  void dualImpliedFreeGetRhsAndRowType(HighsInt row, double& rhs,
                                       HighsPostsolveStack::RowType& rowType,
                                       bool relaxRowDualBounds = false);

  bool isImpliedIntegral(HighsInt col);

  bool isImpliedInteger(HighsInt col);

  bool convertImpliedInteger(HighsInt col, HighsInt row = -1,
                             bool skipInputChecks = false);

  bool isLowerImplied(HighsInt col) const;

  bool isUpperImplied(HighsInt col) const;

  HighsInt countFillin(HighsInt row);

  bool checkFillin(HighsHashTable<HighsInt, HighsInt>& fillinCache,
                   HighsInt row, HighsInt col);

  void reinsertEquation(HighsInt row);

#ifndef NDEBUG
  void debugPrintRow(HighsPostsolveStack& postsolve_stack, HighsInt row);
#endif

  HighsInt findNonzero(HighsInt row, HighsInt col);

  bool okFromCSC(const std::vector<double>& Aval,
                 const std::vector<HighsInt>& Aindex,
                 const std::vector<HighsInt>& Astart);

  bool okFromCSR(const std::vector<double>& ARval,
                 const std::vector<HighsInt>& ARindex,
                 const std::vector<HighsInt>& ARstart);

  void toCSC(std::vector<double>& Aval, std::vector<HighsInt>& Aindex,
             std::vector<HighsInt>& Astart);

  void toCSR(std::vector<double>& ARval, std::vector<HighsInt>& ARindex,
             std::vector<HighsInt>& ARstart);

  void storeRow(HighsInt row);

  HighsTripletPositionSlice getStoredRow() const;

  HighsTripletListSlice getColumnVector(HighsInt col) const;

  HighsTripletTreeSlicePreOrder getRowVector(HighsInt row) const;

  HighsTripletTreeSliceInOrder getSortedRowVector(HighsInt row) const;

  void markRowDeleted(HighsInt row);

  void markColDeleted(HighsInt col);

  bool fixColToLowerOrUnbounded(HighsPostsolveStack& postsolve_stack,
                                HighsInt col);

  bool fixColToUpperOrUnbounded(HighsPostsolveStack& postsolve_stack,
                                HighsInt col);

  void fixColToZero(HighsPostsolveStack& postsolve_stack, HighsInt col);

  void transformColumn(HighsPostsolveStack& postsolve_stack, HighsInt col,
                       double scale, double constant);

  void scaleRow(HighsInt row, double scale, bool integral = false);

  void scaleStoredRow(HighsInt row, double scale, bool integral = false);

  void substitute(HighsInt row, HighsInt col, double rhs);

  void changeColUpper(HighsInt col, double newUpper);

  void changeColLower(HighsInt col, double newLower);

  void changeRowDualUpper(HighsInt row, double newUpper);

  void changeRowDualLower(HighsInt row, double newLower);

  void changeImplColUpper(HighsInt col, double newUpper, HighsInt originRow);

  void changeImplColLower(HighsInt col, double newLower, HighsInt originRow);

  void changeImplRowDualUpper(HighsInt row, double newUpper,
                              HighsInt originCol);

  void changeImplRowDualLower(HighsInt row, double newLower,
                              HighsInt originCol);

  void scaleMIP(HighsPostsolveStack& postsolve_stack);

  Result applyConflictGraphSubstitutions(HighsPostsolveStack& postsolve_stack);

  Result fastPresolveLoop(HighsPostsolveStack& postsolve_stack);

  Result presolve(HighsPostsolveStack& postsolve_stack);

  Result removeSlacks(HighsPostsolveStack& postsolve_stack);

  Result checkTimeLimit();

  Result checkLimits(HighsPostsolveStack& postsolve_stack);

  void storeCurrentProblemSize();

  double problemSizeReduction();

 public:
  // for LP presolve
  bool okSetInput(HighsLp& model_, const HighsOptions& options_,
                  const HighsInt presolve_reduction_limit,
                  HighsTimer* timer = nullptr);

  // for MIP presolve
  bool okSetInput(HighsMipSolver& mipsolver,
                  const HighsInt presolve_reduction_limit);

  void setReductionLimit(size_t reductionLimit) {
    this->reductionLimit = reductionLimit;
  }

  HighsInt numNonzeros() const { return int(Avalue.size() - freeslots.size()); }

  void shrinkProblem(HighsPostsolveStack& postsolve_stack);

  void addToMatrix(const HighsInt row, const HighsInt col, const double val);

  Result runProbing(HighsPostsolveStack& postsolve_stack);

  Result dominatedColumns(HighsPostsolveStack& postsolve_stack);

  Result doubletonEq(HighsPostsolveStack& postsolve_stack, HighsInt row,
                     HighsPostsolveStack::RowType rowType);

  Result singletonRow(HighsPostsolveStack& postsolve_stack, HighsInt row);

  Result emptyCol(HighsPostsolveStack& postsolve_stack, HighsInt col);

  Result singletonCol(HighsPostsolveStack& postsolve_stack, HighsInt col);

  Result rowPresolve(HighsPostsolveStack& postsolve_stack, HighsInt row);

  Result colPresolve(HighsPostsolveStack& postsolve_stack, HighsInt col);

  Result initialRowAndColPresolve(HighsPostsolveStack& postsolve_stack);

  HighsModelStatus run(HighsPostsolveStack& postsolve_stack);

  void computeIntermediateMatrix(std::vector<HighsInt>& flagRow,
                                 std::vector<HighsInt>& flagCol,
                                 size_t& numreductions);

  void substitute(HighsInt substcol, HighsInt staycol, double offset,
                  double scale);

  void removeFixedCol(HighsInt col);

  void removeRow(HighsInt row);

  Result removeDependentEquations(HighsPostsolveStack& postsolve_stack);

  Result removeDependentFreeCols(HighsPostsolveStack& postsolve_stack);

  Result aggregator(HighsPostsolveStack& postsolve_stack);

  Result removeRowSingletons(HighsPostsolveStack& postsolve_stack);

  Result presolveColSingletons(HighsPostsolveStack& postsolve_stack);

  Result presolveChangedRows(HighsPostsolveStack& postsolve_stack);

  Result presolveChangedCols(HighsPostsolveStack& postsolve_stack);

  Result removeDoubletonEquations(HighsPostsolveStack& postsolve_stack);

  Result strengthenInequalities(HighsInt& num_strenghtened);

  HighsInt detectImpliedIntegers();

  Result detectParallelRowsAndCols(HighsPostsolveStack& postsolve_stack);

  Result sparsify(HighsPostsolveStack& postsolve_stack);

  void setRelaxedImpliedBounds();

  const HighsPresolveLog& getPresolveLog() const {
    return analysis_.presolve_log_;
  }

  HighsPresolveStatus getPresolveStatus() const { return presolve_status_; }

  HighsInt debugGetCheckCol() const;
  HighsInt debugGetCheckRow() const;

  // Not currently called
  static void debug(const HighsLp& lp, const HighsOptions& options);
};

}  // namespace presolve
#endif
