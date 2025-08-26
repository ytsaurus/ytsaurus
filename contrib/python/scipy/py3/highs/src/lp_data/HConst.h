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
/**@file lp_data/HConst.h
 * @brief Constants for HiGHS
 */
#ifndef LP_DATA_HCONST_H_
#define LP_DATA_HCONST_H_

#include <cmath>
#include <limits>
#include <string>

#include "util/HighsInt.h"

const std::string kHighsCopyrightStatement =
    "Copyright (c) 2024 HiGHS under MIT licence terms";

const size_t kHighsSize_tInf = std::numeric_limits<size_t>::max();
const HighsInt kHighsIInf = std::numeric_limits<HighsInt>::max();
const HighsInt kHighsIInf32 = std::numeric_limits<int>::max();
const double kHighsInf = std::numeric_limits<double>::infinity();
const double kHighsUndefined = kHighsInf;
const double kHighsTiny = 1e-14;
const double kHighsMacheps = std::ldexp(1, -52);
const double kHighsZero = 1e-50;
const std::string kHighsOffString = "off";
const std::string kHighsChooseString = "choose";
const std::string kHighsOnString = "on";
const HighsInt kHighsMaxStringLength = 512;
const HighsInt kSimplexConcurrencyLimit = 8;
const double kRunningAverageMultiplier = 0.05;
const double kExcessivelyLargeBoundValue = 1e10;
const double kExcessivelyLargeCostValue = 1e10;
const double kExcessivelySmallBoundValue = 1e-4;
const double kExcessivelySmallCostValue = 1e-4;

const bool kAllowDeveloperAssert = false;
const bool kExtendInvertWhenAddingRows = false;

enum class HighsLogType { kInfo = 1, kDetailed, kVerbose, kWarning, kError };

enum SimplexScaleStrategy {
  kSimplexScaleStrategyMin = 0,
  kSimplexScaleStrategyOff = kSimplexScaleStrategyMin,  // 0
  kSimplexScaleStrategyChoose,                          // 1
  kSimplexScaleStrategyEquilibration,                   // 2
  kSimplexScaleStrategyForcedEquilibration,             // 3
  kSimplexScaleStrategyMaxValue015,                     // 4
  kSimplexScaleStrategyMaxValue0157,                    // 5
  kSimplexScaleStrategyMax = kSimplexScaleStrategyMaxValue0157
};

enum HighsDebugLevel {
  kHighsDebugLevelNone = 0,
  kHighsDebugLevelCheap,
  kHighsDebugLevelCostly,
  kHighsDebugLevelExpensive,
  kHighsDebugLevelMin = kHighsDebugLevelNone,
  kHighsDebugLevelMax = kHighsDebugLevelExpensive
};

enum class HighsDebugStatus {
  kNotChecked = -1,
  kOk,
  kSmallError,
  kWarning,
  kLargeError,
  kError,
  kExcessiveError,
  kLogicalError,
};

enum HighsAnalysisLevel {
  kHighsAnalysisLevelNone = 0,
  kHighsAnalysisLevelModelData = 1,
  kHighsAnalysisLevelSolverSummaryData = 2,
  kHighsAnalysisLevelSolverRuntimeData = 4,
  kHighsAnalysisLevelSolverTime = 8,
  kHighsAnalysisLevelNlaData = 16,
  kHighsAnalysisLevelNlaTime = 32,
  kHighsAnalysisLevelMipData = 64,
  kHighsAnalysisLevelMipTime = 128,
  kHighsAnalysisLevelMin = kHighsAnalysisLevelNone,
  kHighsAnalysisLevelMax =
      kHighsAnalysisLevelModelData + kHighsAnalysisLevelSolverSummaryData +
      kHighsAnalysisLevelSolverRuntimeData + kHighsAnalysisLevelSolverTime +
      kHighsAnalysisLevelNlaData + kHighsAnalysisLevelNlaTime +
      kHighsAnalysisLevelMipData + kHighsAnalysisLevelMipTime
};

enum class HighsVarType : uint8_t {
  kContinuous = 0,
  kInteger = 1,
  kSemiContinuous = 2,
  kSemiInteger = 3,
  kImplicitInteger = 4,
};

enum class HighsOptionType { kBool = 0, kInt, kDouble, kString };

enum class HighsInfoType { kInt64 = -1, kInt = 1, kDouble };

enum OptionOffChooseOn {
  kHighsOptionOff = -1,
  kHighsOptionChoose,
  kHighsOptionOn
};

enum IpxDualizeStrategy {
  kIpxDualizeStrategyOff = kHighsOptionOff,
  kIpxDualizeStrategyChoose = kHighsOptionChoose,
  kIpxDualizeStrategyOn = kHighsOptionOn,
  kIpxDualizeStrategyLukas,
  kIpxDualizeStrategyFilippo,
  kIpxDualizeStrategyMin = kIpxDualizeStrategyOff,
  kIpxDualizeStrategyMax = kIpxDualizeStrategyFilippo,
};

/** SCIP/HiGHS Objective sense */
enum class ObjSense { kMinimize = 1, kMaximize = -1 };

enum class MatrixFormat { kColwise = 1, kRowwise, kRowwisePartitioned };

enum class HessianFormat { kTriangular = 1, kSquare };

enum SolutionStatus {
  kSolutionStatusNone = 0,
  kSolutionStatusInfeasible,
  kSolutionStatusFeasible,
  kSolutionStatusMin = kSolutionStatusNone,
  kSolutionStatusMax = kSolutionStatusFeasible
};

enum BasisValidity {
  kBasisValidityInvalid = 0,
  kBasisValidityValid,
  kBasisValidityMin = kBasisValidityInvalid,
  kBasisValidityMax = kBasisValidityValid
};

enum SolutionStyle {
  kSolutionStyleOldRaw = -1,
  kSolutionStyleRaw = 0,
  kSolutionStylePretty,        // 1;
  kSolutionStyleGlpsolRaw,     // 2;
  kSolutionStyleGlpsolPretty,  // 3;
  kSolutionStyleSparse,        // 4;
  kSolutionStyleMin = kSolutionStyleOldRaw,
  kSolutionStyleMax = kSolutionStyleSparse
};

enum GlpsolCostRowLocation {
  kGlpsolCostRowLocationLast = -2,
  kGlpsolCostRowLocationNone,         // -1
  kGlpsolCostRowLocationNoneIfEmpty,  // 0
  kGlpsolCostRowLocationMin = kGlpsolCostRowLocationLast
};

const std::string kHighsFilenameDefault = "";

enum class HighsPresolveStatus {
  kNotPresolved = -1,
  kNotReduced,
  kInfeasible,
  kUnboundedOrInfeasible,
  kReduced,
  kReducedToEmpty,
  kTimeout,
  kNullError,     // V2.0: Delete since it's not used!
  kOptionsError,  // V2.0: Delete since it's not used!
  kNotSet,
  kOutOfMemory,  // V2.0: Move above kNotSet
};

enum class HighsPostsolveStatus {  // V2.0: Delete if not used!
  kNotPresolved = -1,
  kNoPrimalSolutionError,
  kSolutionRecovered,
  kBasisError
};

enum class HighsModelStatus {
  // NB Add new status values to the end so that int cast of status
  // values is unchanged, since enums are not preserved in some
  // interfaces
  kNotset = 0,
  kLoadError,  // V2.0: Delete since it's not used!
  kModelError,
  kPresolveError,  // V2.0: Delete since it's not used!
  kSolveError,
  kPostsolveError,  // V2.0: Delete if not used! Add to documentation if used
  kModelEmpty,
  kOptimal,
  kInfeasible,
  kUnboundedOrInfeasible,
  kUnbounded,
  kObjectiveBound,
  kObjectiveTarget,
  kTimeLimit,
  kIterationLimit,
  // V2.0: put kUnknown after kSolutionLimit and kInterrupt - and then
  // modify kMax and highs_c_api.h, highs_csharp_api.cs,
  // highspy/highs_bindings.cpp
  kUnknown,
  kSolutionLimit,
  kInterrupt,
  kMemoryLimit,
  kMin = kNotset,
  kMax = kMemoryLimit
};

enum HighsCallbackType : int {
  kCallbackMin = 0,
  kCallbackLogging = kCallbackMin,    // 0
  kCallbackSimplexInterrupt,          // 1
  kCallbackIpmInterrupt,              // 2
  kCallbackMipSolution,               // 3
  kCallbackMipImprovingSolution,      // 4
  kCallbackMipLogging,                // 5
  kCallbackMipInterrupt,              // 6
  kCallbackMipGetCutPool,             // 7
  kCallbackMipDefineLazyConstraints,  // 8
  kCallbackMax = kCallbackMipDefineLazyConstraints,
  kNumCallbackType
};

/** SCIP/CPLEX-like HiGHS basis status for columns and rows. */
enum class HighsBasisStatus : uint8_t {
  kLower =
      0,   // (slack) variable is at its lower bound [including fixed variables]
  kBasic,  // (slack) variable is basic
  kUpper,  // (slack) variable is at its upper bound
  kZero,   // free variable is nonbasic and set to zero
  kNonbasic  // nonbasic with no specific bound information - useful for users
             // and postsolve
};

// Types of LP presolve rules
enum PresolveRuleType : int {
  kPresolveRuleIllegal = -1,
  kPresolveRuleMin = 0,
  kPresolveRuleEmptyRow = kPresolveRuleMin,
  kPresolveRuleSingletonRow,
  kPresolveRuleRedundantRow,
  kPresolveRuleEmptyCol,
  kPresolveRuleFixedCol,
  kPresolveRuleDominatedCol,
  // The remaining rules can be switched off
  kPresolveRuleFirstAllowOff,
  kPresolveRuleForcingRow = kPresolveRuleFirstAllowOff,
  kPresolveRuleForcingCol,
  kPresolveRuleFreeColSubstitution,
  kPresolveRuleDoubletonEquation,
  kPresolveRuleDependentEquations,
  kPresolveRuleDependentFreeCols,
  kPresolveRuleAggregator,
  kPresolveRuleParallelRowsAndCols,
  kPresolveRuleMax = kPresolveRuleParallelRowsAndCols,
  kPresolveRuleLastAllowOff = kPresolveRuleMax,
  kPresolveRuleCount,
};

enum IisStrategy {
  kIisStrategyMin = 0,
  kIisStrategyFromLpRowPriority = kIisStrategyMin,  // 0
  kIisStrategyFromLpColPriority,                    // 1
  //  kIisStrategyFromRayRowPriority,                     // 2
  //  kIisStrategyFromRayColPriority,                     // 3
  kIisStrategyMax = kIisStrategyFromLpColPriority
};

// Default and max allowed power-of-two matrix scale factor
const HighsInt kDefaultAllowedMatrixPow2Scale = 20;
const HighsInt kMaxAllowedMatrixPow2Scale = 30;

// Illegal values of num/max/sum infeasibility - used to indicate that true
// values aren't known
const double kHighsIllegalInfeasibilityMeasure = kHighsInf;
const HighsInt kHighsIllegalInfeasibilityCount = -1;

// Illegal values for HighsError - used to indicate that true
// values aren't known
const double kHighsIllegalErrorValue = kHighsInf;
const HighsInt kHighsIllegalErrorIndex = -1;

// Illegal values for complementarity violations used to indicate that true
// values aren't known
const double kHighsIllegalComplementarityViolation = kHighsInf;

// Maximum upper bound on semi-variables
const double kMaxSemiVariableUpper = 1e5;

// Limit on primal values being realistic
const double kExcessivePrimalValue = 1e25;

// Hash marker for duplicates
const HighsInt kHashIsDuplicate = -1;

// Tolerance values for highsDoubleToString
const double kModelValueToStringTolerance = 1e-15;
const double kRangingValueToStringTolerance = 1e-13;
const double kHighsSolutionValueToStringTolerance = 1e-13;
const double kGlpsolSolutionValueToStringTolerance = 1e-12;

// Termination link in linked lists
const HighsInt kNoLink = -1;

const int8_t kPivotIllegal = -1;
const int8_t kPivotLogical = 0;
const int8_t kPivotUnit = 1;
const int8_t kPivotRowSingleton = 2;
const int8_t kPivotColSingleton = 3;
const int8_t kPivotMarkowitz = 4;
#endif /* LP_DATA_HCONST_H_ */
