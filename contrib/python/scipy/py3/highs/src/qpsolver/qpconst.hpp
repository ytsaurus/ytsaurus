#pragma once
#ifndef __SRC_LIB_QPCONST_HPP__
#define __SRC_LIB_QPCONST_HPP__

enum class QpSolverStatus { OK, NOTPOSITIVDEFINITE, DEGENERATE };

enum class QpModelStatus {
  kNotset,  // 0
  kUndetermined,
  kOptimal,
  kUnbounded,
  kInfeasible,
  kIterationLimit,
  kTimeLimit,
  kLargeNullspace,
  kInterrupt,
  kError
};

enum class BasisStatus {
  kInactive,
  kActiveAtLower = 1,
  kActiveAtUpper,
  kInactiveInBasis
};

#endif
