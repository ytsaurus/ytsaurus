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
/**@file lp_data/HighsIis.h
 * @brief Class-independent utilities for HiGHS
 */
#ifndef LP_DATA_HIGHSIIS_H_
#define LP_DATA_HIGHSIIS_H_

#include "lp_data/HighsLp.h"

const bool kIisDevReport = false;

enum IisBoundStatus {
  kIisBoundStatusDropped = -1,
  kIisBoundStatusNull,   // 0
  kIisBoundStatusFree,   // 1
  kIisBoundStatusLower,  // 2
  kIisBoundStatusUpper,  // 3
  kIisBoundStatusBoxed   // 4
};

struct HighsIisInfo {
  double simplex_time = 0;
  HighsInt simplex_iterations = 0;
};

class HighsIis {
 public:
  HighsIis() {}

  void invalidate();
  std::string iisBoundStatusToString(HighsInt bound_status) const;
  void report(const std::string message, const HighsLp& lp) const;
  void addCol(const HighsInt col, const HighsInt status = kIisBoundStatusNull);
  void addRow(const HighsInt row, const HighsInt status = kIisBoundStatusNull);
  void removeCol(const HighsInt col);
  void removeRow(const HighsInt row);
  HighsStatus getData(const HighsLp& lp, const HighsOptions& options,
                      const HighsBasis& basis,
                      const std::vector<HighsInt>& infeasible_row);

  HighsStatus compute(const HighsLp& lp, const HighsOptions& options,
                      const HighsBasis* basis = nullptr);

  bool trivial(const HighsLp& lp, const HighsOptions& options);

  // Data members
  bool valid_ = false;
  HighsInt strategy_ = kIisStrategyMin;
  std::vector<HighsInt> col_index_;
  std::vector<HighsInt> row_index_;
  std::vector<HighsInt> col_bound_;
  std::vector<HighsInt> row_bound_;
  std::vector<HighsIisInfo> info_;
};

#endif  // LP_DATA_HIGHSIIS_H_
