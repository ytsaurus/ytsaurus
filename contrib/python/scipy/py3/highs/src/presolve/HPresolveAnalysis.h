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
/**@file presolve/HPresolveAnalysis.h
 * @brief
 */
#ifndef PRESOLVE_HIGHS_PRESOLVE_ANALYSIS_H_
#define PRESOLVE_HIGHS_PRESOLVE_ANALYSIS_H_

class HPresolveAnalysis {
  const HighsLp* model;
  const HighsOptions* options;
  const bool* allow_rule;
  const HighsInt* numDeletedRows;
  const HighsInt* numDeletedCols;

  // store original problem sizes for reference
  HighsInt original_num_col_;
  HighsInt original_num_row_;

 public:
  std::vector<bool> allow_rule_;

  bool allow_logging_;
  bool logging_on_;

  int log_rule_type_;
  HighsInt num_deleted_rows0_;
  HighsInt num_deleted_cols0_;
  HighsPresolveLog presolve_log_;

  // for LP presolve
  //
  // Transform options->presolve_rule_off into logical settings in
  // allow_rule_[*], commenting on the rules switched off
  void setup(const HighsLp* model_, const HighsOptions* options_,
             const HighsInt& numDeletedRows_, const HighsInt& numDeletedCols_);
  void resetNumDeleted();

  std::string presolveReductionTypeToString(const HighsInt reduction_type);
  void startPresolveRuleLog(const HighsInt rule_type);
  void stopPresolveRuleLog(const HighsInt rule_type);
  bool analysePresolveRuleLog(const bool report = false);
  friend class HPresolve;
};

#endif
