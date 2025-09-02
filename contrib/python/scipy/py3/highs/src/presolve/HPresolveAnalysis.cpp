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
#include "lp_data/HighsModelUtils.h"
#include "presolve/HPresolve.h"

void HPresolveAnalysis::setup(const HighsLp* model_,
                              const HighsOptions* options_,
                              const HighsInt& numDeletedRows_,
                              const HighsInt& numDeletedCols_) {
  model = model_;
  options = options_;
  numDeletedRows = &numDeletedRows_;
  numDeletedCols = &numDeletedCols_;

  this->allow_rule_.assign(kPresolveRuleCount, true);

  if (options->presolve_rule_off) {
    // Some presolve rules are off
    //
    // Transform options->presolve_rule_off into logical settings in
    // allow_rule_[*], commenting on the rules switched off
    highsLogUser(options->log_options, HighsLogType::kInfo,
                 "Presolve rules not allowed:\n");
    HighsInt bit = 1;
    for (HighsInt rule_type = kPresolveRuleMin; rule_type < kPresolveRuleCount;
         rule_type++) {
      // Identify whether this rule is allowed
      const bool allow = !(options->presolve_rule_off & bit);
      if (rule_type >= kPresolveRuleFirstAllowOff) {
        // This is a rule that can be switched off, so comment
        // positively if it is off
        allow_rule_[rule_type] = allow;
        if (!allow)
          highsLogUser(options->log_options, HighsLogType::kInfo,
                       "   Rule %2d (bit %4d): %s\n", (int)rule_type, (int)bit,
                       utilPresolveRuleTypeToString(rule_type).c_str());
      } else if (!allow) {
        // This is a rule that cannot be switched off so, if an
        // attempt is made, don't allow it to be off and comment
        // negatively
        highsLogUser(options->log_options, HighsLogType::kWarning,
                     "Cannot disallow rule %2d (bit %4d): %s\n", (int)rule_type,
                     (int)bit, utilPresolveRuleTypeToString(rule_type).c_str());
      }
      bit *= 2;
    }
  }
  // Allow logging if option is set and model is not a MIP
  allow_logging_ = options_->presolve_rule_logging && !model_->isMip();
  logging_on_ = allow_logging_;
  log_rule_type_ = kPresolveRuleIllegal;
  resetNumDeleted();
  presolve_log_.clear();
  original_num_col_ = model->num_col_;
  original_num_row_ = model->num_row_;
}

void HighsPresolveLog::clear() {
  this->rule.resize(kPresolveRuleCount);
  for (HighsInt rule_type = 0; rule_type < kPresolveRuleCount; rule_type++) {
    this->rule[rule_type].call = 0;
    this->rule[rule_type].col_removed = 0;
    this->rule[rule_type].row_removed = 0;
  }
}

void HPresolveAnalysis::resetNumDeleted() {
  num_deleted_rows0_ = 0;
  num_deleted_cols0_ = 0;
}

void HPresolveAnalysis::startPresolveRuleLog(const HighsInt rule_type) {
  const bool debug_print = false;
  assert(logging_on_);
  assert(rule_type >= kPresolveRuleMin && rule_type <= kPresolveRuleMax);
  assert(allow_rule_[rule_type]);
  // Prevent any future calls to "start" until logging is on again
  logging_on_ = false;
  const HighsInt check_rule = kPresolveRuleIllegal;
  if (debug_print)
    printf("   startPresolveRuleLog [%6d, %6d] for (%2d) %s\n",
           int(*numDeletedRows), int(*numDeletedCols), int(rule_type),
           utilPresolveRuleTypeToString(rule_type).c_str());
  if (rule_type == check_rule) {
    printf(">> startPresolveRuleLog [%6d, %6d] for (%2d) %s\n", int(check_rule),
           int(*numDeletedRows), int(*numDeletedCols),
           utilPresolveRuleTypeToString(check_rule).c_str());
  }
  presolve_log_.rule[rule_type].call++;
  // Check that stop has been called since the last start
  assert(log_rule_type_ == kPresolveRuleIllegal);
  log_rule_type_ = rule_type;
  // Check that no un-logged reductions have been performed
  if (num_deleted_rows0_ != *numDeletedRows ||
      num_deleted_cols0_ != *numDeletedCols) {
    printf(
        "ERROR: Model %s: "
        "%d = num_deleted_rows0_ != *numDeletedRows = %d ||"
        "%d = num_deleted_cols0_ != *numDeletedCols = %d\n",
        model->model_name_.c_str(), int(num_deleted_rows0_),
        int(*numDeletedRows), int(num_deleted_cols0_), int(*numDeletedCols));
    fflush(stdout);
  }
  assert(num_deleted_rows0_ == *numDeletedRows);
  assert(num_deleted_cols0_ == *numDeletedCols);
  num_deleted_rows0_ = *numDeletedRows;
  num_deleted_cols0_ = *numDeletedCols;
  const int check_num_deleted_rows0_ = -255;
  const int check_num_deleted_cols0_ = -688;
  if (num_deleted_rows0_ == check_num_deleted_rows0_ &&
      num_deleted_cols0_ == check_num_deleted_cols0_) {
    printf("num_deleted (%d, %d)\n", int(num_deleted_rows0_),
           int(num_deleted_cols0_));
  }
}

void HPresolveAnalysis::stopPresolveRuleLog(const HighsInt rule_type) {
  const bool debug_print = false;
  assert(logging_on_);
  assert(rule_type == log_rule_type_);
  if (debug_print)
    printf("    stopPresolveRuleLog [%6d, %6d] for (%2d) %s\n",
           int(*numDeletedRows), int(*numDeletedCols), int(rule_type),
           utilPresolveRuleTypeToString(rule_type).c_str());
  const int check_rule = kPresolveRuleIllegal;
  if (rule_type == check_rule) {
    printf(">>  stopPresolveRuleLog [%6d, %6d] for (%2d) %s\n", int(check_rule),
           int(*numDeletedRows), int(*numDeletedCols),
           utilPresolveRuleTypeToString(check_rule).c_str());
  }
  const HighsInt num_removed_row = *numDeletedRows - num_deleted_rows0_;
  const HighsInt num_removed_col = *numDeletedCols - num_deleted_cols0_;
  assert(num_removed_row >= 0);
  assert(num_removed_col >= 0);
  presolve_log_.rule[rule_type].col_removed += num_removed_col;
  presolve_log_.rule[rule_type].row_removed += num_removed_row;

  // Set the rule type to be illegal to indicate that stop has been
  // called, and update the record of num_deleted_rows/cols
  log_rule_type_ = kPresolveRuleIllegal;
  num_deleted_rows0_ = *numDeletedRows;
  num_deleted_cols0_ = *numDeletedCols;

  const bool report = false;
  if (report)
    printf("%-25s Call %9d: (%3d, %3d) (%3d, %3d)\n",
           utilPresolveRuleTypeToString(rule_type).c_str(),
           (int)presolve_log_.rule[rule_type].call, (int)num_removed_col,
           (int)num_removed_row, (int)presolve_log_.rule[rule_type].col_removed,
           (int)presolve_log_.rule[rule_type].row_removed);
  const int check_num_deleted_rows0_ = -212;
  const int check_num_deleted_cols0_ = -637;
  if (num_deleted_rows0_ == check_num_deleted_rows0_ &&
      num_deleted_cols0_ == check_num_deleted_cols0_) {
    printf("num_deleted (%d, %d)\n", int(num_deleted_rows0_),
           int(num_deleted_cols0_));
  }
}

bool HPresolveAnalysis::analysePresolveRuleLog(const bool report) {
  if (!allow_logging_) return true;
  const HighsLogOptions& log_options = options->log_options;
  HighsInt sum_removed_row = 0;
  HighsInt sum_removed_col = 0;
  for (HighsInt rule_type = kPresolveRuleMin; rule_type < kPresolveRuleCount;
       rule_type++) {
    sum_removed_row += presolve_log_.rule[rule_type].row_removed;
    sum_removed_col += presolve_log_.rule[rule_type].col_removed;
  }
  if (report && sum_removed_row + sum_removed_col) {
    const std::string rule =
        "-------------------------------------------------------";
    highsLogDev(log_options, HighsLogType::kInfo, "%s\n", rule.c_str());
    highsLogDev(log_options, HighsLogType::kInfo,
                "%-25s      Rows      Cols     Calls\n",
                "Presolve rule removed");
    highsLogDev(log_options, HighsLogType::kInfo, "%s\n", rule.c_str());
    for (HighsInt rule_type = kPresolveRuleMin; rule_type < kPresolveRuleCount;
         rule_type++)
      if (presolve_log_.rule[rule_type].call ||
          presolve_log_.rule[rule_type].row_removed ||
          presolve_log_.rule[rule_type].col_removed)
        highsLogDev(log_options, HighsLogType::kInfo, "%-25s %9d %9d %9d\n",
                    utilPresolveRuleTypeToString(rule_type).c_str(),
                    (int)presolve_log_.rule[rule_type].row_removed,
                    (int)presolve_log_.rule[rule_type].col_removed,
                    (int)presolve_log_.rule[rule_type].call);
    highsLogDev(log_options, HighsLogType::kInfo, "%s\n", rule.c_str());
    highsLogDev(log_options, HighsLogType::kInfo, "%-25s %9d %9d\n",
                "Total reductions", (int)sum_removed_row, (int)sum_removed_col);
    highsLogDev(log_options, HighsLogType::kInfo, "%s\n", rule.c_str());
    highsLogDev(log_options, HighsLogType::kInfo, "%-25s %9d %9d\n",
                "Original  model", (int)original_num_row_,
                (int)original_num_col_);
    highsLogDev(log_options, HighsLogType::kInfo, "%-25s %9d %9d\n",
                "Presolved model", (int)(original_num_row_ - sum_removed_row),
                (int)(original_num_col_ - sum_removed_col));
    highsLogDev(log_options, HighsLogType::kInfo, "%s\n", rule.c_str());
  }
  if (original_num_row_ == model->num_row_ &&
      original_num_col_ == model->num_col_) {
    if (sum_removed_row != *numDeletedRows) {
      highsLogDev(log_options, HighsLogType::kError,
                  "%d = sum_removed_row != numDeletedRows = %d\n",
                  (int)sum_removed_row, (int)*numDeletedRows);
      fflush(stdout);
      assert(sum_removed_row == *numDeletedRows);
      return false;
    }
    if (sum_removed_col != *numDeletedCols) {
      highsLogDev(log_options, HighsLogType::kError,
                  "%d = sum_removed_col != numDeletedCols = %d\n",
                  (int)sum_removed_col, (int)*numDeletedCols);
      fflush(stdout);
      assert(sum_removed_col == *numDeletedCols);
      return false;
    }
  }
  return true;
}
