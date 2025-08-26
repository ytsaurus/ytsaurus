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
/**@file lp_data/HStruct.h
 * @brief Structs for HiGHS
 */
#ifndef LP_DATA_HSTRUCT_H_
#define LP_DATA_HSTRUCT_H_

#include <unordered_map>
#include <vector>

#include "lp_data/HConst.h"

struct HighsIterationCounts {
  HighsInt simplex = 0;
  HighsInt ipm = 0;
  HighsInt crossover = 0;
  HighsInt qp = 0;
};

struct HighsSolution {
  bool value_valid = false;
  bool dual_valid = false;
  std::vector<double> col_value;
  std::vector<double> col_dual;
  std::vector<double> row_value;
  std::vector<double> row_dual;
  bool hasUndefined();
  void invalidate();
  void clear();
};

struct HighsObjectiveSolution {
  double objective;
  std::vector<double> col_value;
  void clear();
};

struct RefactorInfo {
  bool use = false;
  std::vector<HighsInt> pivot_row;
  std::vector<HighsInt> pivot_var;
  std::vector<int8_t> pivot_type;
  double build_synthetic_tick;
  void clear();
};

struct HotStart {
  bool valid = false;
  RefactorInfo refactor_info;
  std::vector<int8_t> nonbasicMove;
  void clear();
};

struct HighsBasis {
  bool valid = false;
  bool alien = true;
  bool was_alien = true;
  HighsInt debug_id = -1;
  HighsInt debug_update_count = -1;
  std::string debug_origin_name = "None";
  std::vector<HighsBasisStatus> col_status;
  std::vector<HighsBasisStatus> row_status;
  void invalidate();
  void clear();
};

struct HighsScale {
  HighsInt strategy;
  bool has_scaling;
  HighsInt num_col;
  HighsInt num_row;
  double cost;
  std::vector<double> col;
  std::vector<double> row;
};

struct HighsLpMods {
  // Semi-variables with zero lower bound that are treated as non-semi
  std::vector<HighsInt> save_non_semi_variable_index;

  // Semi-variables with inconsistent bounds that are fixed at zero
  std::vector<HighsInt> save_inconsistent_semi_variable_index;
  std::vector<double> save_inconsistent_semi_variable_lower_bound_value;
  std::vector<double> save_inconsistent_semi_variable_upper_bound_value;
  std::vector<HighsVarType> save_inconsistent_semi_variable_type;

  // Semi-variables whose lower bound is ignored when solving the
  // relaxation
  std::vector<HighsInt> save_relaxed_semi_variable_lower_bound_index;
  std::vector<double> save_relaxed_semi_variable_lower_bound_value;

  // Semi-variables whose upper bound is too large to be used as a
  // big-M when converting them to an integer variables plus an
  // integer/continuous variables as appropriate
  std::vector<HighsInt> save_tightened_semi_variable_upper_bound_index;
  std::vector<double> save_tightened_semi_variable_upper_bound_value;

  // Variables with infinite costs that are fixed during solve
  std::vector<HighsInt> save_inf_cost_variable_index;
  std::vector<double> save_inf_cost_variable_cost;
  std::vector<double> save_inf_cost_variable_lower;
  std::vector<double> save_inf_cost_variable_upper;

  void clear();
  bool isClear();
};

struct HighsNameHash {
  std::unordered_map<std::string, int> name2index;
  void form(const std::vector<std::string>& name);
  bool hasDuplicate(const std::vector<std::string>& name);
  void update(int index, const std::string& old_name,
              const std::string& new_name);
  void clear();
};

struct HighsPresolveRuleLog {
  HighsInt call;
  HighsInt col_removed;
  HighsInt row_removed;
};

struct HighsPresolveLog {
  std::vector<HighsPresolveRuleLog> rule;
  void clear();
};

struct HighsIllConditioningRecord {
  HighsInt index;
  double multiplier;
};

struct HighsIllConditioning {
  std::vector<HighsIllConditioningRecord> record;
  void clear();
};

#endif /* LP_DATA_HSTRUCT_H_ */
