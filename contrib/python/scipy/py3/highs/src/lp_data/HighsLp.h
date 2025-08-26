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
/**@file lp_data/HighsLp.h
 * @brief
 */
#ifndef LP_DATA_HIGHS_LP_H_
#define LP_DATA_HIGHS_LP_H_

#include <string>

#include "lp_data/HStruct.h"
#include "util/HighsSparseMatrix.h"

class HighsLp {
 public:
  HighsLp() { clear(); }
  // Model data
  HighsInt num_col_;
  HighsInt num_row_;

  std::vector<double> col_cost_;
  std::vector<double> col_lower_;
  std::vector<double> col_upper_;
  std::vector<double> row_lower_;
  std::vector<double> row_upper_;

  HighsSparseMatrix a_matrix_;

  ObjSense sense_;
  double offset_;

  std::string model_name_;
  std::string objective_name_;

  HighsInt new_col_name_ix_ = 0;
  HighsInt new_row_name_ix_ = 0;
  std::vector<std::string> col_names_;
  std::vector<std::string> row_names_;

  std::vector<HighsVarType> integrality_;

  HighsNameHash col_hash_;
  HighsNameHash row_hash_;

  HighsInt user_bound_scale_;
  HighsInt user_cost_scale_;
  HighsScale scale_;
  bool is_scaled_;
  bool is_moved_;
  HighsInt cost_row_location_;
  bool has_infinite_cost_;
  HighsLpMods mods_;

  bool operator==(const HighsLp& lp) const;
  bool equalButForNames(const HighsLp& lp) const;
  bool equalButForScalingAndNames(const HighsLp& lp) const;
  bool equalNames(const HighsLp& lp) const;
  bool equalScaling(const HighsLp& lp) const;
  bool isMip() const;
  bool hasSemiVariables() const;
  bool hasInfiniteCost(const double infinite_cost) const;
  bool hasMods() const;
  bool needsMods(const double infinite_cost) const;
  double objectiveValue(const std::vector<double>& solution) const;
  HighsCDouble objectiveCDoubleValue(const std::vector<double>& solution) const;
  void setMatrixDimensions();
  void setFormat(const MatrixFormat format);
  void ensureColwise() { this->a_matrix_.ensureColwise(); };
  void ensureRowwise() { this->a_matrix_.ensureRowwise(); };
  void clearScaling();
  void resetScale();
  void clearScale();
  void applyScale();
  void unapplyScale();
  void moveBackLpAndUnapplyScaling(HighsLp& lp);
  bool userBoundScaleOk(const HighsInt user_bound_scale,
                        const double infinite_bound) const;
  void userBoundScale(const HighsInt user_bound_scale);
  bool userCostScaleOk(const HighsInt user_cost_scale,
                       const double infinite_cost) const;
  void userCostScale(const HighsInt user_cost_scale);
  void exactResize();
  void addColNames(const std::string name, const HighsInt num_new_col = 1);
  void addRowNames(const std::string name, const HighsInt num_new_row = 1);
  void deleteColsFromVectors(HighsInt& new_num_col,
                             const HighsIndexCollection& index_collection);
  void deleteRowsFromVectors(HighsInt& new_num_row,
                             const HighsIndexCollection& index_collection);
  void deleteCols(const HighsIndexCollection& index_collection);
  void deleteRows(const HighsIndexCollection& index_collection);
  void unapplyMods();
  void clear();
};

#endif
