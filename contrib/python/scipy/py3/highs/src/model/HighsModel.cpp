/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/*                                                                       */
/*    This file is part of the HiGHS linear optimization suite           */
/*                                                                       */
/*    Written and engineered 2008-2021 at the University of Edinburgh    */
/*                                                                       */
/*    Available as open-source under the MIT License                     */
/*                                                                       */
/*    Authors: Julian Hall, Ivet Galabova, Qi Huangfu, Leona Gottwald    */
/*    and Michael Feldmeier                                              */
/*                                                                       */
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/**@file lp_data/HighsModel.cpp
 * @brief
 */
#include "model/HighsModel.h"

#include <cassert>

bool HighsModel::operator==(const HighsModel& model) const {
  bool equal = equalButForNames(model);
  equal = this->lp_.equalNames(model.lp_) && equal;
  return equal;
}

bool HighsModel::equalButForNames(const HighsModel& model) const {
  bool equal = this->lp_.equalButForNames(model.lp_);
  equal = this->hessian_ == model.hessian_ && equal;
  return equal;
}

bool HighsModel::userCostScaleOk(const HighsInt user_cost_scale,
                                 const double small_matrix_value,
                                 const double large_matrix_value,
                                 const double infinite_cost) const {
  const HighsInt dl_user_cost_scale =
      user_cost_scale - this->lp_.user_cost_scale_;
  if (!dl_user_cost_scale) return true;
  if (this->hessian_.dim_ &&
      !this->hessian_.scaleOk(dl_user_cost_scale, small_matrix_value,
                              large_matrix_value))
    return false;
  return this->lp_.userCostScaleOk(user_cost_scale, infinite_cost);
}

void HighsModel::userCostScale(const HighsInt user_cost_scale) {
  const HighsInt dl_user_cost_scale =
      user_cost_scale - this->lp_.user_cost_scale_;
  if (!dl_user_cost_scale) return;
  double dl_user_cost_scale_value = std::pow(2, dl_user_cost_scale);
  if (this->hessian_.dim_) {
    for (HighsInt iEl = 0; iEl < this->hessian_.start_[this->hessian_.dim_];
         iEl++)
      this->hessian_.value_[iEl] *= dl_user_cost_scale_value;
  }
  this->lp_.userCostScale(user_cost_scale);
}

void HighsModel::clear() {
  this->lp_.clear();
  this->hessian_.clear();
}

double HighsModel::objectiveValue(const std::vector<double>& solution) const {
  return this->hessian_.objectiveValue(solution) +
         this->lp_.objectiveValue(solution);
}

void HighsModel::objectiveGradient(const std::vector<double>& solution,
                                   std::vector<double>& gradient) const {
  if (this->hessian_.dim_ > 0) {
    this->hessian_.product(solution, gradient);
  } else {
    gradient.assign(this->lp_.num_col_, 0);
  }
  for (HighsInt iCol = 0; iCol < this->lp_.num_col_; iCol++)
    gradient[iCol] += this->lp_.col_cost_[iCol];
}
