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
/**@file PresolveComponent.cpp
 * @brief The HiGHS class
 */

#include "presolve/PresolveComponent.h"

#include "presolve/HPresolve.h"

HighsStatus PresolveComponent::init(const HighsLp& lp, HighsTimer& timer,
                                    bool mip) {
  data_.postSolveStack.initializeIndexMaps(lp.num_row_, lp.num_col_);
  data_.reduced_lp_ = lp;
  this->timer = &timer;
  return HighsStatus::kOk;
}

void PresolveComponent::negateReducedLpColDuals() {
  for (HighsInt col = 0; col < data_.reduced_lp_.num_col_; col++)
    data_.recovered_solution_.col_dual[col] =
        -data_.recovered_solution_.col_dual[col];
  return;
}

HighsPresolveStatus PresolveComponent::run() {
  presolve::HPresolve presolve;
  if (!presolve.okSetInput(data_.reduced_lp_, *options_,
                           options_->presolve_reduction_limit, timer)) {
    presolve_status_ = HighsPresolveStatus::kOutOfMemory;
    return presolve_status_;
  }

  presolve.run(data_.postSolveStack);
  data_.presolve_log_ = presolve.getPresolveLog();
  presolve_status_ = presolve.getPresolveStatus();
  return presolve_status_;
}

void PresolveComponent::clear() { data_.clear(); }
