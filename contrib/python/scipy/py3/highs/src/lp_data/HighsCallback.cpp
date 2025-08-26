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
/**@file lp_data/HighsCallback.cpp
 * @brief
 */
#include "lp_data/HighsCallback.h"

#include <cassert>

void HighsCallback::clearHighsCallbackDataOut() {
  this->data_out.log_type = -1;
  this->data_out.running_time = -1;
  this->data_out.simplex_iteration_count = -1;
  this->data_out.ipm_iteration_count = -1;
  this->data_out.pdlp_iteration_count = -1;
  this->data_out.objective_function_value = -kHighsInf;
  this->data_out.mip_node_count = -1;
  this->data_out.mip_primal_bound = kHighsInf;
  this->data_out.mip_dual_bound = -kHighsInf;
  this->data_out.mip_gap = -1;
  this->data_out.mip_solution = nullptr;
}

void HighsCallback::clearHighsCallbackDataIn() {
  this->data_in.user_interrupt = false;
}

void HighsCallback::clear() {
  this->user_callback = nullptr;
  this->user_callback_data = nullptr;
  this->active.assign(kNumCallbackType, false);
  this->clearHighsCallbackDataOut();
  this->clearHighsCallbackDataIn();
}

bool HighsCallback::callbackActive(const int callback_type) {
  // Check that callback function has been defined
  if (!this->user_callback) return false;
  // Check that callback_type is within range
  const bool callback_type_ok =
      callback_type >= kCallbackMin && callback_type <= kCallbackMax;
  assert(callback_type_ok);
  if (!callback_type_ok) return false;
  // Don't call callback if it is not active
  assert(this->active.size() > 0);
  if (!this->active[callback_type]) return false;
  return true;
}

bool HighsCallback::callbackAction(const int callback_type,
                                   std::string message) {
  if (!callbackActive(callback_type)) return false;
  this->user_callback(callback_type, message.c_str(), &this->data_out,
                      &this->data_in, this->user_callback_data);
  // Assess any action
  bool action = this->data_in.user_interrupt;

  // Check for no action if case not handled internally
  if (callback_type == kCallbackMipImprovingSolution ||
      callback_type == kCallbackMipSolution ||
      callback_type == kCallbackMipLogging ||
      callback_type == kCallbackMipGetCutPool ||
      callback_type == kCallbackMipDefineLazyConstraints)
    assert(!action);
  return action;
}
