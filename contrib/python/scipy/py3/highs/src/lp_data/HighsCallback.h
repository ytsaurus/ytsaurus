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
/**@file lp_data/HighsCallback.h
 * @brief
 */
#ifndef LP_DATA_HIGHSCALLBACK_H_
#define LP_DATA_HIGHSCALLBACK_H_

#include <functional>

#include "lp_data/HStruct.h"
#include "lp_data/HighsCallbackStruct.h"

using HighsCallbackFunctionType =
    std::function<void(int, const std::string&, const HighsCallbackDataOut*,
                       HighsCallbackDataIn*, void*)>;

struct HighsCallback {
  // Function pointers cannot be used for Pybind11, so use std::function
  HighsCallbackFunctionType user_callback = nullptr;
  HighsCCallbackType c_callback = nullptr;
  void* user_callback_data = nullptr;
  std::vector<bool> active;
  HighsCallbackDataOut data_out;
  HighsCallbackDataIn data_in;
  bool callbackActive(const int callback_type);
  bool callbackAction(const int callback_type, std::string message = "");
  void clearHighsCallbackDataOut();
  void clearHighsCallbackDataIn();
  void clear();
};
#endif /* LP_DATA_HIGHSCALLBACK_H_ */
