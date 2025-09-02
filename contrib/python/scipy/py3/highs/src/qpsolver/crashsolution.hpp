#ifndef __SRC_LIB_CRASHSOLUTION_HPP__
#define __SRC_LIB_CRASHSOLUTION_HPP__

#include <cstdlib>

#include "runtime.hpp"

inline bool isfreevar(Instance& instance, HighsInt idx) {
  return instance.var_lo[idx] == -std::numeric_limits<double>::infinity() &&
         instance.var_up[idx] == std::numeric_limits<double>::infinity();
}

#endif
