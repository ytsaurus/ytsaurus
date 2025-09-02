#ifndef __SRC_LIB_QPSOLVER_QUASS_HPP__
#define __SRC_LIB_QPSOLVER_QUASS_HPP__

#include "Highs.h"
#include "qpsolver/a_asm.hpp"
#include "qpsolver/instance.hpp"
#include "qpsolver/qpconst.hpp"
#include "qpsolver/settings.hpp"

QpAsmStatus solveqp(Instance& instance, Settings& settings, Statistics& stats,
                    HighsModelStatus& highs_model_status,
                    HighsBasis& highs_basis, HighsSolution& highs_solution,
                    HighsTimer& timer);

#endif
