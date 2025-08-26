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
/**@file lp_data/HSimplexReport.h
 * @brief
 */
#ifndef SIMPLEX_HSIMPLEXREPORT_H_
#define SIMPLEX_HSIMPLEXREPORT_H_

#include "lp_data/HighsOptions.h"
#include "simplex/HSimplex.h"

void reportSimplexPhaseIterations(const HighsLogOptions& log_options,
                                  const HighsInt iteration_count,
                                  HighsSimplexInfo& info,
                                  const bool initialise = false);
#endif  // SIMPLEX_HSIMPLEXREPORT_H_
