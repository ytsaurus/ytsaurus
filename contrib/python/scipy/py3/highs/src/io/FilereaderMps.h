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
/**@file io/FilereaderMps.h
 * @brief
 */
#ifndef IO_FILEREADER_MPS_H_
#define IO_FILEREADER_MPS_H_

#include "io/Filereader.h"
#include "lp_data/HighsStatus.h"

class FilereaderMps : public Filereader {
 public:
  FilereaderRetcode readModelFromFile(const HighsOptions& options,
                                      const std::string filename,
                                      HighsModel& model);
  HighsStatus writeModelToFile(const HighsOptions& options,
                               const std::string filename,
                               const HighsModel& model);
};

#endif
