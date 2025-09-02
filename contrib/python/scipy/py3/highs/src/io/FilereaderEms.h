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
/**@file io/FilereaderEms.h
 * @brief
 */

#ifndef IO_FILEREADER_EMS_H_
#define IO_FILEREADER_EMS_H_

#include <list>

#include "io/Filereader.h"
#include "io/HighsIO.h"  // For messages.

class FilereaderEms : public Filereader {
 public:
  FilereaderRetcode readModelFromFile(const HighsOptions& options,
                                      const std::string filename,
                                      HighsModel& model);
  HighsStatus writeModelToFile(const HighsOptions& options,
                               const std::string filename,
                               const HighsModel& model);
};

#endif
