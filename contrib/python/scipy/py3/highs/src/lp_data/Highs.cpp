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
/**@file lp_data/Highs.cpp
 * @brief
 */
#include "Highs.h"

#include <algorithm>
#include <cassert>
#include <csignal>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>

#include "io/Filereader.h"
#include "io/LoadOptions.h"
#include "lp_data/HighsCallbackStruct.h"
#include "lp_data/HighsInfoDebug.h"
#include "lp_data/HighsLpSolverObject.h"
#include "lp_data/HighsSolve.h"
#include "mip/HighsMipSolver.h"
#include "model/HighsHessianUtils.h"
#include "parallel/HighsParallel.h"
#include "presolve/ICrashX.h"
#include "qpsolver/a_quass.hpp"
#include "qpsolver/runtime.hpp"
#include "simplex/HSimplex.h"
#include "simplex/HSimplexDebug.h"
#include "util/HighsMatrixPic.h"
#include "util/HighsSort.h"

#define STRINGFY(s) STRINGFY0(s)
#define STRINGFY0(s) #s
const char* highsVersion() {
  return STRINGFY(HIGHS_VERSION_MAJOR) "." STRINGFY(
      HIGHS_VERSION_MINOR) "." STRINGFY(HIGHS_VERSION_PATCH);
}
HighsInt highsVersionMajor() { return HIGHS_VERSION_MAJOR; }
HighsInt highsVersionMinor() { return HIGHS_VERSION_MINOR; }
HighsInt highsVersionPatch() { return HIGHS_VERSION_PATCH; }
const char* highsGithash() { return HIGHS_GITHASH; }
const char* highsCompilationDate() { return "deprecated"; }

Highs::Highs() {}

HighsStatus Highs::clear() {
  resetOptions();
  return clearModel();
}

HighsStatus Highs::clearModel() {
  model_.clear();
  return clearSolver();
}

HighsStatus Highs::clearSolver() {
  HighsStatus return_status = HighsStatus::kOk;
  clearPresolve();
  clearStandardFormLp();
  invalidateUserSolverData();
  return returnFromHighs(return_status);
}

HighsStatus Highs::setOptionValue(const std::string& option, const bool value) {
  if (setLocalOptionValue(options_.log_options, option, options_.records,
                          value) == OptionStatus::kOk)
    return optionChangeAction();
  return HighsStatus::kError;
}

HighsStatus Highs::setOptionValue(const std::string& option,
                                  const HighsInt value) {
  if (setLocalOptionValue(options_.log_options, option, options_.records,
                          value) == OptionStatus::kOk)
    return optionChangeAction();
  return HighsStatus::kError;
}

HighsStatus Highs::setOptionValue(const std::string& option,
                                  const double value) {
  if (setLocalOptionValue(options_.log_options, option, options_.records,
                          value) == OptionStatus::kOk)
    return optionChangeAction();
  return HighsStatus::kError;
}

HighsStatus Highs::setOptionValue(const std::string& option,
                                  const std::string& value) {
  HighsLogOptions report_log_options = options_.log_options;
  if (setLocalOptionValue(report_log_options, option, options_.log_options,
                          options_.records, value) == OptionStatus::kOk)
    return optionChangeAction();
  return HighsStatus::kError;
}

HighsStatus Highs::setOptionValue(const std::string& option,
                                  const char* value) {
  HighsLogOptions report_log_options = options_.log_options;
  if (setLocalOptionValue(report_log_options, option, options_.log_options,
                          options_.records, value) == OptionStatus::kOk)
    return optionChangeAction();
  return HighsStatus::kError;
}

HighsStatus Highs::readOptions(const std::string& filename) {
  if (filename.size() <= 0) {
    highsLogUser(options_.log_options, HighsLogType::kWarning,
                 "Empty file name so not reading options\n");
    return HighsStatus::kWarning;
  }
  HighsLogOptions report_log_options = options_.log_options;
  switch (loadOptionsFromFile(report_log_options, options_, filename)) {
    case HighsLoadOptionsStatus::kError:
    case HighsLoadOptionsStatus::kEmpty:
      return HighsStatus::kError;
    default:
      break;
  }
  return optionChangeAction();
}

HighsStatus Highs::passOptions(const HighsOptions& options) {
  if (passLocalOptions(options_.log_options, options, options_) ==
      OptionStatus::kOk)
    return optionChangeAction();
  return HighsStatus::kError;
}

HighsStatus Highs::resetOptions() {
  resetLocalOptions(options_.records);
  return optionChangeAction();
}

HighsStatus Highs::writeOptions(const std::string& filename,
                                const bool report_only_deviations) const {
  HighsStatus return_status = HighsStatus::kOk;
  FILE* file;
  HighsFileType file_type;
  return_status = interpretCallStatus(
      options_.log_options,
      openWriteFile(filename, "writeOptions", file, file_type), return_status,
      "openWriteFile");
  if (return_status == HighsStatus::kError) return return_status;
  // Report to user that options are being written to a file
  if (filename != "")
    highsLogUser(options_.log_options, HighsLogType::kInfo,
                 "Writing the option values to %s\n", filename.c_str());
  return_status =
      interpretCallStatus(options_.log_options,
                          writeOptionsToFile(file, options_.records,
                                             report_only_deviations, file_type),
                          return_status, "writeOptionsToFile");
  if (file != stdout) fclose(file);
  return return_status;
}

// HighsStatus Highs::getOptionType(const char* option, HighsOptionType* type)
// const { return getOptionType(option, type);}

HighsStatus Highs::getOptionName(const HighsInt index,
                                 std::string* name) const {
  if (index < 0 || index >= HighsInt(this->options_.records.size()))
    return HighsStatus::kError;
  *name = this->options_.records[index]->name;
  return HighsStatus::kOk;
}

HighsStatus Highs::getOptionType(const std::string& option,
                                 HighsOptionType* type) const {
  if (getLocalOptionType(options_.log_options, option, options_.records,
                         type) == OptionStatus::kOk)
    return HighsStatus::kOk;
  return HighsStatus::kError;
}

HighsStatus Highs::getBoolOptionValues(const std::string& option,
                                       bool* current_value,
                                       bool* default_value) const {
  if (getLocalOptionValues(options_.log_options, option, options_.records,
                           current_value, default_value) != OptionStatus::kOk)
    return HighsStatus::kError;
  return HighsStatus::kOk;
}

HighsStatus Highs::getIntOptionValues(const std::string& option,
                                      HighsInt* current_value,
                                      HighsInt* min_value, HighsInt* max_value,
                                      HighsInt* default_value) const {
  if (getLocalOptionValues(options_.log_options, option, options_.records,
                           current_value, min_value, max_value,
                           default_value) != OptionStatus::kOk)
    return HighsStatus::kError;
  return HighsStatus::kOk;
}

HighsStatus Highs::getDoubleOptionValues(const std::string& option,
                                         double* current_value,
                                         double* min_value, double* max_value,
                                         double* default_value) const {
  if (getLocalOptionValues(options_.log_options, option, options_.records,
                           current_value, min_value, max_value,
                           default_value) != OptionStatus::kOk)
    return HighsStatus::kError;
  return HighsStatus::kOk;
}

HighsStatus Highs::getStringOptionValues(const std::string& option,
                                         std::string* current_value,
                                         std::string* default_value) const {
  if (getLocalOptionValues(options_.log_options, option, options_.records,
                           current_value, default_value) != OptionStatus::kOk)
    return HighsStatus::kError;
  return HighsStatus::kOk;
}

HighsStatus Highs::getInfoValue(const std::string& info,
                                HighsInt& value) const {
  InfoStatus status = getLocalInfoValue(options_.log_options, info, info_.valid,
                                        info_.records, value);
  if (status == InfoStatus::kOk) {
    return HighsStatus::kOk;
  } else if (status == InfoStatus::kUnavailable) {
    return HighsStatus::kWarning;
  } else {
    return HighsStatus::kError;
  }
}

#ifndef HIGHSINT64
HighsStatus Highs::getInfoValue(const std::string& info, int64_t& value) const {
  InfoStatus status = getLocalInfoValue(options_.log_options, info, info_.valid,
                                        info_.records, value);
  if (status == InfoStatus::kOk) {
    return HighsStatus::kOk;
  } else if (status == InfoStatus::kUnavailable) {
    return HighsStatus::kWarning;
  } else {
    return HighsStatus::kError;
  }
}
#endif

HighsStatus Highs::getInfoType(const std::string& info,
                               HighsInfoType& type) const {
  if (getLocalInfoType(options_.log_options, info, info_.records, type) ==
      InfoStatus::kOk)
    return HighsStatus::kOk;
  return HighsStatus::kError;
}

HighsStatus Highs::getInfoValue(const std::string& info, double& value) const {
  InfoStatus status = getLocalInfoValue(options_.log_options, info, info_.valid,
                                        info_.records, value);
  if (status == InfoStatus::kOk) {
    return HighsStatus::kOk;
  } else if (status == InfoStatus::kUnavailable) {
    return HighsStatus::kWarning;
  } else {
    return HighsStatus::kError;
  }
}

HighsStatus Highs::writeInfo(const std::string& filename) const {
  HighsStatus return_status = HighsStatus::kOk;
  FILE* file;
  HighsFileType file_type;
  return_status =
      interpretCallStatus(options_.log_options,
                          openWriteFile(filename, "writeInfo", file, file_type),
                          return_status, "openWriteFile");
  if (return_status == HighsStatus::kError) return return_status;
  // Report to user that options are being written to a file
  if (filename != "")
    highsLogUser(options_.log_options, HighsLogType::kInfo,
                 "Writing the info values to %s\n", filename.c_str());
  return_status = interpretCallStatus(
      options_.log_options,
      writeInfoToFile(file, info_.valid, info_.records, file_type),
      return_status, "writeInfoToFile");
  if (file != stdout) fclose(file);
  return return_status;
}

/**
 * @brief Get the size of HighsInt
 */
// HighsInt getSizeofHighsInt() {

// Methods below change the incumbent model or solver information
// associated with it. Hence returnFromHighs is called at the end of
// each
HighsStatus Highs::passModel(HighsModel model) {
  // This is the "master" Highs::passModel, in that all the others
  // (and readModel) eventually call it
  this->logHeader();
  // Possibly analyse the LP data
  if (kHighsAnalysisLevelModelData & options_.highs_analysis_level)
    analyseLp(options_.log_options, model.lp_);
  HighsStatus return_status = HighsStatus::kOk;
  // Clear the incumbent model and any associated data
  clearModel();
  HighsLp& lp = model_.lp_;
  HighsHessian& hessian = model_.hessian_;
  // Move the model's LP and Hessian to the internal LP and Hessian
  lp = std::move(model.lp_);
  hessian = std::move(model.hessian_);
  assert(lp.a_matrix_.formatOk());
  if (lp.num_col_ == 0 || lp.num_row_ == 0) {
    // Model constraint matrix has either no columns or no
    // rows. Clearly the matrix is empty, so may have no orientation
    // or starts assigned. HiGHS assumes that such a model will have
    // null starts, so make it column-wise
    highsLogUser(options_.log_options, HighsLogType::kInfo,
                 "Model has either no columns or no rows, so ignoring user "
                 "constraint matrix data and initialising empty matrix\n");
    lp.a_matrix_.format_ = MatrixFormat::kColwise;
    lp.a_matrix_.start_.assign(lp.num_col_ + 1, 0);
    lp.a_matrix_.index_.clear();
    lp.a_matrix_.value_.clear();
  } else {
    // Matrix has rows and columns, so a_matrix format must be valid
    if (!lp.a_matrix_.formatOk()) return HighsStatus::kError;
  }
  // Dimensions in a_matrix_ may not be set, so take them from lp.
  lp.setMatrixDimensions();
  // Residual scale factors may be present. ToDo Allow user-defined
  // scale factors to be passed
  assert(!lp.is_scaled_);
  assert(!lp.is_moved_);
  lp.resetScale();
  // Check that the LP array dimensions are valid
  if (!lpDimensionsOk("passModel", lp, options_.log_options))
    return HighsStatus::kError;
  // Check that the Hessian format is valid
  if (!hessian.formatOk()) return HighsStatus::kError;
  // Check validity of the LP, normalising its values
  return_status = interpretCallStatus(
      options_.log_options, assessLp(lp, options_), return_status, "assessLp");
  if (return_status == HighsStatus::kError) return return_status;
  // Now legality of matrix is established, ensure that it is
  // column-wise
  lp.ensureColwise();
  // Check validity of any Hessian, normalising its entries
  return_status = interpretCallStatus(options_.log_options,
                                      assessHessian(hessian, options_),
                                      return_status, "assessHessian");
  if (return_status == HighsStatus::kError) return return_status;
  if (hessian.dim_) {
    // Clear any zero Hessian
    if (hessian.numNz() == 0) {
      highsLogUser(options_.log_options, HighsLogType::kInfo,
                   "Hessian has dimension %" HIGHSINT_FORMAT
                   " but no nonzeros, so is ignored\n",
                   hessian.dim_);
      hessian.clear();
    }
  }
  // Ensure that any non-zero Hessian of dimension less than the
  // number of columns in the model is completed
  if (hessian.dim_) completeHessian(this->model_.lp_.num_col_, hessian);
  // Clear solver status, solution, basis and info associated with any
  // previous model; clear any HiGHS model object; create a HiGHS
  // model object for this LP
  return_status = interpretCallStatus(options_.log_options, clearSolver(),
                                      return_status, "clearSolver");
  // Apply any user scaling in call to optionChangeAction
  return_status =
      interpretCallStatus(options_.log_options, optionChangeAction(),
                          return_status, "optionChangeAction");
  return returnFromHighs(return_status);
}

HighsStatus Highs::passModel(HighsLp lp) {
  HighsModel model;
  model.lp_ = std::move(lp);
  return passModel(std::move(model));
}

HighsStatus Highs::passModel(
    const HighsInt num_col, const HighsInt num_row, const HighsInt a_num_nz,
    const HighsInt q_num_nz, const HighsInt a_format, const HighsInt q_format,
    const HighsInt sense, const double offset, const double* costs,
    const double* col_lower, const double* col_upper, const double* row_lower,
    const double* row_upper, const HighsInt* a_start, const HighsInt* a_index,
    const double* a_value, const HighsInt* q_start, const HighsInt* q_index,
    const double* q_value, const HighsInt* integrality) {
  this->logHeader();
  HighsModel model;
  HighsLp& lp = model.lp_;
  // Check that the formats of the constraint matrix and Hessian are valid
  if (!aFormatOk(a_num_nz, a_format)) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Model has illegal constraint matrix format\n");
    return HighsStatus::kError;
  }
  if (!qFormatOk(q_num_nz, q_format)) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Model has illegal Hessian matrix format\n");
    return HighsStatus::kError;
  }
  const bool a_rowwise =
      a_num_nz > 0 ? a_format == (HighsInt)MatrixFormat::kRowwise : false;
  //  if (num_nz) a_rowwise = a_format == (HighsInt)MatrixFormat::kRowwise;

  lp.num_col_ = num_col;
  lp.num_row_ = num_row;
  if (num_col > 0) {
    assert(costs != NULL);
    assert(col_lower != NULL);
    assert(col_upper != NULL);
    lp.col_cost_.assign(costs, costs + num_col);
    lp.col_lower_.assign(col_lower, col_lower + num_col);
    lp.col_upper_.assign(col_upper, col_upper + num_col);
  }
  if (num_row > 0) {
    assert(row_lower != NULL);
    assert(row_upper != NULL);
    lp.row_lower_.assign(row_lower, row_lower + num_row);
    lp.row_upper_.assign(row_upper, row_upper + num_row);
  }
  if (a_num_nz > 0) {
    assert(num_col > 0);
    assert(num_row > 0);
    assert(a_start != NULL);
    assert(a_index != NULL);
    assert(a_value != NULL);
    if (a_rowwise) {
      lp.a_matrix_.start_.assign(a_start, a_start + num_row);
    } else {
      lp.a_matrix_.start_.assign(a_start, a_start + num_col);
    }
    lp.a_matrix_.index_.assign(a_index, a_index + a_num_nz);
    lp.a_matrix_.value_.assign(a_value, a_value + a_num_nz);
  }
  if (a_rowwise) {
    lp.a_matrix_.start_.resize(num_row + 1);
    lp.a_matrix_.start_[num_row] = a_num_nz;
    lp.a_matrix_.format_ = MatrixFormat::kRowwise;
  } else {
    lp.a_matrix_.start_.resize(num_col + 1);
    lp.a_matrix_.start_[num_col] = a_num_nz;
    lp.a_matrix_.format_ = MatrixFormat::kColwise;
  }
  if (sense == (HighsInt)ObjSense::kMaximize) {
    lp.sense_ = ObjSense::kMaximize;
  } else {
    lp.sense_ = ObjSense::kMinimize;
  }
  lp.offset_ = offset;
  if (num_col > 0 && integrality != NULL) {
    lp.integrality_.resize(num_col);
    for (HighsInt iCol = 0; iCol < num_col; iCol++) {
      HighsInt integrality_status = integrality[iCol];
      const bool legal_integrality_status =
          integrality_status == (HighsInt)HighsVarType::kContinuous ||
          integrality_status == (HighsInt)HighsVarType::kInteger ||
          integrality_status == (HighsInt)HighsVarType::kSemiContinuous ||
          integrality_status == (HighsInt)HighsVarType::kSemiInteger;
      if (!legal_integrality_status) {
        highsLogDev(
            options_.log_options, HighsLogType::kError,
            "Model has illegal integer value of %d for integrality[%d]\n",
            (int)integrality_status, iCol);
        return HighsStatus::kError;
      }
      lp.integrality_[iCol] = (HighsVarType)integrality_status;
    }
  }
  if (q_num_nz > 0) {
    assert(num_col > 0);
    assert(q_start != NULL);
    assert(q_index != NULL);
    assert(q_value != NULL);
    HighsHessian& hessian = model.hessian_;
    hessian.dim_ = num_col;
    hessian.format_ = HessianFormat::kTriangular;
    hessian.start_.assign(q_start, q_start + num_col);
    hessian.start_.resize(num_col + 1);
    hessian.start_[num_col] = q_num_nz;
    hessian.index_.assign(q_index, q_index + q_num_nz);
    hessian.value_.assign(q_value, q_value + q_num_nz);
  }
  return passModel(std::move(model));
}

HighsStatus Highs::passModel(const HighsInt num_col, const HighsInt num_row,
                             const HighsInt num_nz, const HighsInt a_format,
                             const HighsInt sense, const double offset,
                             const double* costs, const double* col_lower,
                             const double* col_upper, const double* row_lower,
                             const double* row_upper, const HighsInt* a_start,
                             const HighsInt* a_index, const double* a_value,
                             const HighsInt* integrality) {
  return passModel(num_col, num_row, num_nz, 0, a_format, 0, sense, offset,
                   costs, col_lower, col_upper, row_lower, row_upper, a_start,
                   a_index, a_value, NULL, NULL, NULL, integrality);
}

HighsStatus Highs::passHessian(HighsHessian hessian_) {
  this->logHeader();
  HighsStatus return_status = HighsStatus::kOk;
  HighsHessian& hessian = model_.hessian_;
  hessian = std::move(hessian_);
  // Check validity of any Hessian, normalising its entries
  return_status = interpretCallStatus(options_.log_options,
                                      assessHessian(hessian, options_),
                                      return_status, "assessHessian");
  if (return_status == HighsStatus::kError) return return_status;
  if (hessian.dim_) {
    // Clear any zero Hessian
    if (hessian.numNz() == 0) {
      highsLogUser(options_.log_options, HighsLogType::kInfo,
                   "Hessian has dimension %" HIGHSINT_FORMAT
                   " but no nonzeros, so is ignored\n",
                   hessian.dim_);
      hessian.clear();
    }
  }
  // Ensure that any non-zero Hessian of dimension less than the
  // number of columns in the model is completed
  if (hessian.dim_) completeHessian(this->model_.lp_.num_col_, hessian);

  if (this->model_.lp_.user_cost_scale_) {
    // Assess and apply any user cost scaling
    if (!hessian.scaleOk(this->model_.lp_.user_cost_scale_,
                         this->options_.small_matrix_value,
                         this->options_.large_matrix_value)) {
      highsLogUser(
          options_.log_options, HighsLogType::kError,
          "User cost scaling yields zeroed or excessive Hessian values\n");
      return HighsStatus::kError;
    }
    double cost_scale_value = std::pow(2, this->model_.lp_.user_cost_scale_);
    for (HighsInt iEl = 0; iEl < hessian.numNz(); iEl++)
      hessian.value_[iEl] *= cost_scale_value;
  }
  return_status = interpretCallStatus(options_.log_options, clearSolver(),
                                      return_status, "clearSolver");
  return returnFromHighs(return_status);
}

HighsStatus Highs::passHessian(const HighsInt dim, const HighsInt num_nz,
                               const HighsInt format, const HighsInt* start,
                               const HighsInt* index, const double* value) {
  this->logHeader();
  HighsHessian hessian;
  if (!qFormatOk(num_nz, format)) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Model has illegal Hessian matrix format\n");
    return HighsStatus::kError;
  }
  HighsInt num_col = model_.lp_.num_col_;
  if (dim != num_col) return HighsStatus::kError;
  hessian.dim_ = num_col;
  hessian.format_ = HessianFormat::kTriangular;
  if (dim > 0) {
    assert(start != NULL);
    hessian.start_.assign(start, start + num_col);
    hessian.start_.resize(num_col + 1);
    hessian.start_[num_col] = num_nz;
  }
  if (num_nz > 0) {
    assert(index != NULL);
    assert(value != NULL);
    hessian.index_.assign(index, index + num_nz);
    hessian.value_.assign(value, value + num_nz);
  }
  return passHessian(hessian);
}

HighsStatus Highs::passColName(const HighsInt col, const std::string& name) {
  const HighsInt num_col = this->model_.lp_.num_col_;
  if (col < 0 || col >= num_col) {
    highsLogUser(
        options_.log_options, HighsLogType::kError,
        "Index %d for column name %s is outside the range [0, num_col = %d)\n",
        int(col), name.c_str(), int(num_col));
    return HighsStatus::kError;
  }
  if (int(name.length()) <= 0) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Cannot define empty column names\n");
    return HighsStatus::kError;
  }
  this->model_.lp_.col_names_.resize(num_col);
  this->model_.lp_.col_hash_.update(col, this->model_.lp_.col_names_[col],
                                    name);
  this->model_.lp_.col_names_[col] = name;
  return HighsStatus::kOk;
}

HighsStatus Highs::passRowName(const HighsInt row, const std::string& name) {
  const HighsInt num_row = this->model_.lp_.num_row_;
  if (row < 0 || row >= num_row) {
    highsLogUser(
        options_.log_options, HighsLogType::kError,
        "Index %d for row name %s is outside the range [0, num_row = %d)\n",
        int(row), name.c_str(), int(num_row));
    return HighsStatus::kError;
  }
  if (int(name.length()) <= 0) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Cannot define empty column names\n");
    return HighsStatus::kError;
  }
  this->model_.lp_.row_names_.resize(num_row);
  this->model_.lp_.row_hash_.update(row, this->model_.lp_.row_names_[row],
                                    name);
  this->model_.lp_.row_names_[row] = name;
  return HighsStatus::kOk;
}

HighsStatus Highs::passModelName(const std::string& name) {
  if (int(name.length()) <= 0) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Cannot define empty model names\n");
    return HighsStatus::kError;
  }
  this->model_.lp_.model_name_ = name;
  return HighsStatus::kOk;
}

HighsStatus Highs::readModel(const std::string& filename) {
  this->logHeader();
  HighsStatus return_status = HighsStatus::kOk;
  Filereader* reader =
      Filereader::getFilereader(options_.log_options, filename);
  if (reader == NULL) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Model file %s not supported\n", filename.c_str());
    return HighsStatus::kError;
  }

  HighsModel model;
  FilereaderRetcode call_code =
      reader->readModelFromFile(options_, filename, model);
  delete reader;
  if (call_code != FilereaderRetcode::kOk) {
    interpretFilereaderRetcode(options_.log_options, filename.c_str(),
                               call_code);
    return_status =
        interpretCallStatus(options_.log_options, HighsStatus::kError,
                            return_status, "readModelFromFile");
    if (return_status == HighsStatus::kError) return return_status;
  }
  model.lp_.model_name_ = extractModelName(filename);
  const bool remove_rows_of_count_1 = false;
  if (remove_rows_of_count_1) {
    // .lp files from PWSC (notably st-test23.lp) have bounds for
    // semi-continuous variables in the constraints section. By default,
    // these are interpreted as constraints, so the semi-continuous
    // variables are not set up correctly. Fix is to remove all rows of
    // count 1, interpreting their bounds as bounds on the corresponding
    // variable.
    removeRowsOfCountOne(options_.log_options, model.lp_);
  }
  return_status =
      interpretCallStatus(options_.log_options, passModel(std::move(model)),
                          return_status, "passModel");
  return returnFromHighs(return_status);
}

HighsStatus Highs::readBasis(const std::string& filename) {
  this->logHeader();
  HighsStatus return_status = HighsStatus::kOk;
  // Try to read basis file into read_basis
  HighsBasis read_basis = basis_;
  return_status = interpretCallStatus(
      options_.log_options,
      readBasisFile(options_.log_options, read_basis, filename), return_status,
      "readBasis");
  if (return_status != HighsStatus::kOk) return return_status;
  // Basis read OK: check whether it's consistent with the LP
  if (!isBasisConsistent(model_.lp_, read_basis)) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "readBasis: invalid basis\n");
    return HighsStatus::kError;
  }
  // Update the HiGHS basis and invalidate any simplex basis for the model
  basis_ = read_basis;
  basis_.valid = true;
  // Follow implications of a new HiGHS basis
  newHighsBasis();
  // Can't use returnFromHighs since...
  return HighsStatus::kOk;
}

HighsStatus Highs::writeModel(const std::string& filename) {
  return writeLocalModel(model_, filename);
}

HighsStatus Highs::writePresolvedModel(const std::string& filename) {
  return writeLocalModel(presolved_model_, filename);
}

HighsStatus Highs::writeLocalModel(HighsModel& model,
                                   const std::string& filename) {
  HighsStatus return_status = HighsStatus::kOk;

  // Ensure that the LP is column-wise
  model.lp_.ensureColwise();
  // Check for repeated column or row names that would corrupt the file
  if (model.lp_.col_hash_.hasDuplicate(model.lp_.col_names_)) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Model has repeated column names\n");
    return returnFromHighs(HighsStatus::kError);
  }
  if (model.lp_.row_hash_.hasDuplicate(model.lp_.row_names_)) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Model has repeated row names\n");
    return returnFromHighs(HighsStatus::kError);
  }
  if (filename == "") {
    // Empty file name: report model on logging stream
    reportModel(model);
    return_status = HighsStatus::kOk;
  } else {
    Filereader* writer =
        Filereader::getFilereader(options_.log_options, filename);
    if (writer == NULL) {
      highsLogUser(options_.log_options, HighsLogType::kError,
                   "Model file %s not supported\n", filename.c_str());
      return HighsStatus::kError;
    }
    // Report to user that model is being written
    highsLogUser(options_.log_options, HighsLogType::kInfo,
                 "Writing the model to %s\n", filename.c_str());
    return_status =
        interpretCallStatus(options_.log_options,
                            writer->writeModelToFile(options_, filename, model),
                            return_status, "writeModelToFile");
    delete writer;
  }
  return returnFromHighs(return_status);
}

HighsStatus Highs::writeBasis(const std::string& filename) {
  HighsStatus return_status = HighsStatus::kOk;
  HighsStatus call_status;
  FILE* file;
  HighsFileType file_type;
  call_status = openWriteFile(filename, "writebasis", file, file_type);
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "openWriteFile");
  if (return_status == HighsStatus::kError) return return_status;
  // Report to user that basis is being written
  if (filename != "")
    highsLogUser(options_.log_options, HighsLogType::kInfo,
                 "Writing the basis to %s\n", filename.c_str());
  writeBasisFile(file, basis_);
  if (file != stdout) fclose(file);
  return returnFromHighs(return_status);
}

HighsStatus Highs::presolve() {
  if (model_.needsMods(options_.infinite_cost)) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Model contains infinite costs or semi-variables, so cannot "
                 "be presolved independently\n");
    return HighsStatus::kError;
  }
  HighsStatus return_status = HighsStatus::kOk;

  clearPresolve();
  if (model_.isEmpty()) {
    model_presolve_status_ = HighsPresolveStatus::kNotReduced;
  } else {
    const bool force_presolve = true;
    // make sure global scheduler is initialized before calling presolve, since
    // MIP presolve may use parallelism
    highs::parallel::initialize_scheduler(options_.threads);
    max_threads = highs::parallel::num_threads();
    if (options_.threads != 0 && max_threads != options_.threads) {
      highsLogUser(
          options_.log_options, HighsLogType::kError,
          "Option 'threads' is set to %d but global scheduler has already been "
          "initialized to use %d threads. The previous scheduler instance can "
          "be destroyed by calling Highs::resetGlobalScheduler().\n",
          (int)options_.threads, max_threads);
      return HighsStatus::kError;
    }
    const bool force_lp_presolve = false;
    model_presolve_status_ = runPresolve(force_lp_presolve, force_presolve);
  }

  bool using_reduced_lp = false;
  switch (model_presolve_status_) {
    case HighsPresolveStatus::kNotPresolved: {
      // Shouldn't happen
      assert(model_presolve_status_ != HighsPresolveStatus::kNotPresolved);
      return_status = HighsStatus::kError;
      break;
    }
    case HighsPresolveStatus::kNotReduced:
    case HighsPresolveStatus::kInfeasible:
    case HighsPresolveStatus::kReduced:
    case HighsPresolveStatus::kReducedToEmpty:
    case HighsPresolveStatus::kUnboundedOrInfeasible: {
      // All OK
      if (model_presolve_status_ == HighsPresolveStatus::kInfeasible) {
        // Infeasible model, so indicate that the incumbent model is
        // known as such
        setHighsModelStatusAndClearSolutionAndBasis(
            HighsModelStatus::kInfeasible);
      } else if (model_presolve_status_ == HighsPresolveStatus::kNotReduced) {
        // No reduction, so fill Highs presolved model with the
        // incumbent model
        presolved_model_ = model_;
      } else if (model_presolve_status_ == HighsPresolveStatus::kReduced ||
                 model_presolve_status_ ==
                     HighsPresolveStatus::kReducedToEmpty) {
        // Nontrivial reduction, so fill Highs presolved model with the
        // presolved model
        using_reduced_lp = true;
      }
      return_status = HighsStatus::kOk;
      break;
    }
    case HighsPresolveStatus::kTimeout: {
      // Timeout, so assume that it's OK to fill the Highs presolved model with
      // the presolved model, but return warning.
      using_reduced_lp = true;
      return_status = HighsStatus::kWarning;
      break;
    }
    default: {
      // case HighsPresolveStatus::kOutOfMemory
      assert(model_presolve_status_ == HighsPresolveStatus::kOutOfMemory);
      highsLogUser(options_.log_options, HighsLogType::kError,
                   "Presolve fails due to memory allocation error\n");
      setHighsModelStatusAndClearSolutionAndBasis(
          HighsModelStatus::kPresolveError);
      return_status = HighsStatus::kError;
    }
  }
  if (using_reduced_lp) {
    presolved_model_.lp_ = presolve_.getReducedProblem();
    presolved_model_.lp_.setMatrixDimensions();
  }

  highsLogUser(options_.log_options, HighsLogType::kInfo,
               "Presolve status: %s\n",
               presolveStatusToString(model_presolve_status_).c_str());
  return returnFromHighs(return_status);
}

// Checks the options calls presolve and postsolve if needed. Solvers are called
// with callSolveLp(..)
HighsStatus Highs::run() {
  HighsInt min_highs_debug_level = kHighsDebugLevelMin;
  // kHighsDebugLevelCostly;
  // kHighsDebugLevelMax;
  //
  //  if (model_.lp_.num_row_>0 && model_.lp_.num_col_>0)
  //  writeLpMatrixPicToFile(options_, "LpMatrix", model_.lp_);
  if (options_.highs_debug_level < min_highs_debug_level)
    options_.highs_debug_level = min_highs_debug_level;

  const bool possibly_use_log_dev_level_2 = false;
  const HighsInt log_dev_level = options_.log_dev_level;
  const bool output_flag = options_.output_flag;
  HighsInt use_log_dev_level = log_dev_level;
  bool use_output_flag = output_flag;
  const HighsInt check_debug_run_call_num = -103757;
  const HighsInt check_num_col = -317;
  const HighsInt check_num_row = -714;
  if (possibly_use_log_dev_level_2) {
    if (this->debug_run_call_num_ == check_debug_run_call_num &&
        model_.lp_.num_col_ == check_num_col &&
        model_.lp_.num_row_ == check_num_row) {
      std::string message =
          "Entering Highs::run(): run/col/row matching check ";
      highsLogDev(options_.log_options, HighsLogType::kInfo,
                  "%s: run %d: LP(%6d, %6d)\n", message.c_str(),
                  (int)this->debug_run_call_num_, (int)model_.lp_.num_col_,
                  (int)model_.lp_.num_row_);
      // highsPause(true, message);
      use_log_dev_level = 2;
      use_output_flag = true;
    }
  }

  if (ekk_instance_.status_.has_nla)
    assert(ekk_instance_.lpFactorRowCompatible(model_.lp_.num_row_));

  highs::parallel::initialize_scheduler(options_.threads);

  max_threads = highs::parallel::num_threads();
  if (options_.threads != 0 && max_threads != options_.threads) {
    highsLogUser(
        options_.log_options, HighsLogType::kError,
        "Option 'threads' is set to %d but global scheduler has already been "
        "initialized to use %d threads. The previous scheduler instance can "
        "be destroyed by calling Highs::resetGlobalScheduler().\n",
        (int)options_.threads, max_threads);
    return HighsStatus::kError;
  }
  assert(max_threads > 0);
  if (max_threads <= 0)
    highsLogDev(options_.log_options, HighsLogType::kWarning,
                "WARNING: max_threads() returns %" HIGHSINT_FORMAT "\n",
                max_threads);
  highsLogDev(options_.log_options, HighsLogType::kDetailed,
              "Running with %" HIGHSINT_FORMAT " thread(s)\n", max_threads);

  // returnFromRun() is a common exit method to ensure consistency of
  // values set by run() and many other things. It's important to be
  // able to check that it's been called, and this is done with
  // this->called_return_from_run
  //
  // Make sure here that returnFromRun() has been called after any
  // previous call to run()

  assert(called_return_from_run);
  if (!called_return_from_run) {
    highsLogDev(options_.log_options, HighsLogType::kError,
                "Highs::run() called with called_return_from_run false\n");
    return HighsStatus::kError;
  }

  // Check whether model is consistent with any user bound/cost scaling
  assert(this->model_.lp_.user_bound_scale_ == this->options_.user_bound_scale);
  assert(this->model_.lp_.user_cost_scale_ == this->options_.user_cost_scale);
  // Assess whether to warn the user about excessive bounds and costs
  assessExcessiveBoundCost(options_.log_options, this->model_);

  // HiGHS solvers require models with no infinite costs, and no semi-variables
  //
  // Since completeSolutionFromDiscreteAssignment() may require a call
  // to run() - with initial check that called_return_from_run is true
  // - called_return_from_run cannot yet be set false.
  //
  // This possible call to run() means that any need to modify the problem to
  // remove infinite costs must be done first.
  //
  // Set undo_mods = false so that returnFromRun() doesn't undo any
  // mods that must be preserved - such as when solving a MIP node
  bool undo_mods = false;
  if (model_.lp_.has_infinite_cost_) {
    // If the model has infinite costs, then try to remove them. The
    // return_status indicates the success of this operation and, if
    // it's unsuccessful, the model will not have been modified and
    // run() can simply return an error with model status
    // HighsModelStatus::kUnknown
    assert(model_.lp_.hasInfiniteCost(options_.infinite_cost));
    HighsStatus return_status = handleInfCost();
    if (return_status != HighsStatus::kOk) {
      assert(return_status == HighsStatus::kError);
      setHighsModelStatusAndClearSolutionAndBasis(HighsModelStatus::kUnknown);
      return return_status;
    }
    // Modifications have been performed, so must be undone before
    // this call to run() returns
    assert(!model_.lp_.has_infinite_cost_);
    undo_mods = true;
  } else {
    assert(!model_.lp_.hasInfiniteCost(options_.infinite_cost));
  }

  // Ensure that all vectors in the model have exactly the right size
  exactResizeModel();

  if (model_.isMip() && solution_.value_valid) {
    // Determine whether the current (partial) solution of a MIP is
    // feasible and, if not, try to complete the assignment with
    // integer values (if necessary) and continuous values (if
    // necessary) to achieve a feasible solution. Valuable in the case
    // where users make a heuristic (partial) assignment of discrete variables
    HighsStatus call_status = completeSolutionFromDiscreteAssignment();
    if (call_status != HighsStatus::kOk) return HighsStatus::kError;
  }

  // Set this so that calls to returnFromRun() can be checked: from
  // here all return statements execute returnFromRun()
  called_return_from_run = false;
  HighsStatus return_status = HighsStatus::kOk;
  HighsStatus call_status;
  // Initialise the HiGHS model status
  model_status_ = HighsModelStatus::kNotset;
  // Clear the run info
  invalidateInfo();
  // Zero the iteration counts
  zeroIterationCounts();
  // Start the HiGHS run clock
  timer_.startRunHighsClock();
  // Return immediately if the model has no columns
  if (!model_.lp_.num_col_) {
    setHighsModelStatusAndClearSolutionAndBasis(HighsModelStatus::kModelEmpty);
    return returnFromRun(HighsStatus::kOk, undo_mods);
  }
  // Return immediately if the model is infeasible due to inconsistent
  // bounds, modifying any bounds with tiny infeasibilities
  if (!infeasibleBoundsOk()) {
    setHighsModelStatusAndClearSolutionAndBasis(HighsModelStatus::kInfeasible);
    return returnFromRun(return_status, undo_mods);
  }
  // Ensure that the LP (and any simplex LP) has the matrix column-wise
  model_.lp_.ensureColwise();
  // Ensure that the matrix has no large values
  if (model_.lp_.a_matrix_.hasLargeValue(options_.large_matrix_value)) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Cannot solve a model with a |value| exceeding %g in "
                 "constraint matrix\n",
                 options_.large_matrix_value);
    return returnFromRun(HighsStatus::kError, undo_mods);
  }
  if (options_.highs_debug_level > min_highs_debug_level) {
    // Shouldn't have to check validity of the LP since this is done when it is
    // loaded or modified
    call_status = assessLp(model_.lp_, options_);
    // If any errors have been found or normalisation carried out,
    // call_status will be kError or kWarning, so only valid return is OK.
    assert(call_status == HighsStatus::kOk);
    return_status = interpretCallStatus(options_.log_options, call_status,
                                        return_status, "assessLp");
    if (return_status == HighsStatus::kError)
      return returnFromRun(return_status, undo_mods);
    // Shouldn't have to check that the options settings are legal,
    // since they are checked when modified
    if (checkOptions(options_.log_options, options_.records) !=
        OptionStatus::kOk) {
      return_status = HighsStatus::kError;
      return returnFromRun(return_status, undo_mods);
    }
  }

  if (model_.lp_.model_name_.compare(""))
    highsLogDev(options_.log_options, HighsLogType::kVerbose,
                "Solving model: %s\n", model_.lp_.model_name_.c_str());

  if (!options_.solve_relaxation) {
    // Not solving the relaxation, so check validity of any
    // integrality, keeping a record of any bound and type
    // modifications for semi-variables
    bool made_semi_variable_mods = false;
    call_status =
        assessSemiVariables(model_.lp_, options_, made_semi_variable_mods);
    undo_mods = undo_mods || made_semi_variable_mods;
    if (call_status == HighsStatus::kError) {
      setHighsModelStatusAndClearSolutionAndBasis(
          HighsModelStatus::kSolveError);
      return returnFromRun(HighsStatus::kError, undo_mods);
    }
  }
  const bool use_simplex_or_ipm =
      (options_.solver.compare(kHighsChooseString) != 0);
  if (!use_simplex_or_ipm) {
    // Leaving HiGHS to choose method according to model class
    if (model_.isQp()) {
      if (model_.isMip()) {
        if (options_.solve_relaxation) {
          // Relax any semi-variables
          bool made_semi_variable_mods = false;
          relaxSemiVariables(model_.lp_, made_semi_variable_mods);
          undo_mods = undo_mods || made_semi_variable_mods;
        } else {
          highsLogUser(options_.log_options, HighsLogType::kError,
                       "Cannot solve MIQP problems with HiGHS\n");
          return returnFromRun(HighsStatus::kError, undo_mods);
        }
      }
      // Ensure that its diagonal entries are OK in the context of the
      // objective sense. It's OK to be semi-definite
      if (!okHessianDiagonal(options_, model_.hessian_, model_.lp_.sense_)) {
        highsLogUser(options_.log_options, HighsLogType::kError,
                     "Cannot solve non-convex QP problems with HiGHS\n");
        return returnFromRun(HighsStatus::kError, undo_mods);
      }
      call_status = callSolveQp();
      return_status = interpretCallStatus(options_.log_options, call_status,
                                          return_status, "callSolveQp");
      return returnFromRun(return_status, undo_mods);
    } else if (model_.isMip() && !options_.solve_relaxation) {
      // Model is a MIP and not solving just the relaxation
      call_status = callSolveMip();
      return_status = interpretCallStatus(options_.log_options, call_status,
                                          return_status, "callSolveMip");
      return returnFromRun(return_status, undo_mods);
    }
  }
  // If model is MIP, must be solving the relaxation or not leaving
  // HiGHS to choose method according to model class
  if (model_.isMip()) {
    assert(options_.solve_relaxation || use_simplex_or_ipm);
    // Relax any semi-variables
    bool made_semi_variable_mods = false;
    relaxSemiVariables(model_.lp_, made_semi_variable_mods);
    undo_mods = undo_mods || made_semi_variable_mods;
    highsLogUser(
        options_.log_options, HighsLogType::kInfo,
        "Solving LP relaxation since%s%s%s\n",
        options_.solve_relaxation ? " solve_relaxation is true" : "",
        options_.solve_relaxation && use_simplex_or_ipm ? " and" : "",
        use_simplex_or_ipm ? (" solver = " + options_.solver).c_str() : "");
  }
  // Solve the model as an LP
  HighsLp& incumbent_lp = model_.lp_;
  HighsLogOptions& log_options = options_.log_options;
  bool no_incumbent_lp_solution_or_basis = false;
  //
  // Record the initial time and set the component times and postsolve
  // iteration count to -1 to identify whether they are not required
  double initial_time = timer_.readRunHighsClock();
  double this_presolve_time = -1;
  double this_solve_presolved_lp_time = -1;
  double this_postsolve_time = -1;
  double this_solve_original_lp_time = -1;
  HighsInt postsolve_iteration_count = -1;
  const bool ipx_no_crossover = options_.solver == kIpmString &&
                                options_.run_crossover == kHighsOffString;

  if (options_.icrash) {
    ICrashStrategy strategy = ICrashStrategy::kICA;
    bool strategy_ok = parseICrashStrategy(options_.icrash_strategy, strategy);
    if (!strategy_ok) {
      // std::cout << "ICrash error: unknown strategy." << std::endl;
      highsLogUser(options_.log_options, HighsLogType::kError,
                   "ICrash error: unknown strategy.\n");
      return HighsStatus::kError;
    }
    ICrashOptions icrash_options{
        options_.icrash_dualize,         strategy,
        options_.icrash_starting_weight, options_.icrash_iterations,
        options_.icrash_approx_iter,     options_.icrash_exact,
        options_.icrash_breakpoints,     options_.log_options};

    HighsStatus icrash_status =
        callICrash(model_.lp_, icrash_options, icrash_info_);

    if (icrash_status != HighsStatus::kOk)
      return returnFromRun(icrash_status, undo_mods);

    // for now set the solution_.col_value
    solution_.col_value = icrash_info_.x_values;
    // Better not to use Highs::crossover
    const bool use_highs_crossover = false;
    if (use_highs_crossover) {
      crossover(solution_);
      // loops:
      called_return_from_run = true;

      options_.icrash = false;  // to avoid loop
    } else {
      HighsStatus crossover_status =
          callCrossover(options_, model_.lp_, basis_, solution_, model_status_,
                        info_, callback_);
      // callCrossover can return HighsStatus::kWarning due to
      // imprecise dual values. Ignore this since primal simplex will
      // be called to clean up duals
      highsLogUser(log_options, HighsLogType::kInfo,
                   "Crossover following iCrash has return status of %s, and "
                   "problem status is %s\n",
                   highsStatusToString(crossover_status).c_str(),
                   modelStatusToString(model_status_).c_str());
      if (crossover_status == HighsStatus::kError)
        return returnFromRun(crossover_status, undo_mods);
      assert(options_.simplex_strategy == kSimplexStrategyPrimal);
    }
    // timer_.stopRunHighsClock();
    // run();

    // todo: add "dual" values
    // return HighsStatus::kOk;
  }

  if (!basis_.valid && solution_.value_valid) {
    // There is no valid basis, but there is a valid solution, so use
    // it to construct a basis
    return_status =
        interpretCallStatus(options_.log_options, basisForSolution(),
                            return_status, "basisForSolution");
    if (return_status == HighsStatus::kError)
      return returnFromRun(return_status, undo_mods);
    assert(basis_.valid);
  }

  // lambda for Lp solving
  auto solveLp = [&](HighsLp& lp, const std::string& lpSolveDescription,
                     double& time) {
    time = -timer_.read(timer_.solve_clock);
    if (possibly_use_log_dev_level_2) {
      options_.log_dev_level = use_log_dev_level;
      options_.output_flag = use_output_flag;
    }
    timer_.start(timer_.solve_clock);
    call_status = callSolveLp(lp, lpSolveDescription);
    timer_.stop(timer_.solve_clock);
    if (possibly_use_log_dev_level_2) {
      options_.log_dev_level = log_dev_level;
      options_.output_flag = output_flag;
    }
    time += timer_.read(timer_.solve_clock);
  };

  const bool unconstrained_lp = incumbent_lp.a_matrix_.numNz() == 0;
  assert(incumbent_lp.num_row_ || unconstrained_lp);
  if (basis_.valid || options_.presolve == kHighsOffString ||
      unconstrained_lp) {
    // There is a valid basis for the problem, presolve is off, or LP
    // has no constraint matrix
    ekk_instance_.lp_name_ =
        "LP without presolve, or with basis, or unconstrained";
    // If there is a valid HiGHS basis, refine any status values that
    // are simply HighsBasisStatus::kNonbasic
    if (basis_.valid) refineBasis(incumbent_lp, solution_, basis_);
    solveLp(incumbent_lp,
            "Solving LP without presolve, or with basis, or unconstrained",
            this_solve_original_lp_time);
    return_status = interpretCallStatus(options_.log_options, call_status,
                                        return_status, "callSolveLp");
    if (return_status == HighsStatus::kError)
      return returnFromRun(return_status, undo_mods);
  } else {
    // Otherwise, consider presolve
    //
    // If using IPX to solve the reduced LP, but not crossover, set
    // lp_presolve_requires_basis_postsolve so that presolve can use
    // rules for which postsolve does not generate a basis.
    const bool lp_presolve_requires_basis_postsolve =
        options_.lp_presolve_requires_basis_postsolve;
    if (ipx_no_crossover) options_.lp_presolve_requires_basis_postsolve = false;
    // Possibly presolve - according to option_.presolve
    //
    // If solving the relaxation of a MIP, make sure that LP presolve
    // is used - so that rules assuming MIP properties are not applied.
    const double from_presolve_time = timer_.read(timer_.presolve_clock);
    this_presolve_time = -from_presolve_time;
    timer_.start(timer_.presolve_clock);
    const bool force_lp_presolve = true;
    model_presolve_status_ = runPresolve(force_lp_presolve);
    timer_.stop(timer_.presolve_clock);
    const double to_presolve_time = timer_.read(timer_.presolve_clock);
    this_presolve_time += to_presolve_time;
    presolve_.info_.presolve_time = this_presolve_time;
    // Recover any modified options
    options_.lp_presolve_requires_basis_postsolve =
        lp_presolve_requires_basis_postsolve;

    // Set an illegal local pivot threshold value that's updated after
    // solving the presolved LP - if simplex is used
    double factor_pivot_threshold = -1;

    // Run solver.
    bool have_optimal_solution = false;
    // ToDo Put solution of presolved problem in a separate method

    switch (model_presolve_status_) {
      case HighsPresolveStatus::kNotPresolved: {
        ekk_instance_.lp_name_ = "Original LP";
        solveLp(incumbent_lp, "Not presolved: solving the LP",
                this_solve_original_lp_time);
        return_status = interpretCallStatus(options_.log_options, call_status,
                                            return_status, "callSolveLp");
        if (return_status == HighsStatus::kError)
          return returnFromRun(return_status, undo_mods);
        break;
      }
      case HighsPresolveStatus::kNotReduced: {
        ekk_instance_.lp_name_ = "Unreduced LP";
        // Log the presolve reductions
        reportPresolveReductions(log_options, incumbent_lp, false);
        solveLp(incumbent_lp, "Problem not reduced by presolve: solving the LP",
                this_solve_original_lp_time);
        return_status = interpretCallStatus(options_.log_options, call_status,
                                            return_status, "callSolveLp");
        if (return_status == HighsStatus::kError)
          return returnFromRun(return_status, undo_mods);
        break;
      }
      case HighsPresolveStatus::kReduced: {
        HighsLp& reduced_lp = presolve_.getReducedProblem();
        reduced_lp.setMatrixDimensions();
        if (kAllowDeveloperAssert) {
          // Validate the reduced LP
          //
          // Although presolve can yield small values in the matrix,
          // they are only stripped out (by assessLp) in debug. This
          // suggests that they are no real danger to the simplex
          // solver. The only danger is pivoting on them, but that
          // implies that values of roughly that size have been chosen
          // in the ratio test. Even with the filter, values of 1e-9
          // could be in the matrix, and these would be bad
          // pivots. Hence, since the small values may play a
          // meaningful role in postsolve, then it's better to keep
          // them.
          //
          // ToDo. Analyse the extent of small value creation. See #1187
          assert(assessLp(reduced_lp, options_) == HighsStatus::kOk);
        } else {
          reduced_lp.a_matrix_.assessSmallValues(options_.log_options,
                                                 options_.small_matrix_value);
        }
        call_status = cleanBounds(options_, reduced_lp);
        // Ignore any warning from clean bounds since the original LP
        // is still solved after presolve
        if (interpretCallStatus(options_.log_options, call_status,
                                return_status,
                                "cleanBounds") == HighsStatus::kError)
          return HighsStatus::kError;
        // Log the presolve reductions
        reportPresolveReductions(log_options, incumbent_lp, reduced_lp);
        // Solving the presolved LP with strictly reduced dimensions
        // so ensure that the Ekk instance is cleared
        ekk_instance_.clear();
        ekk_instance_.lp_name_ = "Presolved LP";
        // Don't try dual cut-off when solving the presolved LP, as the
        // objective values aren't correct
        const double save_objective_bound = options_.objective_bound;
        options_.objective_bound = kHighsInf;
        solveLp(reduced_lp, "Solving the presolved LP",
                this_solve_presolved_lp_time);
        if (ekk_instance_.status_.initialised_for_solve) {
          // Record the pivot threshold resulting from solving the presolved LP
          // with simplex
          factor_pivot_threshold = ekk_instance_.info_.factor_pivot_threshold;
        }
        // Restore the dual objective cut-off
        options_.objective_bound = save_objective_bound;
        return_status = interpretCallStatus(options_.log_options, call_status,
                                            return_status, "callSolveLp");
        if (return_status == HighsStatus::kError)
          return returnFromRun(return_status, undo_mods);
        have_optimal_solution = model_status_ == HighsModelStatus::kOptimal;
        no_incumbent_lp_solution_or_basis =
            model_status_ == HighsModelStatus::kInfeasible ||
            model_status_ == HighsModelStatus::kUnbounded ||
            model_status_ == HighsModelStatus::kUnboundedOrInfeasible ||
            model_status_ == HighsModelStatus::kTimeLimit ||
            model_status_ == HighsModelStatus::kIterationLimit ||
            model_status_ == HighsModelStatus::kInterrupt;
        break;
      }
      case HighsPresolveStatus::kReducedToEmpty: {
        reportPresolveReductions(log_options, incumbent_lp, true);
        // Create a trivial optimal solution for postsolve to use
        solution_.clear();
        basis_.clear();
        basis_.debug_origin_name = "Presolve to empty";
        basis_.valid = true;
        basis_.alien = false;
        basis_.was_alien = false;
        solution_.value_valid = true;
        solution_.dual_valid = true;
        have_optimal_solution = true;
        break;
      }
      case HighsPresolveStatus::kInfeasible: {
        setHighsModelStatusAndClearSolutionAndBasis(
            HighsModelStatus::kInfeasible);
        highsLogUser(log_options, HighsLogType::kInfo,
                     "Problem status detected on presolve: %s\n",
                     modelStatusToString(model_status_).c_str());
        return returnFromRun(return_status, undo_mods);
      }
      case HighsPresolveStatus::kUnboundedOrInfeasible: {
        highsLogUser(
            log_options, HighsLogType::kInfo,
            "Problem status detected on presolve: %s\n",
            modelStatusToString(HighsModelStatus::kUnboundedOrInfeasible)
                .c_str());
        if (options_.allow_unbounded_or_infeasible) {
          setHighsModelStatusAndClearSolutionAndBasis(
              HighsModelStatus::kUnboundedOrInfeasible);
          return returnFromRun(return_status, undo_mods);
        }
        // Presolve has returned kUnboundedOrInfeasible, but HiGHS
        // can't return this. Use primal simplex solver on the original
        // LP
        HighsOptions save_options = options_;
        options_.solver = "simplex";
        options_.simplex_strategy = kSimplexStrategyPrimal;
        solveLp(incumbent_lp,
                "Solving the original LP with primal simplex "
                "to determine infeasible or unbounded",
                this_solve_original_lp_time);
        // Recover the options
        options_ = save_options;
        if (return_status == HighsStatus::kError)
          return returnFromRun(return_status, undo_mods);
        // ToDo Eliminate setBasisValidity once ctest passes. Asserts
        // verify that it does nothing - other than setting
        // info_.valid = true;
        setBasisValidity();
        assert(model_status_ == HighsModelStatus::kInfeasible ||
               model_status_ == HighsModelStatus::kUnbounded);
        return returnFromRun(return_status, undo_mods);
      }
      case HighsPresolveStatus::kTimeout: {
        setHighsModelStatusAndClearSolutionAndBasis(
            HighsModelStatus::kTimeLimit);
        highsLogDev(log_options, HighsLogType::kWarning,
                    "Presolve reached timeout\n");
        return returnFromRun(HighsStatus::kWarning, undo_mods);
      }
      case HighsPresolveStatus::kOutOfMemory: {
        setHighsModelStatusAndClearSolutionAndBasis(
            HighsModelStatus::kMemoryLimit);
        highsLogUser(options_.log_options, HighsLogType::kError,
                     "Presolve fails due to memory allocation error\n");
        return returnFromRun(HighsStatus::kError, undo_mods);
      }
      default: {
        assert(model_presolve_status_ == HighsPresolveStatus::kNullError);
        setHighsModelStatusAndClearSolutionAndBasis(
            HighsModelStatus::kPresolveError);
        highsLogDev(log_options, HighsLogType::kError,
                    "Presolve returned status %d\n",
                    (int)model_presolve_status_);
        return returnFromRun(HighsStatus::kError, undo_mods);
      }
    }
    // End of presolve
    //
    // Cases of infeasibility/unboundedness timeout and memory errors
    // all handled, so just the successes remain
    assert(model_presolve_status_ == HighsPresolveStatus::kNotPresolved ||
           model_presolve_status_ == HighsPresolveStatus::kNotReduced ||
           model_presolve_status_ == HighsPresolveStatus::kReduced ||
           model_presolve_status_ == HighsPresolveStatus::kReducedToEmpty);

    // Postsolve. Does nothing if there were no reductions during presolve.

    if (have_optimal_solution) {
      // ToDo Put this in a separate method
      assert(model_status_ == HighsModelStatus::kOptimal ||
             model_presolve_status_ == HighsPresolveStatus::kReducedToEmpty);
      if (model_presolve_status_ == HighsPresolveStatus::kReduced ||
          model_presolve_status_ == HighsPresolveStatus::kReducedToEmpty) {
        // If presolve is nontrivial, extract the optimal solution
        // and basis for the presolved problem in order to generate
        // the solution and basis for postsolve to use to generate a
        // solution(?) and basis that is, hopefully, optimal. This is
        // confirmed or corrected by hot-starting the simplex solver
        presolve_.data_.recovered_solution_ = solution_;
        presolve_.data_.recovered_basis_ = basis_;

        this_postsolve_time = -timer_.read(timer_.postsolve_clock);
        timer_.start(timer_.postsolve_clock);
        HighsPostsolveStatus postsolve_status = runPostsolve();
        timer_.stop(timer_.postsolve_clock);
        this_postsolve_time += -timer_.read(timer_.postsolve_clock);
        presolve_.info_.postsolve_time = this_postsolve_time;

        if (postsolve_status == HighsPostsolveStatus::kSolutionRecovered) {
          highsLogDev(log_options, HighsLogType::kVerbose,
                      "Postsolve finished\n");
          // Set solution and its status
          solution_.clear();
          solution_ = presolve_.data_.recovered_solution_;
          solution_.value_valid = true;
          //          if (ipx_no_crossover) {
          if (!basis_.valid) {
            // Have a primal-dual solution, but no basis, since IPX
            // was used without crossover, either because
            // run_crossover was "off" or "choose" and IPX determined
            // optimality
            solution_.dual_valid = true;
            basis_.invalidate();
          } else {
            //
            // Hot-start the simplex solver for the incumbent LP
            //
            solution_.dual_valid = true;
            // Set basis and its status
            basis_.valid = true;
            basis_.col_status = presolve_.data_.recovered_basis_.col_status;
            basis_.row_status = presolve_.data_.recovered_basis_.row_status;
            basis_.debug_origin_name += ": after postsolve";
            // Basic primal activities are wrong after postsolve, so
            // possibly skip KKT check
            const bool perform_kkt_check = true;
            if (perform_kkt_check) {
              // Possibly force debug to perform KKT check on what's
              // returned from postsolve
              const bool force_debug = false;
              HighsInt save_highs_debug_level = options_.highs_debug_level;
              if (force_debug)
                options_.highs_debug_level = kHighsDebugLevelCostly;
              if (debugHighsSolution("After returning from postsolve", options_,
                                     model_, solution_,
                                     basis_) == HighsDebugStatus::kLogicalError)
                return returnFromRun(HighsStatus::kError, undo_mods);
              options_.highs_debug_level = save_highs_debug_level;
            }
            // Save the options to allow the best simplex strategy to
            // be used
            HighsOptions save_options = options_;
            const bool full_logging = false;
            if (full_logging) options_.log_dev_level = kHighsLogDevLevelVerbose;
            // Force the use of simplex to clean up if IPM has been used
            // to solve the presolved problem
            if (options_.solver == kIpmString) options_.solver = kSimplexString;
            options_.simplex_strategy = kSimplexStrategyChoose;
            // Ensure that the parallel solver isn't used
            options_.simplex_min_concurrency = 1;
            options_.simplex_max_concurrency = 1;
            // Use any pivot threshold resulting from solving the presolved LP
            if (factor_pivot_threshold > 0)
              options_.factor_pivot_threshold = factor_pivot_threshold;
            // The basis returned from postsolve is just basic/nonbasic
            // and EKK expects a refined basis, so set it up now
            refineBasis(incumbent_lp, solution_, basis_);
            // Scrap the EKK data from solving the presolved LP
            ekk_instance_.invalidate();
            ekk_instance_.lp_name_ = "Postsolve LP";
            // Set up the iteration count and timing records so that
            // adding the corresponding values after callSolveLp gives
            // difference
            postsolve_iteration_count = -info_.simplex_iteration_count;
            solveLp(incumbent_lp,
                    "Solving the original LP from the solution after postsolve",
                    this_solve_original_lp_time);
            // Determine the iteration count
            postsolve_iteration_count += info_.simplex_iteration_count;
            return_status =
                interpretCallStatus(options_.log_options, call_status,
                                    return_status, "callSolveLp");
            // Recover the options
            options_ = save_options;
            if (return_status == HighsStatus::kError)
              return returnFromRun(return_status, undo_mods);
            if (postsolve_iteration_count > 0)
              highsLogUser(options_.log_options, HighsLogType::kInfo,
                           "Required %d simplex iterations after postsolve\n",
                           int(postsolve_iteration_count));
          }
        } else {
          highsLogUser(log_options, HighsLogType::kError,
                       "Postsolve return status is %d\n",
                       (int)postsolve_status);
          setHighsModelStatusAndClearSolutionAndBasis(
              HighsModelStatus::kPostsolveError);
          return returnFromRun(HighsStatus::kError, undo_mods);
        }
      } else {
        // LP was not reduced by presolve, so have simply solved the original LP
        assert(model_presolve_status_ == HighsPresolveStatus::kNotReduced);
      }
    }
  }
  // Cycling can yield model_status_ == HighsModelStatus::kNotset,
  //  assert(model_status_ != HighsModelStatus::kNotset);
  if (no_incumbent_lp_solution_or_basis) {
    // In solving the (strictly reduced) presolved LP, it is found to
    // be infeasible or unbounded, the time/iteration limit has been
    // reached, a user interrupt has occurred, or the status is unknown
    // (cycling)
    //
    // Hence there's no incumbent lp solution or basis to drive dual
    // postsolve
    assert(model_status_ == HighsModelStatus::kInfeasible ||
           model_status_ == HighsModelStatus::kUnbounded ||
           model_status_ == HighsModelStatus::kUnboundedOrInfeasible ||
           model_status_ == HighsModelStatus::kTimeLimit ||
           model_status_ == HighsModelStatus::kIterationLimit ||
           model_status_ == HighsModelStatus::kInterrupt ||
           model_status_ == HighsModelStatus::kUnknown);
    // The HEkk data correspond to the (strictly reduced) presolved LP
    // so must be cleared
    ekk_instance_.clear();
    setHighsModelStatusAndClearSolutionAndBasis(model_status_);
  } else {
    // ToDo Eliminate setBasisValidity once ctest passes. Asserts
    // verify that it does nothing - other than setting info_.valid =
    // true;
    setBasisValidity();
  }
  double lp_solve_final_time = timer_.readRunHighsClock();
  double this_solve_time = lp_solve_final_time - initial_time;
  if (postsolve_iteration_count < 0) {
    highsLogDev(log_options, HighsLogType::kInfo, "Postsolve  : \n");
  } else {
    highsLogDev(log_options, HighsLogType::kInfo,
                "Postsolve  : %" HIGHSINT_FORMAT "\n",
                postsolve_iteration_count);
  }
  if (this_solve_time > 0)
    highsLogDev(log_options, HighsLogType::kInfo, "Time       : %8.2f\n",
                this_solve_time);
  if (this_presolve_time > 0)
    highsLogDev(log_options, HighsLogType::kInfo, "Time Pre   : %8.2f\n",
                this_presolve_time);
  if (this_solve_presolved_lp_time > 0)
    highsLogDev(log_options, HighsLogType::kInfo, "Time PreLP : %8.2f\n",
                this_solve_presolved_lp_time);
  if (this_solve_original_lp_time > 0)
    highsLogDev(log_options, HighsLogType::kInfo, "Time PostLP: %8.2f\n",
                this_solve_original_lp_time);
  if (this_solve_time > 0) {
    highsLogDev(log_options, HighsLogType::kInfo, "For LP %16s",
                incumbent_lp.model_name_.c_str());
    double sum_time = 0;
    if (this_presolve_time > 0) {
      sum_time += this_presolve_time;
      HighsInt pct = (100 * this_presolve_time) / this_solve_time;
      highsLogDev(log_options, HighsLogType::kInfo,
                  ": Presolve %8.2f (%3" HIGHSINT_FORMAT "%%)",
                  this_presolve_time, pct);
    }
    if (this_solve_presolved_lp_time > 0) {
      sum_time += this_solve_presolved_lp_time;
      HighsInt pct = (100 * this_solve_presolved_lp_time) / this_solve_time;
      highsLogDev(log_options, HighsLogType::kInfo,
                  ": Solve presolved LP %8.2f (%3" HIGHSINT_FORMAT "%%)",
                  this_solve_presolved_lp_time, pct);
    }
    if (this_postsolve_time > 0) {
      sum_time += this_postsolve_time;
      HighsInt pct = (100 * this_postsolve_time) / this_solve_time;
      highsLogDev(log_options, HighsLogType::kInfo,
                  ": Postsolve %8.2f (%3" HIGHSINT_FORMAT "%%)",
                  this_postsolve_time, pct);
    }
    if (this_solve_original_lp_time > 0) {
      sum_time += this_solve_original_lp_time;
      HighsInt pct = (100 * this_solve_original_lp_time) / this_solve_time;
      highsLogDev(log_options, HighsLogType::kInfo,
                  ": Solve original LP %8.2f (%3" HIGHSINT_FORMAT "%%)",
                  this_solve_original_lp_time, pct);
    }
    highsLogDev(log_options, HighsLogType::kInfo, "\n");
    double rlv_time_difference =
        fabs(sum_time - this_solve_time) / this_solve_time;
    if (rlv_time_difference > 0.1)
      highsLogDev(options_.log_options, HighsLogType::kInfo,
                  "Strange: Solve time = %g; Sum times = %g: relative "
                  "difference = %g\n",
                  this_solve_time, sum_time, rlv_time_difference);
  }
  // Assess success according to the scaled model status, unless
  // something worse has happened earlier
  call_status = highsStatusFromHighsModelStatus(model_status_);
  return_status =
      interpretCallStatus(options_.log_options, call_status, return_status,
                          "highsStatusFromHighsModelStatus");
  return returnFromRun(return_status, undo_mods);
}

HighsStatus Highs::getStandardFormLp(HighsInt& num_col, HighsInt& num_row,
                                     HighsInt& num_nz, double& offset,
                                     double* cost, double* rhs, HighsInt* start,
                                     HighsInt* index, double* value) {
  if (!this->standard_form_valid_) {
    HighsStatus status = formStandardFormLp();
    assert(status == HighsStatus::kOk);
  }
  num_col = this->standard_form_cost_.size();
  num_row = this->standard_form_rhs_.size();
  num_nz = this->standard_form_matrix_.start_[num_col];
  offset = this->standard_form_offset_;
  for (HighsInt iCol = 0; iCol < num_col; iCol++) {
    if (cost) cost[iCol] = this->standard_form_cost_[iCol];
    if (start) start[iCol] = this->standard_form_matrix_.start_[iCol];
    if (index || value) {
      for (HighsInt iEl = this->standard_form_matrix_.start_[iCol];
           iEl < this->standard_form_matrix_.start_[iCol + 1]; iEl++) {
        if (index) index[iEl] = this->standard_form_matrix_.index_[iEl];
        if (value) value[iEl] = this->standard_form_matrix_.value_[iEl];
      }
    }
  }
  if (start) start[num_col] = this->standard_form_matrix_.start_[num_col];
  if (rhs) {
    for (HighsInt iRow = 0; iRow < num_row; iRow++)
      rhs[iRow] = this->standard_form_rhs_[iRow];
  }
  return HighsStatus::kOk;
}

HighsStatus Highs::getDualRay(bool& has_dual_ray, double* dual_ray_value) {
  has_dual_ray = false;
  return getDualRayInterface(has_dual_ray, dual_ray_value);
}

HighsStatus Highs::getDualRaySparse(bool& has_dual_ray,
                                    HVector& row_ep_buffer) {
  has_dual_ray = ekk_instance_.status_.has_dual_ray;
  if (has_dual_ray) {
    ekk_instance_.setNlaPointersForLpAndScale(model_.lp_);
    row_ep_buffer.clear();
    row_ep_buffer.count = 1;
    row_ep_buffer.packFlag = true;
    HighsInt iRow = ekk_instance_.info_.dual_ray_row_;
    row_ep_buffer.index[0] = iRow;
    row_ep_buffer.array[iRow] = ekk_instance_.info_.dual_ray_sign_;

    ekk_instance_.btran(row_ep_buffer, ekk_instance_.info_.row_ep_density);
  }

  return HighsStatus::kOk;
}

HighsStatus Highs::getDualUnboundednessDirection(
    bool& has_dual_unboundedness_direction,
    double* dual_unboundedness_direction_value) {
  if (dual_unboundedness_direction_value) {
    std::vector<double> dual_ray_value(this->model_.lp_.num_row_);
    HighsStatus status =
        getDualRay(has_dual_unboundedness_direction, dual_ray_value.data());
    if (status != HighsStatus::kOk || !has_dual_unboundedness_direction)
      return HighsStatus::kError;
    std::vector<double> dual_unboundedness_direction;
    this->model_.lp_.a_matrix_.productTransposeQuad(
        dual_unboundedness_direction, dual_ray_value);
    for (HighsInt iCol = 0; iCol < this->model_.lp_.num_col_; iCol++)
      dual_unboundedness_direction_value[iCol] =
          dual_unboundedness_direction[iCol];
  } else {
    return getDualRay(has_dual_unboundedness_direction, nullptr);
  }
  return HighsStatus::kOk;
}

HighsStatus Highs::getPrimalRay(bool& has_primal_ray,
                                double* primal_ray_value) {
  has_primal_ray = false;
  return getPrimalRayInterface(has_primal_ray, primal_ray_value);
}

HighsStatus Highs::getRanging(HighsRanging& ranging) {
  HighsStatus return_status = getRangingInterface();
  ranging = this->ranging_;
  return return_status;
}

HighsStatus Highs::feasibilityRelaxation(const double global_lower_penalty,
                                         const double global_upper_penalty,
                                         const double global_rhs_penalty,
                                         const double* local_lower_penalty,
                                         const double* local_upper_penalty,
                                         const double* local_rhs_penalty) {
  std::vector<HighsInt> infeasible_row_subset;
  return elasticityFilter(global_lower_penalty, global_upper_penalty,
                          global_rhs_penalty, local_lower_penalty,
                          local_upper_penalty, local_rhs_penalty, false,
                          infeasible_row_subset);
}

HighsStatus Highs::getIllConditioning(HighsIllConditioning& ill_conditioning,
                                      const bool constraint,
                                      const HighsInt method,
                                      const double ill_conditioning_bound) {
  if (!basis_.valid) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Cannot get ill-conditioning without a valid basis\n");
    return HighsStatus::kError;
  }
  return computeIllConditioning(ill_conditioning, constraint, method,
                                ill_conditioning_bound);
}

HighsStatus Highs::getIis(HighsIis& iis) {
  if (this->model_status_ == HighsModelStatus::kOptimal ||
      this->model_status_ == HighsModelStatus::kUnbounded) {
    // Strange to call getIis for a model that's known to be feasible
    highsLogUser(
        options_.log_options, HighsLogType::kInfo,
        "Calling Highs::getIis for a model that is known to be feasible\n");
    iis.invalidate();
    // No IIS exists, so validate the empty HighsIis instance
    iis.valid_ = true;
    return HighsStatus::kOk;
  }
  HighsStatus return_status = HighsStatus::kOk;
  if (this->model_status_ != HighsModelStatus::kNotset &&
      this->model_status_ != HighsModelStatus::kInfeasible) {
    return_status = HighsStatus::kWarning;
    highsLogUser(options_.log_options, HighsLogType::kWarning,
                 "Calling Highs::getIis for a model with status %s\n",
                 this->modelStatusToString(this->model_status_).c_str());
  }
  return_status =
      interpretCallStatus(options_.log_options, this->getIisInterface(),
                          return_status, "getIisInterface");
  iis = this->iis_;
  return return_status;
}

HighsStatus Highs::getDualObjectiveValue(
    double& dual_objective_function_value) {
  bool have_dual_objective_value = false;
  if (!this->model_.isQp())
    have_dual_objective_value = computeDualObjectiveValue(
        model_.lp_, solution_, dual_objective_function_value);
  return have_dual_objective_value ? HighsStatus::kOk : HighsStatus::kError;
}

bool Highs::hasInvert() const { return ekk_instance_.status_.has_invert; }

const HighsInt* Highs::getBasicVariablesArray() const {
  assert(ekk_instance_.status_.has_invert);
  return ekk_instance_.basis_.basicIndex_.data();
}

HighsStatus Highs::getBasicVariables(HighsInt* basic_variables) {
  if (basic_variables == NULL) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "getBasicVariables: basic_variables is NULL\n");
    return HighsStatus::kError;
  }
  return getBasicVariablesInterface(basic_variables);
}

HighsStatus Highs::getBasisInverseRowSparse(const HighsInt row,
                                            HVector& row_ep_buffer) {
  ekk_instance_.setNlaPointersForLpAndScale(model_.lp_);
  row_ep_buffer.clear();
  row_ep_buffer.count = 1;
  row_ep_buffer.index[0] = row;
  row_ep_buffer.array[row] = 1;
  row_ep_buffer.packFlag = true;

  ekk_instance_.btran(row_ep_buffer, ekk_instance_.info_.row_ep_density);

  return HighsStatus::kOk;
}

HighsStatus Highs::getBasisInverseRow(const HighsInt row, double* row_vector,
                                      HighsInt* row_num_nz,
                                      HighsInt* row_indices) {
  if (row_vector == NULL) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "getBasisInverseRow: row_vector is NULL\n");
    return HighsStatus::kError;
  }
  // row_indices can be NULL - it's the trigger that determines
  // whether they are identified or not
  HighsInt num_row = model_.lp_.num_row_;
  if (row < 0 || row >= num_row) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Row index %" HIGHSINT_FORMAT
                 " out of range [0, %" HIGHSINT_FORMAT
                 "] in getBasisInverseRow\n",
                 row, num_row - 1);
    return HighsStatus::kError;
  }
  if (!ekk_instance_.status_.has_invert)
    return invertRequirementError("getBasisInverseRow");
  // Compute a row i of the inverse of the basis matrix by solving B^Tx=e_i
  vector<double> rhs;
  rhs.assign(num_row, 0);
  rhs[row] = 1;
  basisSolveInterface(rhs, row_vector, row_num_nz, row_indices, true);
  return HighsStatus::kOk;
}

HighsStatus Highs::getBasisInverseCol(const HighsInt col, double* col_vector,
                                      HighsInt* col_num_nz,
                                      HighsInt* col_indices) {
  if (col_vector == NULL) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "getBasisInverseCol: col_vector is NULL\n");
    return HighsStatus::kError;
  }
  // col_indices can be NULL - it's the trigger that determines
  // whether they are identified or not
  HighsInt num_row = model_.lp_.num_row_;
  if (col < 0 || col >= num_row) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Column index %" HIGHSINT_FORMAT
                 " out of range [0, %" HIGHSINT_FORMAT
                 "] in getBasisInverseCol\n",
                 col, num_row - 1);
    return HighsStatus::kError;
  }
  if (!ekk_instance_.status_.has_invert)
    return invertRequirementError("getBasisInverseCol");
  // Compute a col i of the inverse of the basis matrix by solving Bx=e_i
  vector<double> rhs;
  rhs.assign(num_row, 0);
  rhs[col] = 1;
  basisSolveInterface(rhs, col_vector, col_num_nz, col_indices, false);
  return HighsStatus::kOk;
}

HighsStatus Highs::getBasisSolve(const double* Xrhs, double* solution_vector,
                                 HighsInt* solution_num_nz,
                                 HighsInt* solution_indices) {
  if (Xrhs == NULL) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "getBasisSolve: Xrhs is NULL\n");
    return HighsStatus::kError;
  }
  if (solution_vector == NULL) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "getBasisSolve: solution_vector is NULL\n");
    return HighsStatus::kError;
  }
  // solution_indices can be NULL - it's the trigger that determines
  // whether they are identified or not
  if (!ekk_instance_.status_.has_invert)
    return invertRequirementError("getBasisSolve");
  HighsInt num_row = model_.lp_.num_row_;
  vector<double> rhs;
  rhs.assign(num_row, 0);
  for (HighsInt row = 0; row < num_row; row++) rhs[row] = Xrhs[row];
  basisSolveInterface(rhs, solution_vector, solution_num_nz, solution_indices,
                      false);
  return HighsStatus::kOk;
}

HighsStatus Highs::getBasisTransposeSolve(const double* Xrhs,
                                          double* solution_vector,
                                          HighsInt* solution_num_nz,
                                          HighsInt* solution_indices) {
  if (Xrhs == NULL) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "getBasisTransposeSolve: Xrhs is NULL\n");
    return HighsStatus::kError;
  }
  if (solution_vector == NULL) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "getBasisTransposeSolve: solution_vector is NULL\n");
    return HighsStatus::kError;
  }
  // solution_indices can be NULL - it's the trigger that determines
  // whether they are identified or not
  if (!ekk_instance_.status_.has_invert)
    return invertRequirementError("getBasisTransposeSolve");
  HighsInt num_row = model_.lp_.num_row_;
  vector<double> rhs;
  rhs.assign(num_row, 0);
  for (HighsInt row = 0; row < num_row; row++) rhs[row] = Xrhs[row];
  basisSolveInterface(rhs, solution_vector, solution_num_nz, solution_indices,
                      true);
  return HighsStatus::kOk;
}

HighsStatus Highs::getReducedRow(const HighsInt row, double* row_vector,
                                 HighsInt* row_num_nz, HighsInt* row_indices,
                                 const double* pass_basis_inverse_row_vector) {
  HighsLp& lp = model_.lp_;
  // Ensure that the LP is column-wise
  lp.ensureColwise();
  if (row_vector == NULL) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "getReducedRow: row_vector is NULL\n");
    return HighsStatus::kError;
  }
  // row_indices can be NULL - it's the trigger that determines
  // whether they are identified or not pass_basis_inverse_row_vector
  // NULL - it's the trigger to determine whether it's computed or not
  if (row < 0 || row >= lp.num_row_) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Row index %" HIGHSINT_FORMAT
                 " out of range [0, %" HIGHSINT_FORMAT "] in getReducedRow\n",
                 row, lp.num_row_ - 1);
    return HighsStatus::kError;
  }
  if (!ekk_instance_.status_.has_invert)
    return invertRequirementError("getReducedRow");
  HighsInt num_row = lp.num_row_;
  vector<double> basis_inverse_row;
  double* basis_inverse_row_vector = (double*)pass_basis_inverse_row_vector;
  if (basis_inverse_row_vector == NULL) {
    vector<double> rhs;
    vector<HighsInt> col_indices;
    rhs.assign(num_row, 0);
    rhs[row] = 1;
    basis_inverse_row.resize(num_row, 0);
    // Form B^{-T}e_{row}
    basisSolveInterface(rhs, basis_inverse_row.data(), NULL, NULL, true);
    basis_inverse_row_vector = basis_inverse_row.data();
  }
  bool return_indices = row_num_nz != NULL;
  if (return_indices) *row_num_nz = 0;
  for (HighsInt col = 0; col < lp.num_col_; col++) {
    double value = 0;
    for (HighsInt el = lp.a_matrix_.start_[col];
         el < lp.a_matrix_.start_[col + 1]; el++) {
      HighsInt row = lp.a_matrix_.index_[el];
      value += lp.a_matrix_.value_[el] * basis_inverse_row_vector[row];
    }
    row_vector[col] = 0;
    if (fabs(value) > kHighsTiny) {
      if (return_indices) row_indices[(*row_num_nz)++] = col;
      row_vector[col] = value;
    }
  }
  return HighsStatus::kOk;
}

HighsStatus Highs::getReducedColumn(const HighsInt col, double* col_vector,
                                    HighsInt* col_num_nz,
                                    HighsInt* col_indices) {
  HighsLp& lp = model_.lp_;
  // Ensure that the LP is column-wise
  lp.ensureColwise();
  if (col_vector == NULL) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "getReducedColumn: col_vector is NULL\n");
    return HighsStatus::kError;
  }
  // col_indices can be NULL - it's the trigger that determines
  // whether they are identified or not
  if (col < 0 || col >= lp.num_col_) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Column index %" HIGHSINT_FORMAT
                 " out of range [0, %" HIGHSINT_FORMAT
                 "] in getReducedColumn\n",
                 col, lp.num_col_ - 1);
    return HighsStatus::kError;
  }
  if (!ekk_instance_.status_.has_invert)
    return invertRequirementError("getReducedColumn");
  HighsInt num_row = lp.num_row_;
  vector<double> rhs;
  rhs.assign(num_row, 0);
  for (HighsInt el = lp.a_matrix_.start_[col];
       el < lp.a_matrix_.start_[col + 1]; el++)
    rhs[lp.a_matrix_.index_[el]] = lp.a_matrix_.value_[el];
  basisSolveInterface(rhs, col_vector, col_num_nz, col_indices, false);
  return HighsStatus::kOk;
}

HighsStatus Highs::getKappa(double& kappa, const bool exact,
                            const bool report) {
  if (!ekk_instance_.status_.has_invert)
    return invertRequirementError("getBasisInverseRow");
  kappa = ekk_instance_.computeBasisCondition(this->model_.lp_, exact, report);
  return HighsStatus::kOk;
}

HighsStatus Highs::setSolution(const HighsSolution& solution) {
  HighsStatus return_status = HighsStatus::kOk;
  // Determine whether a new solution will be defined. If so,
  // the old solution and any basis are cleared
  const bool new_primal_solution =
      model_.lp_.num_col_ > 0 &&
      solution.col_value.size() >= static_cast<size_t>(model_.lp_.num_col_);
  const bool new_dual_solution =
      model_.lp_.num_row_ > 0 &&
      solution.row_dual.size() >= static_cast<size_t>(model_.lp_.num_row_);
  const bool new_solution = new_primal_solution || new_dual_solution;

  if (new_solution) {
    invalidateUserSolverData();
  } else {
    // Solution is rejected, so give a logging message and error
    // return
    highsLogUser(
        options_.log_options, HighsLogType::kError,
        "setSolution: User solution is rejected due to mismatch between "
        "size of col_value and row_dual vectors (%d, %d) and number "
        "of columns and rows in the model (%d, %d)\n",
        int(solution.col_value.size()), int(solution.row_dual.size()),
        int(model_.lp_.num_col_), int(model_.lp_.num_row_));
    return_status = HighsStatus::kError;
  }

  if (new_primal_solution) {
    solution_.col_value = solution.col_value;
    if (model_.lp_.num_row_ > 0) {
      // Worth computing the row values
      solution_.row_value.resize(model_.lp_.num_row_);
      // Matrix must be column-wise
      model_.lp_.a_matrix_.ensureColwise();
      return_status = interpretCallStatus(
          options_.log_options, calculateRowValuesQuad(model_.lp_, solution_),
          return_status, "calculateRowValuesQuad");
      if (return_status == HighsStatus::kError) return return_status;
    }
    solution_.value_valid = true;
  }
  if (new_dual_solution) {
    solution_.row_dual = solution.row_dual;
    if (model_.lp_.num_col_ > 0) {
      // Worth computing the column duals
      solution_.col_dual.resize(model_.lp_.num_col_);
      // Matrix must be column-wise
      model_.lp_.a_matrix_.ensureColwise();
      return_status = interpretCallStatus(
          options_.log_options, calculateColDualsQuad(model_.lp_, solution_),
          return_status, "calculateColDuals");
      if (return_status == HighsStatus::kError) return return_status;
    }
    solution_.dual_valid = true;
  }
  return returnFromHighs(return_status);
}

HighsStatus Highs::setSolution(const HighsInt num_entries,
                               const HighsInt* index, const double* value) {
  HighsStatus return_status = HighsStatus::kOk;
  // Warn about duplicates in index
  HighsInt num_duplicates = 0;
  std::vector<bool> is_set;
  is_set.assign(model_.lp_.num_col_, false);
  for (HighsInt iX = 0; iX < num_entries; iX++) {
    HighsInt iCol = index[iX];
    if (iCol < 0 || iCol > model_.lp_.num_col_) {
      highsLogUser(options_.log_options, HighsLogType::kError,
                   "setSolution: User solution index %d has value %d out of "
                   "range [0, %d)",
                   int(iX), int(iCol), int(model_.lp_.num_col_));
      return HighsStatus::kError;
    } else if (value[iX] < model_.lp_.col_lower_[iCol] -
                               options_.primal_feasibility_tolerance ||
               model_.lp_.col_upper_[iCol] +
                       options_.primal_feasibility_tolerance <
                   value[iX]) {
      highsLogUser(options_.log_options, HighsLogType::kError,
                   "setSolution: User solution value %d of %g is infeasible "
                   "for bounds [%g, %g]",
                   int(iX), value[iX], model_.lp_.col_lower_[iCol],
                   model_.lp_.col_upper_[iCol]);
      return HighsStatus::kError;
    }
    if (is_set[iCol]) num_duplicates++;
    is_set[iCol] = true;
  }
  if (num_duplicates > 0) {
    highsLogUser(options_.log_options, HighsLogType::kWarning,
                 "setSolution: User set of indices has %d duplicate%s: last "
                 "value used\n",
                 int(num_duplicates), num_duplicates > 1 ? "s" : "");
    return_status = HighsStatus::kWarning;
  }

  // Clear the solution, indicate the values not determined by the
  // user, and insert the values determined by the user
  HighsSolution new_solution;
  new_solution.col_value.assign(model_.lp_.num_col_, kHighsUndefined);
  for (HighsInt iX = 0; iX < num_entries; iX++) {
    HighsInt iCol = index[iX];
    new_solution.col_value[iCol] = value[iX];
  }
  return interpretCallStatus(options_.log_options, setSolution(new_solution),
                             return_status, "setSolution");
}

HighsStatus Highs::setCallback(HighsCallbackFunctionType user_callback,
                               void* user_callback_data) {
  this->callback_.clear();
  this->callback_.user_callback = user_callback;
  this->callback_.user_callback_data = user_callback_data;

  options_.log_options.user_callback = this->callback_.user_callback;
  options_.log_options.user_callback_data = this->callback_.user_callback_data;
  options_.log_options.user_callback_active = false;
  return HighsStatus::kOk;
}

HighsStatus Highs::setCallback(HighsCCallbackType c_callback,
                               void* user_callback_data) {
  this->callback_.clear();
  this->callback_.user_callback =
      [c_callback](int a, const std::string& b, const HighsCallbackDataOut* c,
                   HighsCallbackDataIn* d,
                   void* e) { c_callback(a, b.c_str(), c, d, e); };
  this->callback_.user_callback_data = user_callback_data;

  options_.log_options.user_callback = this->callback_.user_callback;
  options_.log_options.user_callback_data = this->callback_.user_callback_data;
  options_.log_options.user_callback_active = false;
  return HighsStatus::kOk;
}

HighsStatus Highs::startCallback(const int callback_type) {
  const bool callback_type_ok =
      callback_type >= kCallbackMin && callback_type <= kCallbackMax;
  assert(callback_type_ok);
  if (!callback_type_ok) return HighsStatus::kError;
  if (!this->callback_.user_callback) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Cannot start callback when user_callback not defined\n");
    return HighsStatus::kError;
  }
  assert(int(this->callback_.active.size()) == kNumCallbackType);
  this->callback_.active[callback_type] = true;
  // Possibly modify the logging callback activity
  if (callback_type == kCallbackLogging)
    options_.log_options.user_callback_active = true;
  return HighsStatus::kOk;
}

HighsStatus Highs::startCallback(const HighsCallbackType callback_type) {
  const bool callback_type_ok =
      callback_type >= kCallbackMin && callback_type <= kCallbackMax;
  assert(callback_type_ok);
  if (!callback_type_ok) return HighsStatus::kError;
  if (!this->callback_.user_callback) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Cannot start callback when user_callback not defined\n");
    return HighsStatus::kError;
  }
  assert(int(this->callback_.active.size()) == kNumCallbackType);
  this->callback_.active[callback_type] = true;
  // Possibly modify the logging callback activity
  if (callback_type == kCallbackLogging)
    options_.log_options.user_callback_active = true;
  return HighsStatus::kOk;
}

HighsStatus Highs::stopCallback(const int callback_type) {
  const bool callback_type_ok =
      callback_type >= kCallbackMin && callback_type <= kCallbackMax;
  assert(callback_type_ok);
  if (!callback_type_ok) return HighsStatus::kError;
  if (!this->callback_.user_callback) {
    highsLogUser(options_.log_options, HighsLogType::kWarning,
                 "Cannot stop callback when user_callback not defined\n");
    return HighsStatus::kWarning;
  }
  assert(int(this->callback_.active.size()) == kNumCallbackType);
  this->callback_.active[callback_type] = false;
  // Possibly modify the logging callback activity
  if (callback_type == kCallbackLogging)
    options_.log_options.user_callback_active = false;
  return HighsStatus::kOk;
}

HighsStatus Highs::stopCallback(const HighsCallbackType callback_type) {
  const bool callback_type_ok =
      callback_type >= kCallbackMin && callback_type <= kCallbackMax;
  assert(callback_type_ok);
  if (!callback_type_ok) return HighsStatus::kError;
  if (!this->callback_.user_callback) {
    highsLogUser(options_.log_options, HighsLogType::kWarning,
                 "Cannot stop callback when user_callback not defined\n");
    return HighsStatus::kWarning;
  }
  assert(int(this->callback_.active.size()) == kNumCallbackType);
  this->callback_.active[callback_type] = false;
  // Possibly modify the logging callback activity
  if (callback_type == kCallbackLogging)
    options_.log_options.user_callback_active = false;
  return HighsStatus::kOk;
}

HighsStatus Highs::setBasis(const HighsBasis& basis,
                            const std::string& origin) {
  if (basis.alien) {
    // An alien basis needs to be checked properly, since it may be
    // singular, or even incomplete.
    if (model_.lp_.num_row_ == 0) {
      // Special case where there are no rows, so no singularity
      // issues. All columns with basic status must be set nonbasic
      for (HighsInt iCol = 0; iCol < model_.lp_.num_col_; iCol++)
        basis_.col_status[iCol] =
            basis.col_status[iCol] == HighsBasisStatus::kBasic
                ? HighsBasisStatus::kNonbasic
                : basis.col_status[iCol];
      basis_.alien = false;
    } else {
      // Check whether a new basis can be defined
      if (!isBasisRightSize(model_.lp_, basis)) {
        highsLogUser(
            options_.log_options, HighsLogType::kError,
            "setBasis: User basis is rejected due to mismatch between "
            "size of column and row status vectors (%d, %d) and number "
            "of columns and rows in the model (%d, %d)\n",
            int(basis_.col_status.size()), int(basis_.row_status.size()),
            int(model_.lp_.num_col_), int(model_.lp_.num_row_));
        return HighsStatus::kError;
      }
      HighsBasis modifiable_basis = basis;
      modifiable_basis.was_alien = true;
      HighsLpSolverObject solver_object(model_.lp_, modifiable_basis, solution_,
                                        info_, ekk_instance_, callback_,
                                        options_, timer_);
      HighsStatus return_status = formSimplexLpBasisAndFactor(solver_object);
      if (return_status != HighsStatus::kOk) return HighsStatus::kError;
      // Update the HiGHS basis
      basis_ = std::move(modifiable_basis);
    }
  } else {
    // Check the user-supplied basis
    if (!isBasisConsistent(model_.lp_, basis)) {
      highsLogUser(options_.log_options, HighsLogType::kError,
                   "setBasis: invalid basis\n");
      return HighsStatus::kError;
    }
    // Update the HiGHS basis
    basis_ = basis;
  }
  basis_.valid = true;
  if (origin != "") basis_.debug_origin_name = origin;
  assert(basis_.debug_origin_name != "");
  assert(!basis_.alien);
  if (basis_.was_alien) {
    highsLogDev(
        options_.log_options, HighsLogType::kInfo,
        "Highs::setBasis Was alien = %-5s; Id = %9d; UpdateCount = %4d; Origin "
        "(%s)\n",
        highsBoolToString(basis_.was_alien).c_str(), (int)basis_.debug_id,
        (int)basis_.debug_update_count, basis_.debug_origin_name.c_str());
  }

  // Follow implications of a new HiGHS basis
  newHighsBasis();
  // Can't use returnFromHighs since...
  return HighsStatus::kOk;
}

HighsStatus Highs::setBasis() {
  // Invalidate the basis for HiGHS
  //
  // Don't set to logical basis since that causes presolve to be
  // skipped
  basis_.invalidate();
  // Follow implications of a new HiGHS basis
  newHighsBasis();
  // Can't use returnFromHighs since...
  return HighsStatus::kOk;
}

HighsStatus Highs::setHotStart(const HotStart& hot_start) {
  // Check that the user-supplied hot start is valid
  if (!hot_start.valid) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "setHotStart: invalid hot start\n");
    return HighsStatus::kError;
  }
  HighsStatus return_status = setHotStartInterface(hot_start);
  return returnFromHighs(return_status);
}

HighsStatus Highs::freezeBasis(HighsInt& frozen_basis_id) {
  frozen_basis_id = kNoLink;
  // Check that there is a simplex basis to freeze
  if (!ekk_instance_.status_.has_invert) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "freezeBasis: no simplex factorization to freeze\n");
    return HighsStatus::kError;
  }
  ekk_instance_.freezeBasis(frozen_basis_id);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::unfreezeBasis(const HighsInt frozen_basis_id) {
  // Check that there is a simplex basis to unfreeze
  if (!ekk_instance_.status_.initialised_for_new_lp) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "unfreezeBasis: no simplex information to unfreeze\n");
    return HighsStatus::kError;
  }
  HighsStatus call_status = ekk_instance_.unfreezeBasis(frozen_basis_id);
  if (call_status != HighsStatus::kOk) return call_status;
  // Reset simplex NLA pointers
  ekk_instance_.setNlaPointersForTrans(model_.lp_);
  // Get the corresponding HiGHS basis
  basis_ = ekk_instance_.getHighsBasis(model_.lp_);
  // Clear everything else
  invalidateModelStatusSolutionAndInfo();
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::putIterate() {
  // Check that there is a simplex iterate to put
  if (!ekk_instance_.status_.has_invert) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "putIterate: no simplex iterate to put\n");
    return HighsStatus::kError;
  }
  ekk_instance_.putIterate();
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::getIterate() {
  // Check that there is a simplex iterate to get
  if (!ekk_instance_.status_.initialised_for_new_lp) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "getIterate: no simplex iterate to get\n");
    return HighsStatus::kError;
  }
  HighsStatus call_status = ekk_instance_.getIterate();
  if (call_status != HighsStatus::kOk) return call_status;
  // Get the corresponding HiGHS basis
  basis_ = ekk_instance_.getHighsBasis(model_.lp_);
  // Clear everything else
  invalidateModelStatusSolutionAndInfo();
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::addCol(const double cost, const double lower_bound,
                          const double upper_bound, const HighsInt num_new_nz,
                          const HighsInt* indices, const double* values) {
  this->logHeader();
  HighsInt starts = 0;
  return addCols(1, &cost, &lower_bound, &upper_bound, num_new_nz, &starts,
                 indices, values);
}

HighsStatus Highs::addCols(const HighsInt num_new_col, const double* costs,
                           const double* lower_bounds,
                           const double* upper_bounds,
                           const HighsInt num_new_nz, const HighsInt* starts,
                           const HighsInt* indices, const double* values) {
  this->logHeader();
  HighsStatus return_status = HighsStatus::kOk;
  clearPresolve();
  clearStandardFormLp();
  return_status = interpretCallStatus(
      options_.log_options,
      addColsInterface(num_new_col, costs, lower_bounds, upper_bounds,
                       num_new_nz, starts, indices, values),
      return_status, "addCols");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::addVars(const HighsInt num_new_var, const double* lower,
                           const double* upper) {
  this->logHeader();
  HighsStatus return_status = HighsStatus::kOk;
  // Avoid touching entry [0] of a vector of size 0
  if (num_new_var <= 0) returnFromHighs(return_status);
  std::vector<double> cost;
  cost.assign(num_new_var, 0);
  return addCols(num_new_var, cost.data(), lower, upper, 0, nullptr, nullptr,
                 nullptr);
}

HighsStatus Highs::addRow(const double lower_bound, const double upper_bound,
                          const HighsInt num_new_nz, const HighsInt* indices,
                          const double* values) {
  this->logHeader();
  HighsInt starts = 0;
  return addRows(1, &lower_bound, &upper_bound, num_new_nz, &starts, indices,
                 values);
}

HighsStatus Highs::addRows(const HighsInt num_new_row,
                           const double* lower_bounds,
                           const double* upper_bounds,
                           const HighsInt num_new_nz, const HighsInt* starts,
                           const HighsInt* indices, const double* values) {
  this->logHeader();
  HighsStatus return_status = HighsStatus::kOk;
  clearPresolve();
  clearStandardFormLp();
  return_status = interpretCallStatus(
      options_.log_options,
      addRowsInterface(num_new_row, lower_bounds, upper_bounds, num_new_nz,
                       starts, indices, values),
      return_status, "addRows");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::changeObjectiveSense(const ObjSense sense) {
  if ((sense == ObjSense::kMinimize) !=
      (model_.lp_.sense_ == ObjSense::kMinimize)) {
    model_.lp_.sense_ = sense;
    // Nontrivial change
    clearPresolve();
    clearStandardFormLp();
    invalidateModelStatusSolutionAndInfo();
  }
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::changeObjectiveOffset(const double offset) {
  // Update the objective value
  info_.objective_function_value += (offset - model_.lp_.offset_);
  model_.lp_.offset_ = offset;
  presolved_model_.lp_.offset_ += offset;
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::changeColIntegrality(const HighsInt col,
                                        const HighsVarType integrality) {
  return changeColsIntegrality(1, &col, &integrality);
}

HighsStatus Highs::changeColsIntegrality(const HighsInt from_col,
                                         const HighsInt to_col,
                                         const HighsVarType* integrality) {
  clearPresolve();
  HighsIndexCollection index_collection;
  const HighsInt create_error =
      create(index_collection, from_col, to_col, model_.lp_.num_col_);
  if (create_error) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Interval [%d, %d] supplied to Highs::changeColsIntegrality "
                 "is out of range [0, %d)\n",
                 int(from_col), int(to_col), int(model_.lp_.num_col_));
    return HighsStatus::kError;
  }
  HighsStatus call_status =
      changeIntegralityInterface(index_collection, integrality);
  HighsStatus return_status = HighsStatus::kOk;
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "changeIntegrality");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

static HighsStatus analyseSetCreateError(HighsLogOptions log_options,
                                         const std::string method,
                                         const HighsInt create_error,
                                         const bool ordered,
                                         const HighsInt num_set_entries,
                                         const HighsInt* set,
                                         const HighsInt dimension) {
  if (create_error == kIndexCollectionCreateIllegalSetSize) {
    highsLogUser(log_options, HighsLogType::kError,
                 "Set supplied to Highs::%s has illegal size of %d\n",
                 method.c_str(), int(num_set_entries));
  } else if (create_error == kIndexCollectionCreateIllegalSetOrder) {
    if (ordered) {
      // Creating an index_collection data structure for the set
      // includes a test that the indices increase strictly. If this
      // is not the case then, since an increasing set was created
      // locally, it must contain duplicate entries. return with an
      // error
      highsLogUser(log_options, HighsLogType::kError,
                   "Set supplied to Highs::%s contains duplicate entries\n",
                   method.c_str());
    } else {
      highsLogUser(log_options, HighsLogType::kError,
                   "Set supplied to Highs::%s not ordered\n", method.c_str());
    }
  } else if (create_error < 0) {
    HighsInt illegal_set_index = -1 - create_error;
    HighsInt illegal_set_entry = set[illegal_set_index];
    highsLogUser(
        log_options, HighsLogType::kError,
        "Set supplied to Highs::%s has entry %d of %d out of range [0, %d)\n",
        method.c_str(), int(illegal_set_index), int(illegal_set_entry),
        int(dimension));
  }
  assert(create_error != kIndexCollectionCreateIllegalSetDimension);
  return HighsStatus::kError;
}

HighsStatus Highs::changeColsIntegrality(const HighsInt num_set_entries,
                                         const HighsInt* set,
                                         const HighsVarType* integrality) {
  if (num_set_entries == 0) return HighsStatus::kOk;
  clearPresolve();
  // Ensure that the set and data are in ascending order
  std::vector<HighsVarType> local_integrality{integrality,
                                              integrality + num_set_entries};
  std::vector<HighsInt> local_set{set, set + num_set_entries};
  sortSetData(num_set_entries, local_set, integrality,
              local_integrality.data());
  HighsIndexCollection index_collection;
  const HighsInt create_error = create(index_collection, num_set_entries,
                                       local_set.data(), model_.lp_.num_col_);
  if (create_error)
    return analyseSetCreateError(options_.log_options, "changeColsIntegrality",
                                 create_error, true, num_set_entries,
                                 local_set.data(), model_.lp_.num_col_);
  HighsStatus call_status =
      changeIntegralityInterface(index_collection, local_integrality.data());
  HighsStatus return_status = HighsStatus::kOk;
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "changeIntegrality");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::changeColsIntegrality(const HighsInt* mask,
                                         const HighsVarType* integrality) {
  clearPresolve();
  HighsIndexCollection index_collection;
  const bool create_error = create(index_collection, mask, model_.lp_.num_col_);
  assert(!create_error);
  (void)create_error;
  HighsStatus call_status =
      changeIntegralityInterface(index_collection, integrality);
  HighsStatus return_status = HighsStatus::kOk;
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "changeIntegrality");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::changeColCost(const HighsInt col, const double cost) {
  return changeColsCost(1, &col, &cost);
}

HighsStatus Highs::changeColsCost(const HighsInt from_col,
                                  const HighsInt to_col, const double* cost) {
  clearPresolve();
  clearStandardFormLp();
  HighsIndexCollection index_collection;
  const HighsInt create_error =
      create(index_collection, from_col, to_col, model_.lp_.num_col_);
  if (create_error) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Interval [%d, %d] supplied to Highs::changeColsCost is out "
                 "of range [0, %d)\n",
                 int(from_col), int(to_col), int(model_.lp_.num_col_));
    return HighsStatus::kError;
  }
  HighsStatus call_status = changeCostsInterface(index_collection, cost);
  HighsStatus return_status = HighsStatus::kOk;
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "changeCosts");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::changeColsCost(const HighsInt num_set_entries,
                                  const HighsInt* set, const double* cost) {
  if (num_set_entries == 0) return HighsStatus::kOk;
  // Check for NULL data in "set" version of changeColsCost since
  // values are sorted with set
  if (doubleUserDataNotNull(options_.log_options, cost, "column costs"))
    return HighsStatus::kError;
  clearPresolve();
  clearStandardFormLp();
  // Ensure that the set and data are in ascending order
  std::vector<double> local_cost{cost, cost + num_set_entries};
  std::vector<HighsInt> local_set{set, set + num_set_entries};
  sortSetData(num_set_entries, local_set, cost, NULL, NULL, local_cost.data(),
              NULL, NULL);
  HighsIndexCollection index_collection;
  const HighsInt create_error = create(index_collection, num_set_entries,
                                       local_set.data(), model_.lp_.num_col_);
  if (create_error)
    return analyseSetCreateError(options_.log_options, "changeColsCost",
                                 create_error, true, num_set_entries,
                                 local_set.data(), model_.lp_.num_col_);
  HighsStatus call_status =
      changeCostsInterface(index_collection, local_cost.data());
  HighsStatus return_status = HighsStatus::kOk;
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "changeCosts");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::changeColsCost(const HighsInt* mask, const double* cost) {
  clearPresolve();
  clearStandardFormLp();
  HighsIndexCollection index_collection;
  const bool create_error = create(index_collection, mask, model_.lp_.num_col_);
  assert(!create_error);
  (void)create_error;
  HighsStatus call_status = changeCostsInterface(index_collection, cost);
  HighsStatus return_status = HighsStatus::kOk;
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "changeCosts");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::changeColBounds(const HighsInt col, const double lower,
                                   const double upper) {
  return changeColsBounds(1, &col, &lower, &upper);
}

HighsStatus Highs::changeColsBounds(const HighsInt from_col,
                                    const HighsInt to_col, const double* lower,
                                    const double* upper) {
  clearPresolve();
  clearStandardFormLp();
  HighsIndexCollection index_collection;
  const HighsInt create_error =
      create(index_collection, from_col, to_col, model_.lp_.num_col_);
  if (create_error) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Interval [%d, %d] supplied to Highs::changeColsBounds is out "
                 "of range [0, %d)\n",
                 int(from_col), int(to_col), int(model_.lp_.num_col_));
    return HighsStatus::kError;
  }
  HighsStatus call_status =
      changeColBoundsInterface(index_collection, lower, upper);
  HighsStatus return_status = HighsStatus::kOk;
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "changeColBounds");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::changeColsBounds(const HighsInt num_set_entries,
                                    const HighsInt* set, const double* lower,
                                    const double* upper) {
  if (num_set_entries == 0) return HighsStatus::kOk;
  // Check for NULL data in "set" version of changeColsBounds since
  // values are sorted with set
  bool null_data = false;
  null_data = doubleUserDataNotNull(options_.log_options, lower,
                                    "column lower bounds") ||
              null_data;
  null_data = doubleUserDataNotNull(options_.log_options, upper,
                                    "column upper bounds") ||
              null_data;
  if (null_data) return HighsStatus::kError;
  clearPresolve();
  clearStandardFormLp();
  // Ensure that the set and data are in ascending order
  std::vector<double> local_lower{lower, lower + num_set_entries};
  std::vector<double> local_upper{upper, upper + num_set_entries};
  std::vector<HighsInt> local_set{set, set + num_set_entries};
  sortSetData(num_set_entries, local_set, lower, upper, NULL,
              local_lower.data(), local_upper.data(), NULL);
  HighsIndexCollection index_collection;
  const HighsInt create_error = create(index_collection, num_set_entries,
                                       local_set.data(), model_.lp_.num_col_);
  if (create_error)
    return analyseSetCreateError(options_.log_options, "changeColsBounds",
                                 create_error, true, num_set_entries,
                                 local_set.data(), model_.lp_.num_col_);
  HighsStatus call_status = changeColBoundsInterface(
      index_collection, local_lower.data(), local_upper.data());
  HighsStatus return_status = HighsStatus::kOk;
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "changeColBounds");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::changeColsBounds(const HighsInt* mask, const double* lower,
                                    const double* upper) {
  clearPresolve();
  clearStandardFormLp();
  HighsIndexCollection index_collection;
  const bool create_error = create(index_collection, mask, model_.lp_.num_col_);
  assert(!create_error);
  (void)create_error;
  HighsStatus call_status =
      changeColBoundsInterface(index_collection, lower, upper);
  HighsStatus return_status = HighsStatus::kOk;
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "changeColBounds");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::changeRowBounds(const HighsInt row, const double lower,
                                   const double upper) {
  return changeRowsBounds(1, &row, &lower, &upper);
}

HighsStatus Highs::changeRowsBounds(const HighsInt from_row,
                                    const HighsInt to_row, const double* lower,
                                    const double* upper) {
  clearPresolve();
  clearStandardFormLp();
  HighsIndexCollection index_collection;
  const HighsInt create_error =
      create(index_collection, from_row, to_row, model_.lp_.num_row_);
  if (create_error) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Interval [%d, %d] supplied to Highs::changeRowsBounds is out "
                 "of range [0, %d)\n",
                 int(from_row), int(to_row), int(model_.lp_.num_row_));
    return HighsStatus::kError;
  }
  HighsStatus call_status =
      changeRowBoundsInterface(index_collection, lower, upper);
  HighsStatus return_status = HighsStatus::kOk;
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "changeRowBounds");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::changeRowsBounds(const HighsInt num_set_entries,
                                    const HighsInt* set, const double* lower,
                                    const double* upper) {
  if (num_set_entries == 0) return HighsStatus::kOk;
  // Check for NULL data in "set" version of changeRowsBounds since
  // values are sorted with set
  bool null_data = false;
  null_data =
      doubleUserDataNotNull(options_.log_options, lower, "row lower bounds") ||
      null_data;
  null_data =
      doubleUserDataNotNull(options_.log_options, upper, "row upper bounds") ||
      null_data;
  if (null_data) return HighsStatus::kError;
  clearPresolve();
  clearStandardFormLp();
  // Ensure that the set and data are in ascending order
  std::vector<double> local_lower{lower, lower + num_set_entries};
  std::vector<double> local_upper{upper, upper + num_set_entries};
  std::vector<HighsInt> local_set{set, set + num_set_entries};
  sortSetData(num_set_entries, local_set, lower, upper, NULL,
              local_lower.data(), local_upper.data(), NULL);
  HighsIndexCollection index_collection;
  const HighsInt create_error = create(index_collection, num_set_entries,
                                       local_set.data(), model_.lp_.num_row_);
  if (create_error)
    return analyseSetCreateError(options_.log_options, "changeRowsBounds",
                                 create_error, true, num_set_entries,
                                 local_set.data(), model_.lp_.num_row_);
  HighsStatus call_status = changeRowBoundsInterface(
      index_collection, local_lower.data(), local_upper.data());
  HighsStatus return_status = HighsStatus::kOk;
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "changeRowBounds");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::changeRowsBounds(const HighsInt* mask, const double* lower,
                                    const double* upper) {
  clearPresolve();
  clearStandardFormLp();
  HighsIndexCollection index_collection;
  const bool create_error = create(index_collection, mask, model_.lp_.num_row_);
  assert(!create_error);
  (void)create_error;
  HighsStatus call_status =
      changeRowBoundsInterface(index_collection, lower, upper);
  HighsStatus return_status = HighsStatus::kOk;
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "changeRowBounds");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::changeCoeff(const HighsInt row, const HighsInt col,
                               const double value) {
  if (row < 0 || row >= model_.lp_.num_row_) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Row %" HIGHSINT_FORMAT
                 " supplied to Highs::changeCoeff is not in the range [0, "
                 "%" HIGHSINT_FORMAT "]\n",
                 row, model_.lp_.num_row_);
    return HighsStatus::kError;
  }
  if (col < 0 || col >= model_.lp_.num_col_) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Col %" HIGHSINT_FORMAT
                 " supplied to Highs::changeCoeff is not in the range [0, "
                 "%" HIGHSINT_FORMAT "]\n",
                 col, model_.lp_.num_col_);
    return HighsStatus::kError;
  }
  const double abs_value = std::fabs(value);
  if (0 < abs_value && abs_value <= options_.small_matrix_value) {
    highsLogUser(options_.log_options, HighsLogType::kWarning,
                 "|Value| of %g supplied to Highs::changeCoeff is in (0, %g]: "
                 "zeroes any existing coefficient, otherwise ignored\n",
                 abs_value, options_.small_matrix_value);
  }
  changeCoefficientInterface(row, col, value);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::getObjectiveSense(ObjSense& sense) const {
  sense = model_.lp_.sense_;
  return HighsStatus::kOk;
}

HighsStatus Highs::getObjectiveOffset(double& offset) const {
  offset = model_.lp_.offset_;
  return HighsStatus::kOk;
}

HighsStatus Highs::getCols(const HighsInt from_col, const HighsInt to_col,
                           HighsInt& num_col, double* costs, double* lower,
                           double* upper, HighsInt& num_nz, HighsInt* start,
                           HighsInt* index, double* value) {
  if (from_col > to_col) {
    // Empty interval
    num_col = 0;
    num_nz = 0;
    return HighsStatus::kOk;
  }
  HighsIndexCollection index_collection;
  const HighsInt create_error =
      create(index_collection, from_col, to_col, model_.lp_.num_col_);
  if (create_error) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Interval [%d, %d] supplied to Highs::getCols is out of range "
                 "[0, %d)\n",
                 int(from_col), int(to_col), int(model_.lp_.num_col_));
    return HighsStatus::kError;
  }
  getColsInterface(index_collection, num_col, costs, lower, upper, num_nz,
                   start, index, value);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::getCols(const HighsInt num_set_entries, const HighsInt* set,
                           HighsInt& num_col, double* costs, double* lower,
                           double* upper, HighsInt& num_nz, HighsInt* start,
                           HighsInt* index, double* value) {
  if (num_set_entries == 0) {
    // Empty interval
    num_col = 0;
    num_nz = 0;
    return HighsStatus::kOk;
  }
  HighsIndexCollection index_collection;
  const HighsInt create_error =
      create(index_collection, num_set_entries, set, model_.lp_.num_col_);
  if (create_error)
    return analyseSetCreateError(options_.log_options, "getCols", create_error,
                                 false, num_set_entries, set,
                                 model_.lp_.num_col_);
  getColsInterface(index_collection, num_col, costs, lower, upper, num_nz,
                   start, index, value);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::getCols(const HighsInt* mask, HighsInt& num_col,
                           double* costs, double* lower, double* upper,
                           HighsInt& num_nz, HighsInt* start, HighsInt* index,
                           double* value) {
  HighsIndexCollection index_collection;
  const bool create_error = create(index_collection, mask, model_.lp_.num_col_);
  assert(!create_error);
  (void)create_error;
  getColsInterface(index_collection, num_col, costs, lower, upper, num_nz,
                   start, index, value);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::getColName(const HighsInt col, std::string& name) const {
  const HighsInt num_col = this->model_.lp_.num_col_;
  if (col < 0 || col >= num_col) {
    highsLogUser(
        options_.log_options, HighsLogType::kError,
        "Index %d for column name is outside the range [0, num_col = %d)\n",
        int(col), int(num_col));
    return HighsStatus::kError;
  }
  const HighsInt num_col_name = this->model_.lp_.col_names_.size();
  if (col >= num_col_name) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Index %d for column name is outside the range [0, "
                 "num_col_name = %d)\n",
                 int(col), int(num_col_name));
    return HighsStatus::kError;
  }
  name = this->model_.lp_.col_names_[col];
  return HighsStatus::kOk;
}

HighsStatus Highs::getColByName(const std::string& name, HighsInt& col) {
  HighsLp& lp = model_.lp_;
  if (!lp.col_names_.size()) return HighsStatus::kError;
  if (!lp.col_hash_.name2index.size()) lp.col_hash_.form(lp.col_names_);
  auto search = lp.col_hash_.name2index.find(name);
  if (search == lp.col_hash_.name2index.end()) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Highs::getColByName: name %s is not found\n", name.c_str());
    return HighsStatus::kError;
  }
  if (search->second == kHashIsDuplicate) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Highs::getColByName: name %s is duplicated\n", name.c_str());
    return HighsStatus::kError;
  }
  col = search->second;
  assert(lp.col_names_[col] == name);
  return HighsStatus::kOk;
}

HighsStatus Highs::getColIntegrality(const HighsInt col,
                                     HighsVarType& integrality) const {
  const HighsInt num_col = this->model_.lp_.num_col_;
  if (col < 0 || col >= num_col) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Index %d for column integrality is outside the range [0, "
                 "num_col = %d)\n",
                 int(col), int(num_col));
    return HighsStatus::kError;
  }
  if (static_cast<size_t>(col) < this->model_.lp_.integrality_.size()) {
    integrality = this->model_.lp_.integrality_[col];
    return HighsStatus::kOk;
  } else {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Model integrality does not exist for index %d\n", int(col));
    return HighsStatus::kError;
  }
}

HighsStatus Highs::getRows(const HighsInt from_row, const HighsInt to_row,
                           HighsInt& num_row, double* lower, double* upper,
                           HighsInt& num_nz, HighsInt* start, HighsInt* index,
                           double* value) {
  if (from_row > to_row) {
    // Empty interval
    num_row = 0;
    num_nz = 0;
    return HighsStatus::kOk;
  }
  HighsIndexCollection index_collection;
  const HighsInt create_error =
      create(index_collection, from_row, to_row, model_.lp_.num_row_);
  if (create_error) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Interval [%d, %d] supplied to Highs::getRows is out of range "
                 "[0, %d)\n",
                 int(from_row), int(to_row), int(model_.lp_.num_row_));
    return HighsStatus::kError;
  }
  getRowsInterface(index_collection, num_row, lower, upper, num_nz, start,
                   index, value);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::getRows(const HighsInt num_set_entries, const HighsInt* set,
                           HighsInt& num_row, double* lower, double* upper,
                           HighsInt& num_nz, HighsInt* start, HighsInt* index,
                           double* value) {
  if (num_set_entries == 0) {
    num_row = 0;
    num_nz = 0;
    return HighsStatus::kOk;
  }
  HighsIndexCollection index_collection;
  const HighsInt create_error =
      create(index_collection, num_set_entries, set, model_.lp_.num_row_);
  if (create_error)
    return analyseSetCreateError(options_.log_options, "getRows", create_error,
                                 false, num_set_entries, set,
                                 model_.lp_.num_row_);
  getRowsInterface(index_collection, num_row, lower, upper, num_nz, start,
                   index, value);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::getRows(const HighsInt* mask, HighsInt& num_row,
                           double* lower, double* upper, HighsInt& num_nz,
                           HighsInt* start, HighsInt* index, double* value) {
  HighsIndexCollection index_collection;
  const bool create_error = create(index_collection, mask, model_.lp_.num_row_);
  assert(!create_error);
  (void)create_error;
  getRowsInterface(index_collection, num_row, lower, upper, num_nz, start,
                   index, value);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::getRowName(const HighsInt row, std::string& name) const {
  const HighsInt num_row = this->model_.lp_.num_row_;
  if (row < 0 || row >= num_row) {
    highsLogUser(
        options_.log_options, HighsLogType::kError,
        "Index %d for row name is outside the range [0, num_row = %d)\n",
        int(row), int(num_row));
    return HighsStatus::kError;
  }
  const HighsInt num_row_name = this->model_.lp_.row_names_.size();
  if (row >= num_row_name) {
    highsLogUser(
        options_.log_options, HighsLogType::kError,
        "Index %d for row name is outside the range [0, num_row_name = %d)\n",
        int(row), int(num_row_name));
    return HighsStatus::kError;
  }
  name = this->model_.lp_.row_names_[row];
  return HighsStatus::kOk;
}

HighsStatus Highs::getRowByName(const std::string& name, HighsInt& row) {
  HighsLp& lp = model_.lp_;
  if (!lp.row_names_.size()) return HighsStatus::kError;
  if (!lp.row_hash_.name2index.size()) lp.row_hash_.form(lp.row_names_);
  auto search = lp.row_hash_.name2index.find(name);
  if (search == lp.row_hash_.name2index.end()) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Highs::getRowByName: name %s is not found\n", name.c_str());
    return HighsStatus::kError;
  }
  if (search->second == kHashIsDuplicate) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Highs::getRowByName: name %s is duplicated\n", name.c_str());
    return HighsStatus::kError;
  }
  row = search->second;
  assert(lp.row_names_[row] == name);
  return HighsStatus::kOk;
}

HighsStatus Highs::getCoeff(const HighsInt row, const HighsInt col,
                            double& value) {
  if (row < 0 || row >= model_.lp_.num_row_) {
    highsLogUser(
        options_.log_options, HighsLogType::kError,
        "Row %" HIGHSINT_FORMAT
        " supplied to Highs::getCoeff is not in the range [0, %" HIGHSINT_FORMAT
        "]\n",
        row, model_.lp_.num_row_);
    return HighsStatus::kError;
  }
  if (col < 0 || col >= model_.lp_.num_col_) {
    highsLogUser(
        options_.log_options, HighsLogType::kError,
        "Col %" HIGHSINT_FORMAT
        " supplied to Highs::getCoeff is not in the range [0, %" HIGHSINT_FORMAT
        "]\n",
        col, model_.lp_.num_col_);
    return HighsStatus::kError;
  }
  getCoefficientInterface(row, col, value);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::deleteCols(const HighsInt from_col, const HighsInt to_col) {
  clearPresolve();
  clearStandardFormLp();
  HighsIndexCollection index_collection;
  const HighsInt create_error =
      create(index_collection, from_col, to_col, model_.lp_.num_col_);
  if (create_error) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Interval [%d, %d] supplied to Highs::deleteCols is out of "
                 "range [0, %d)\n",
                 int(from_col), int(to_col), int(model_.lp_.num_col_));
    return HighsStatus::kError;
  }
  deleteColsInterface(index_collection);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::deleteCols(const HighsInt num_set_entries,
                              const HighsInt* set) {
  if (num_set_entries == 0) return HighsStatus::kOk;
  clearPresolve();
  clearStandardFormLp();
  HighsIndexCollection index_collection;
  const HighsInt create_error =
      create(index_collection, num_set_entries, set, model_.lp_.num_col_);
  if (create_error)
    return analyseSetCreateError(options_.log_options, "deleteCols",
                                 create_error, false, num_set_entries, set,
                                 model_.lp_.num_col_);
  deleteColsInterface(index_collection);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::deleteCols(HighsInt* mask) {
  clearPresolve();
  clearStandardFormLp();
  const HighsInt original_num_col = model_.lp_.num_col_;
  HighsIndexCollection index_collection;
  const bool create_error = create(index_collection, mask, original_num_col);
  assert(!create_error);
  (void)create_error;
  deleteColsInterface(index_collection);
  for (HighsInt iCol = 0; iCol < original_num_col; iCol++)
    mask[iCol] = index_collection.mask_[iCol];
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::deleteRows(const HighsInt from_row, const HighsInt to_row) {
  clearPresolve();
  clearStandardFormLp();
  HighsIndexCollection index_collection;
  const HighsInt create_error =
      create(index_collection, from_row, to_row, model_.lp_.num_row_);
  if (create_error) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Interval [%d, %d] supplied to Highs::deleteRows is out of "
                 "range [0, %d)\n",
                 int(from_row), int(to_row), int(model_.lp_.num_row_));
    return HighsStatus::kError;
  }
  deleteRowsInterface(index_collection);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::deleteRows(const HighsInt num_set_entries,
                              const HighsInt* set) {
  if (num_set_entries == 0) return HighsStatus::kOk;
  clearPresolve();
  clearStandardFormLp();
  HighsIndexCollection index_collection;
  const HighsInt create_error =
      create(index_collection, num_set_entries, set, model_.lp_.num_row_);
  if (create_error)
    return analyseSetCreateError(options_.log_options, "deleteRows",
                                 create_error, false, num_set_entries, set,
                                 model_.lp_.num_row_);
  deleteRowsInterface(index_collection);
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::deleteRows(HighsInt* mask) {
  clearPresolve();
  clearStandardFormLp();
  const HighsInt original_num_row = model_.lp_.num_row_;
  HighsIndexCollection index_collection;
  const bool create_error = create(index_collection, mask, original_num_row);
  assert(!create_error);
  (void)create_error;
  deleteRowsInterface(index_collection);
  for (HighsInt iRow = 0; iRow < original_num_row; iRow++)
    mask[iRow] = index_collection.mask_[iRow];
  return returnFromHighs(HighsStatus::kOk);
}

HighsStatus Highs::scaleCol(const HighsInt col, const double scale_value) {
  HighsStatus return_status = HighsStatus::kOk;
  clearPresolve();
  clearStandardFormLp();
  HighsStatus call_status = scaleColInterface(col, scale_value);
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "scaleCol");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::scaleRow(const HighsInt row, const double scale_value) {
  HighsStatus return_status = HighsStatus::kOk;
  clearPresolve();
  clearStandardFormLp();
  HighsStatus call_status = scaleRowInterface(row, scale_value);
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "scaleRow");
  if (return_status == HighsStatus::kError) return HighsStatus::kError;
  return returnFromHighs(return_status);
}

HighsStatus Highs::postsolve(const HighsSolution& solution) {
  HighsBasis basis;
  return this->postsolve(solution, basis);
}

HighsStatus Highs::postsolve(const HighsSolution& solution,
                             const HighsBasis& basis) {
  const bool can_run_postsolve =
      model_presolve_status_ == HighsPresolveStatus::kNotPresolved ||
      model_presolve_status_ == HighsPresolveStatus::kNotReduced ||
      model_presolve_status_ == HighsPresolveStatus::kReduced ||
      model_presolve_status_ == HighsPresolveStatus::kReducedToEmpty ||
      model_presolve_status_ == HighsPresolveStatus::kTimeout ||
      model_presolve_status_ == HighsPresolveStatus::kOutOfMemory;
  if (!can_run_postsolve) {
    highsLogUser(options_.log_options, HighsLogType::kWarning,
                 "Cannot run postsolve with presolve status: %s\n",
                 presolveStatusToString(model_presolve_status_).c_str());
    return HighsStatus::kWarning;
  }
  HighsStatus return_status = callRunPostsolve(solution, basis);
  return returnFromHighs(return_status);
}

HighsStatus Highs::writeSolution(const std::string& filename,
                                 const HighsInt style) {
  HighsStatus return_status = HighsStatus::kOk;
  HighsStatus call_status;
  FILE* file;
  HighsFileType file_type;
  call_status = openWriteFile(filename, "writeSolution", file, file_type);
  return_status = interpretCallStatus(options_.log_options, call_status,
                                      return_status, "openWriteFile");
  if (return_status == HighsStatus::kError) return return_status;
  // Report to user that solution is being written
  if (filename != "")
    highsLogUser(options_.log_options, HighsLogType::kInfo,
                 "Writing the solution to %s\n", filename.c_str());
  writeSolutionFile(file, options_, model_, basis_, solution_, info_,
                    model_status_, style);
  if (style == kSolutionStyleSparse)
    return returnFromWriteSolution(file, return_status);
  if (style == kSolutionStyleRaw) {
    fprintf(file, "\n# Basis\n");
    writeBasisFile(file, basis_);
  }
  if (options_.ranging == kHighsOnString) {
    if (model_.isMip() || model_.isQp()) {
      highsLogUser(options_.log_options, HighsLogType::kError,
                   "Cannot determine ranging information for MIP or QP\n");
      return_status = HighsStatus::kError;
      return returnFromWriteSolution(file, return_status);
    }
    return_status =
        interpretCallStatus(options_.log_options, this->getRangingInterface(),
                            return_status, "getRangingInterface");
    if (return_status == HighsStatus::kError)
      returnFromWriteSolution(file, return_status);
    fprintf(file, "\n# Ranging\n");
    writeRangingFile(file, model_.lp_, info_.objective_function_value, basis_,
                     solution_, ranging_, style);
  }
  return returnFromWriteSolution(file, return_status);
}

HighsStatus Highs::readSolution(const std::string& filename,
                                const HighsInt style) {
  return readSolutionFile(filename, options_, model_.lp_, basis_, solution_,
                          style);
}

HighsStatus Highs::assessPrimalSolution(bool& valid, bool& integral,
                                        bool& feasible) const {
  return assessLpPrimalSolution("", options_, model_.lp_, solution_, valid,
                                integral, feasible);
}

std::string Highs::presolveStatusToString(
    const HighsPresolveStatus presolve_status) const {
  switch (presolve_status) {
    case HighsPresolveStatus::kNotPresolved:
      return "Not presolved";
    case HighsPresolveStatus::kNotReduced:
      return "Not reduced";
    case HighsPresolveStatus::kInfeasible:
      return "Infeasible";
    case HighsPresolveStatus::kUnboundedOrInfeasible:
      return "Unbounded or infeasible";
    case HighsPresolveStatus::kReduced:
      return "Reduced";
    case HighsPresolveStatus::kReducedToEmpty:
      return "Reduced to empty";
    case HighsPresolveStatus::kTimeout:
      return "Timeout";
    case HighsPresolveStatus::kOutOfMemory:
      return "Memory allocation error";
    default:
      assert(1 == 0);
      return "Unrecognised presolve status";
  }
}

std::string Highs::modelStatusToString(
    const HighsModelStatus model_status) const {
  return utilModelStatusToString(model_status);
}

std::string Highs::solutionStatusToString(
    const HighsInt solution_status) const {
  return utilSolutionStatusToString(solution_status);
}

std::string Highs::basisStatusToString(
    const HighsBasisStatus basis_status) const {
  return utilBasisStatusToString(basis_status);
}

std::string Highs::basisValidityToString(const HighsInt basis_validity) const {
  return utilBasisValidityToString(basis_validity);
}

std::string Highs::presolveRuleTypeToString(
    const HighsInt presolve_rule) const {
  return utilPresolveRuleTypeToString(presolve_rule);
}

// Private methods
void Highs::deprecationMessage(const std::string& method_name,
                               const std::string& alt_method_name) const {
  if (alt_method_name.compare("None") == 0) {
    highsLogUser(options_.log_options, HighsLogType::kWarning,
                 "Method %s is deprecated: no alternative method\n",
                 method_name.c_str());
  } else {
    highsLogUser(options_.log_options, HighsLogType::kWarning,
                 "Method %s is deprecated: alternative method is %s\n",
                 method_name.c_str(), alt_method_name.c_str());
  }
}

HighsPresolveStatus Highs::runPresolve(const bool force_lp_presolve,
                                       const bool force_presolve) {
  presolve_.clear();
  // Exit if presolve is set to off (unless presolve is forced)
  if (options_.presolve == kHighsOffString && !force_presolve)
    return HighsPresolveStatus::kNotPresolved;

  if (model_.isEmpty()) {
    // Empty models shouldn't reach here, but this status would cause
    // no harm if one did
    assert(1 == 0);
    return HighsPresolveStatus::kNotReduced;
  }

  // Ensure that the LP is column-wise
  HighsLp& original_lp = model_.lp_;
  original_lp.ensureColwise();

  if (original_lp.num_col_ == 0 && original_lp.num_row_ == 0)
    return HighsPresolveStatus::kNullError;

  // Ensure that the RunHighsClock is running
  if (!timer_.runningRunHighsClock()) timer_.startRunHighsClock();
  double start_presolve = timer_.readRunHighsClock();

  // Set time limit.
  if (options_.time_limit > 0 && options_.time_limit < kHighsInf) {
    double left = options_.time_limit - start_presolve;
    if (left <= 0) {
      highsLogDev(options_.log_options, HighsLogType::kError,
                  "Time limit reached while reading in matrix\n");
      return HighsPresolveStatus::kTimeout;
    }

    highsLogDev(options_.log_options, HighsLogType::kVerbose,
                "Time limit set: reading matrix took %.2g, presolve "
                "time left: %.2g\n",
                start_presolve, left);
  }

  // Presolve.
  HighsPresolveStatus presolve_return_status =
      HighsPresolveStatus::kNotPresolved;
  if (model_.isMip() && !force_lp_presolve) {
    // Use presolve for MIP
    //
    // Presolved model is extracted now since it's part of solver,
    // which is lost on return
    HighsMipSolver solver(callback_, options_, original_lp, solution_);
    // Start the MIP solver's total clock so that timeout in presolve
    // can be identified
    solver.timer_.start(timer_.total_clock);
    // Only place that HighsMipSolver::runPresolve is called
    solver.runPresolve(options_.presolve_reduction_limit);
    presolve_return_status = solver.getPresolveStatus();
    // Assign values to data members of presolve_
    presolve_.data_.reduced_lp_ = solver.getPresolvedModel();
    presolve_.data_.postSolveStack = solver.getPostsolveStack();
    presolve_.presolve_status_ = presolve_return_status;
    //    presolve_.data_.presolve_log_ =
  } else {
    // Use presolve for LP
    presolve_.init(original_lp, timer_);
    presolve_.options_ = &options_;
    if (options_.time_limit > 0 && options_.time_limit < kHighsInf) {
      double current = timer_.readRunHighsClock();
      double time_init = current - start_presolve;
      double left = presolve_.options_->time_limit - time_init;
      if (left <= 0) {
        highsLogDev(options_.log_options, HighsLogType::kError,
                    "Time limit reached while copying matrix into presolve.\n");
        return HighsPresolveStatus::kTimeout;
      }
      highsLogDev(options_.log_options, HighsLogType::kVerbose,
                  "Time limit set: copying matrix took %.2g, presolve "
                  "time left: %.2g\n",
                  time_init, left);
    }

    presolve_return_status = presolve_.run();
  }

  highsLogDev(options_.log_options, HighsLogType::kVerbose,
              "presolve_.run() returns status: %s\n",
              presolveStatusToString(presolve_return_status).c_str());

  // Update reduction counts.
  assert(presolve_return_status == presolve_.presolve_status_);
  presolve_log_ = presolve_.getPresolveLog();
  switch (presolve_.presolve_status_) {
    case HighsPresolveStatus::kReduced: {
      HighsLp& reduced_lp = presolve_.getReducedProblem();
      presolve_.info_.n_cols_removed =
          original_lp.num_col_ - reduced_lp.num_col_;
      presolve_.info_.n_rows_removed =
          original_lp.num_row_ - reduced_lp.num_row_;
      presolve_.info_.n_nnz_removed = (HighsInt)original_lp.a_matrix_.numNz() -
                                      (HighsInt)reduced_lp.a_matrix_.numNz();
      // Clear any scaling information inherited by the reduced LP
      reduced_lp.clearScale();
      assert(lpDimensionsOk("RunPresolve: reduced_lp", reduced_lp,
                            options_.log_options));
      break;
    }
    case HighsPresolveStatus::kReducedToEmpty: {
      presolve_.info_.n_cols_removed = original_lp.num_col_;
      presolve_.info_.n_rows_removed = original_lp.num_row_;
      presolve_.info_.n_nnz_removed = (HighsInt)original_lp.a_matrix_.numNz();
      break;
    }
    default:
      break;
  }
  // Presolve creates integrality vector for an LP, so clear it
  if (!model_.isMip()) presolve_.data_.reduced_lp_.integrality_.clear();

  return presolve_return_status;
}

HighsPostsolveStatus Highs::runPostsolve() {
  // assert(presolve_.has_run_);
  const bool have_primal_solution =
      presolve_.data_.recovered_solution_.value_valid;
  // Need at least a primal solution
  if (!have_primal_solution)
    return HighsPostsolveStatus::kNoPrimalSolutionError;
  const bool have_dual_solution =
      presolve_.data_.recovered_solution_.dual_valid;
  presolve_.data_.postSolveStack.undo(options_,
                                      presolve_.data_.recovered_solution_,
                                      presolve_.data_.recovered_basis_);
  // Compute the row activities
  assert(model_.lp_.a_matrix_.isColwise());
  calculateRowValuesQuad(model_.lp_, presolve_.data_.recovered_solution_);

  if (have_dual_solution && model_.lp_.sense_ == ObjSense::kMaximize)
    presolve_.negateReducedLpColDuals();

  // Ensure that the postsolve status is used to set
  // presolve_.postsolve_status_, as well as being returned
  HighsPostsolveStatus postsolve_status =
      HighsPostsolveStatus::kSolutionRecovered;
  presolve_.postsolve_status_ = postsolve_status;
  return postsolve_status;
}

void Highs::clearPresolve() {
  model_presolve_status_ = HighsPresolveStatus::kNotPresolved;
  presolved_model_.clear();
  presolve_.clear();
}

void Highs::clearStandardFormLp() {
  standard_form_valid_ = false;
  standard_form_offset_ = 0;
  standard_form_cost_.clear();
  standard_form_rhs_.clear();
  standard_form_matrix_.clear();
}

void Highs::invalidateUserSolverData() {
  invalidateModelStatus();
  invalidateSolution();
  invalidateBasis();
  invalidateRanging();
  invalidateInfo();
  invalidateEkk();
  invalidateIis();
}

void Highs::invalidateModelStatusSolutionAndInfo() {
  invalidateModelStatus();
  invalidateSolution();
  invalidateRanging();
  invalidateInfo();
  invalidateIis();
}

void Highs::invalidateModelStatus() {
  model_status_ = HighsModelStatus::kNotset;
}

void Highs::invalidateSolution() {
  info_.primal_solution_status = kSolutionStatusNone;
  info_.dual_solution_status = kSolutionStatusNone;
  info_.num_primal_infeasibilities = kHighsIllegalInfeasibilityCount;
  info_.max_primal_infeasibility = kHighsIllegalInfeasibilityMeasure;
  info_.sum_primal_infeasibilities = kHighsIllegalInfeasibilityMeasure;
  info_.num_dual_infeasibilities = kHighsIllegalInfeasibilityCount;
  info_.max_dual_infeasibility = kHighsIllegalInfeasibilityMeasure;
  info_.sum_dual_infeasibilities = kHighsIllegalInfeasibilityMeasure;
  this->solution_.invalidate();
}

void Highs::invalidateBasis() {
  info_.basis_validity = kBasisValidityInvalid;
  this->basis_.invalidate();
}

void Highs::invalidateInfo() { info_.invalidate(); }

void Highs::invalidateRanging() { ranging_.invalidate(); }

void Highs::invalidateEkk() { ekk_instance_.invalidate(); }

void Highs::invalidateIis() { iis_.invalidate(); }

HighsStatus Highs::completeSolutionFromDiscreteAssignment() {
  // Determine whether the current solution of a MIP is feasible and,
  // if not, try to assign values to continuous variables and discrete
  // variables not at integer values to achieve a feasible
  // solution. Valuable in the case where users make a heuristic
  // (partial) assignment of discrete variables
  assert(model_.isMip() && solution_.value_valid);
  HighsLp& lp = model_.lp_;
  // Determine whether the solution contains undefined values, in
  // order to decide whether to check its feasibility
  const bool contains_undefined_values = solution_.hasUndefined();
  if (!contains_undefined_values) {
    bool valid, integral, feasible;
    // Determine whether this solution is integer feasible
    HighsStatus return_status = assessLpPrimalSolution(
        "", options_, lp, solution_, valid, integral, feasible);
    assert(return_status != HighsStatus::kError);
    assert(valid);
    // If the current solution is integer feasible, then it can be
    // used by MIP solver to get a primal bound
    if (feasible) return HighsStatus::kOk;
  }
  // Save the column bounds and integrality in preparation for fixing
  // the discrete variables when user-supplied values are
  // integer
  std::vector<double> save_col_lower = lp.col_lower_;
  std::vector<double> save_col_upper = lp.col_upper_;
  std::vector<HighsVarType> save_integrality = lp.integrality_;
  const bool have_integrality = (lp.integrality_.size() != 0);
  assert(have_integrality);
  // Count the number of fixed and unfixed discrete variables
  HighsInt num_fixed_discrete_variable = 0;
  HighsInt num_unfixed_discrete_variable = 0;
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    const double primal = solution_.col_value[iCol];
    // Default value is lower bound, unless primal is integer for a
    // discrete variable
    solution_.col_value[iCol] = lp.col_lower_[iCol];
    if (lp.integrality_[iCol] == HighsVarType::kContinuous) continue;
    // Fix discrete variable if its value is defined and integer
    if (primal == kHighsUndefined) {
      num_unfixed_discrete_variable++;
    } else {
      const double lower = lp.col_lower_[iCol];
      const double upper = lp.col_upper_[iCol];
      const HighsVarType type =
          have_integrality ? lp.integrality_[iCol] : HighsVarType::kContinuous;
      double col_infeasibility = 0;
      double integer_infeasibility = 0;
      assessColPrimalSolution(options_, primal, lower, upper, type,
                              col_infeasibility, integer_infeasibility);
      if (integer_infeasibility > options_.mip_feasibility_tolerance) {
        num_unfixed_discrete_variable++;
      } else {
        // Variable is integer feasible, so fix it at this value and
        // remove its integrality
        num_fixed_discrete_variable++;
        lp.col_lower_[iCol] = primal;
        lp.col_upper_[iCol] = primal;
        lp.integrality_[iCol] = HighsVarType::kContinuous;
      }
    }
  }
  assert(!solution_.hasUndefined());
  const HighsInt num_discrete_variable =
      num_unfixed_discrete_variable + num_fixed_discrete_variable;
  const HighsInt num_continuous_variable = lp.num_col_ - num_discrete_variable;
  assert(num_continuous_variable >= 0);
  bool call_run = true;
  const bool few_fixed_discrete_variables =
      10 * num_fixed_discrete_variable < num_discrete_variable;
  if (num_unfixed_discrete_variable == 0) {
    // Solution is integer valued
    if (num_continuous_variable == 0) {
      // There are no continuous variables, so no feasible solution can be
      // deduced
      highsLogUser(options_.log_options, HighsLogType::kInfo,
                   "User-supplied values of discrete variables cannot yield "
                   "feasible solution\n");
      call_run = false;
    } else {
      // Solve an LP, so clear all integrality
      lp.integrality_.clear();
      highsLogUser(
          options_.log_options, HighsLogType::kInfo,
          "Attempting to find feasible solution "
          "by solving LP for user-supplied values of discrete variables\n");
    }
  } else {
    // There are unfixed discrete variables
    if (few_fixed_discrete_variables) {
      // Too few discrete variables are fixed so warn, but still
      // attempt to complete a feasible solution
      highsLogUser(
          options_.log_options, HighsLogType::kWarning,
          "User-supplied values fix only %d / %d discrete variables, "
          "so attempt to complete a feasible solution may be expensive\n",
          int(num_fixed_discrete_variable), int(num_discrete_variable));
    } else {
      highsLogUser(options_.log_options, HighsLogType::kInfo,
                   "Attempting to find feasible solution "
                   "by solving MIP for user-supplied values of %d / %d "
                   "discrete variables\n",
                   int(num_fixed_discrete_variable),
                   int(num_discrete_variable));
    }
  }
  HighsStatus return_status = HighsStatus::kOk;
  // Clear the current solution since either the user solution has
  // been used to fix (a subset of) discrete variables - so a valid
  // solution will be obtained from run() if the local model is
  // feasible - or it's not worth using the user solution
  solution_.clear();
  if (call_run) {
    // Solve the model, using mip_max_start_nodes for
    // mip_max_nodes...
    const HighsInt mip_max_nodes = options_.mip_max_nodes;
    options_.mip_max_nodes = options_.mip_max_start_nodes;
    // Solve the model
    basis_.clear();
    return_status = this->run();
    // ... remembering to recover the original value of mip_max_nodes
    options_.mip_max_nodes = mip_max_nodes;
  }
  // Recover the column bounds and integrality
  lp.col_lower_ = save_col_lower;
  lp.col_upper_ = save_col_upper;
  lp.integrality_ = save_integrality;
  // Handle the error return
  if (return_status == HighsStatus::kError) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Highs::run() error trying to find feasible solution\n");
    return HighsStatus::kError;
  }
  return HighsStatus::kOk;
}

// The method below runs calls solveLp for the given LP
HighsStatus Highs::callSolveLp(HighsLp& lp, const string message) {
  HighsStatus return_status = HighsStatus::kOk;

  HighsLpSolverObject solver_object(lp, basis_, solution_, info_, ekk_instance_,
                                    callback_, options_, timer_);

  // Check that the model is column-wise
  assert(model_.lp_.a_matrix_.isColwise());

  // Solve the LP
  return_status = solveLp(solver_object, message);
  // Extract the model status
  model_status_ = solver_object.model_status_;
  if (model_status_ == HighsModelStatus::kOptimal)
    checkOptimality("LP", return_status);
  return return_status;
}

HighsStatus Highs::callSolveQp() {
  // Check that the model is column-wise
  HighsLp& lp = model_.lp_;
  HighsHessian& hessian = model_.hessian_;
  assert(model_.lp_.a_matrix_.isColwise());
  if (hessian.dim_ > lp.num_col_) {
    highsLogDev(
        options_.log_options, HighsLogType::kError,
        "Hessian dimension = %d is incompatible with matrix dimension = %d\n",
        int(hessian.dim_), int(lp.num_col_));
    model_status_ = HighsModelStatus::kModelError;
    solution_.value_valid = false;
    solution_.dual_valid = false;
    return HighsStatus::kError;
  }
  //
  // Run the QP solver
  Instance instance(lp.num_col_, lp.num_row_);

  instance.sense = HighsInt(lp.sense_);
  instance.num_con = lp.num_row_;
  instance.num_var = lp.num_col_;

  instance.A.mat.num_col = lp.num_col_;
  instance.A.mat.num_row = lp.num_row_;
  instance.A.mat.start = lp.a_matrix_.start_;
  instance.A.mat.index = lp.a_matrix_.index_;
  instance.A.mat.value = lp.a_matrix_.value_;
  instance.c.value = lp.col_cost_;
  instance.offset = lp.offset_;
  instance.con_lo = lp.row_lower_;
  instance.con_up = lp.row_upper_;
  instance.var_lo = lp.col_lower_;
  instance.var_up = lp.col_upper_;
  instance.Q.mat.num_col = lp.num_col_;
  instance.Q.mat.num_row = lp.num_col_;
  triangularToSquareHessian(hessian, instance.Q.mat.start, instance.Q.mat.index,
                            instance.Q.mat.value);

  for (HighsInt i = 0; i < (HighsInt)instance.c.value.size(); i++) {
    if (instance.c.value[i] != 0.0) {
      instance.c.index[instance.c.num_nz++] = i;
    }
  }

  if (lp.sense_ == ObjSense::kMaximize) {
    // Negate the vector and Hessian
    for (double& i : instance.c.value) {
      i *= -1.0;
    }
    for (double& i : instance.Q.mat.value) {
      i *= -1.0;
    }
  }

  Settings settings;
  Statistics stats;

  settings.reportingfequency = 100;

  // Setting qp_update_limit = 10 leads to error with lpHighs3
  const HighsInt qp_update_limit = 1000;  // 1000; // default
  if (qp_update_limit != settings.reinvertfrequency) {
    highsLogUser(options_.log_options, HighsLogType::kInfo,
                 "Changing QP reinversion frequency from %d to %d\n",
                 int(settings.reinvertfrequency), int(qp_update_limit));
    settings.reinvertfrequency = qp_update_limit;
  }

  settings.iteration_limit = options_.qp_iteration_limit;
  settings.nullspace_limit = options_.qp_nullspace_limit;

  // Define the QP model status logging function
  settings.qp_model_status_log.subscribe(
      [this](QpModelStatus& qp_model_status) {
        if (qp_model_status == QpModelStatus::kUndetermined ||
            qp_model_status == QpModelStatus::kLargeNullspace ||
            qp_model_status == QpModelStatus::kError ||
            qp_model_status == QpModelStatus::kNotset)
          highsLogUser(options_.log_options, HighsLogType::kInfo,
                       "QP solver model status: %s\n",
                       qpModelStatusToString(qp_model_status).c_str());
      });

  // Define the QP solver iteration logging function
  settings.iteration_log.subscribe([this](Statistics& stats) {
    int rep = stats.iteration.size() - 1;
    highsLogUser(options_.log_options, HighsLogType::kInfo,
                 "%11d  %15.8g           %6d %9.2fs\n",
                 int(stats.iteration[rep]), stats.objval[rep],
                 int(stats.nullspacedimension[rep]), stats.time[rep]);
  });

  // Define the QP nullspace limit logging function
  settings.nullspace_limit_log.subscribe([this](HighsInt& nullspace_limit) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "QP solver has exceeded nullspace limit of %d\n",
                 int(nullspace_limit));
  });

  settings.time_limit = options_.time_limit;
  settings.lambda_zero_threshold = options_.dual_feasibility_tolerance;

  switch (options_.simplex_primal_edge_weight_strategy) {
    case 0:
      settings.pricing = PricingStrategy::DantzigWolfe;
      break;
    case 1:
      settings.pricing = PricingStrategy::Devex;
      break;
    case 2:
      settings.pricing = PricingStrategy::SteepestEdge;
      break;
    default:
      settings.pricing = PricingStrategy::Devex;
  }

  // print header for QP solver output
  highsLogUser(options_.log_options, HighsLogType::kInfo,
               "  Iteration        Objective     NullspaceDim\n");

  QpAsmStatus status = solveqp(instance, settings, stats, model_status_, basis_,
                               solution_, timer_);
  // QP solver can fail, so should return something other than QpAsmStatus::kOk
  if (status == QpAsmStatus::kError) return HighsStatus::kError;

  assert(status == QpAsmStatus::kOk || status == QpAsmStatus::kWarning);
  HighsStatus return_status = status == QpAsmStatus::kWarning
                                  ? HighsStatus::kWarning
                                  : HighsStatus::kOk;

  // Get the objective and any KKT failures
  info_.objective_function_value = model_.objectiveValue(solution_.col_value);
  getKktFailures(options_, model_, solution_, basis_, info_);
  // Set the QP-specific values of info_
  info_.simplex_iteration_count += stats.phase1_iterations;
  info_.qp_iteration_count += stats.num_iterations;
  info_.valid = true;
  if (model_status_ == HighsModelStatus::kOptimal)
    checkOptimality("QP", return_status);
  return return_status;
}

HighsStatus Highs::callSolveMip() {
  // Record whether there is a valid primal solution on entry
  const bool user_solution = solution_.value_valid;
  std::vector<double> user_solution_col_value;
  std::vector<double> user_solution_row_value;
  if (user_solution) {
    // Save the col and row values
    user_solution_col_value = std::move(solution_.col_value);
    user_solution_row_value = std::move(solution_.row_value);
  }
  // Ensure that any solver data for users in Highs class members are
  // cleared
  invalidateUserSolverData();
  if (user_solution) {
    // Recover the col and row values
    solution_.col_value = std::move(user_solution_col_value);
    solution_.row_value = std::move(user_solution_row_value);
    solution_.value_valid = true;
  }
  // Run the MIP solver
  HighsInt log_dev_level = options_.log_dev_level;
  //  options_.log_dev_level = kHighsLogDevLevelInfo;
  // Check that the model isn't row-wise
  assert(model_.lp_.a_matrix_.format_ != MatrixFormat::kRowwise);
  const bool has_semi_variables = model_.lp_.hasSemiVariables();
  HighsLp use_lp;
  if (has_semi_variables) {
    // Replace any semi-variables by a continuous/integer variable and
    // a (temporary) binary. Any initial solution must accommodate this.
    use_lp = withoutSemiVariables(model_.lp_, solution_,
                                  options_.primal_feasibility_tolerance);
  }
  HighsLp& lp = has_semi_variables ? use_lp : model_.lp_;
  HighsMipSolver solver(callback_, options_, lp, solution_);
  solver.run();
  options_.log_dev_level = log_dev_level;
  // Set the return_status, model status and, for completeness, scaled
  // model status
  HighsStatus return_status =
      highsStatusFromHighsModelStatus(solver.modelstatus_);
  model_status_ = solver.modelstatus_;
  // Extract the solution
  if (solver.solution_objective_ != kHighsInf) {
    // There is a primal solution
    HighsInt solver_solution_size = solver.solution_.size();
    assert(solver_solution_size >= lp.num_col_);
    // If the original model has semi-variables, its solution is
    // (still) given by the first model_.lp_.num_col_ entries of the
    // solution from the MIP solver
    solution_.col_value.resize(model_.lp_.num_col_);
    solution_.col_value = solver.solution_;
    saved_objective_and_solution_ = solver.saved_objective_and_solution_;
    model_.lp_.a_matrix_.productQuad(solution_.row_value, solution_.col_value);
    solution_.value_valid = true;
  } else {
    // There is no primal solution: should be so by default
    assert(!solution_.value_valid);
  }
  // Check that no modified upper bounds for semi-variables are active
  if (solution_.value_valid &&
      activeModifiedUpperBounds(options_, model_.lp_, solution_.col_value)) {
    solution_.value_valid = false;
    model_status_ = HighsModelStatus::kSolveError;
    return_status = HighsStatus::kError;
  }
  // There is no dual solution: should be so by default
  assert(!solution_.dual_valid);
  // There is no basis: should be so by default
  assert(!basis_.valid);
  // Get the objective and any KKT failures
  info_.objective_function_value = solver.solution_objective_;
  const bool use_mip_feasibility_tolerance = true;
  double primal_feasibility_tolerance = options_.primal_feasibility_tolerance;
  if (use_mip_feasibility_tolerance) {
    options_.primal_feasibility_tolerance = options_.mip_feasibility_tolerance;
  }
  // NB getKktFailures sets the primal and dual solution status
  getKktFailures(options_, model_, solution_, basis_, info_);
  // Set the MIP-specific values of info_
  info_.mip_node_count = solver.node_count_;
  info_.mip_dual_bound = solver.dual_bound_;
  info_.mip_gap = solver.gap_;
  info_.primal_dual_integral = solver.primal_dual_integral_;
  // Get the number of LP iterations, avoiding overflow if the int64_t
  // value is too large
  int64_t mip_total_lp_iterations = solver.total_lp_iterations_;
  info_.simplex_iteration_count = mip_total_lp_iterations > kHighsIInf
                                      ? -1
                                      : HighsInt(mip_total_lp_iterations);
  info_.valid = true;
  if (model_status_ == HighsModelStatus::kOptimal)
    checkOptimality("MIP", return_status);
  if (use_mip_feasibility_tolerance) {
    // Overwrite max infeasibility to include integrality if there is a solution
    if (solver.solution_objective_ != kHighsInf) {
      const double mip_max_bound_violation =
          std::max(solver.row_violation_, solver.bound_violation_);
      const double delta_max_bound_violation =
          std::abs(mip_max_bound_violation - info_.max_primal_infeasibility);
      // Possibly report a mis-match between the max bound violation
      // returned by the MIP solver, and the value obtained from the
      // solution
      if (delta_max_bound_violation > 1e-12)
        highsLogDev(options_.log_options, HighsLogType::kWarning,
                    "Inconsistent max bound violation: MIP solver (%10.4g); LP "
                    "(%10.4g); Difference of %10.4g\n",
                    mip_max_bound_violation, info_.max_primal_infeasibility,
                    delta_max_bound_violation);
      info_.max_integrality_violation = solver.integrality_violation_;
      if (info_.max_integrality_violation >
          options_.mip_feasibility_tolerance) {
        info_.primal_solution_status = kSolutionStatusInfeasible;
        assert(model_status_ == HighsModelStatus::kInfeasible);
      }
    }
    // Recover the primal feasibility tolerance
    options_.primal_feasibility_tolerance = primal_feasibility_tolerance;
  }
  return return_status;
}

// Only called from Highs::postsolve
HighsStatus Highs::callRunPostsolve(const HighsSolution& solution,
                                    const HighsBasis& basis) {
  HighsStatus return_status = HighsStatus::kOk;
  HighsStatus call_status;
  const HighsLp& presolved_lp = presolve_.getReducedProblem();

  // Must at least have a primal column solution of the right size
  if (HighsInt(solution.col_value.size()) != presolved_lp.num_col_) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "Primal solution provided to postsolve is incorrect size\n");
    return HighsStatus::kError;
  }
  // Check any basis that is supplied
  const bool basis_supplied =
      basis.col_status.size() > 0 || basis.row_status.size() > 0 || basis.valid;
  if (basis_supplied) {
    if (!isBasisConsistent(presolved_lp, basis)) {
      highsLogUser(
          options_.log_options, HighsLogType::kError,
          "Basis provided to postsolve is incorrect size or inconsistent\n");
      return HighsStatus::kError;
    }
  }
  // Copy in the solution provided
  presolve_.data_.recovered_solution_ = solution;
  // Ignore any row values
  presolve_.data_.recovered_solution_.row_value.assign(presolved_lp.num_row_,
                                                       0);
  presolve_.data_.recovered_solution_.value_valid = true;

  if (this->model_.isMip() && !basis.valid) {
    // Postsolving a MIP without a valid basis - which, if valid,
    // would imply that the relaxation had been solved, a case handled
    // below
    //
    // Ignore any dual values
    presolve_.data_.recovered_solution_.dual_valid = false;
    presolve_.data_.recovered_solution_.col_dual.clear();
    presolve_.data_.recovered_solution_.row_dual.clear();
    // Ignore any basis
    presolve_.data_.recovered_basis_.valid = false;

    HighsPostsolveStatus postsolve_status = runPostsolve();

    if (postsolve_status == HighsPostsolveStatus::kSolutionRecovered) {
      this->solution_ = presolve_.data_.recovered_solution_;
      this->model_status_ = HighsModelStatus::kUnknown;
      this->info_.invalidate();
      HighsLp& lp = this->model_.lp_;
      this->info_.objective_function_value =
          computeObjectiveValue(lp, this->solution_);
      getKktFailures(this->options_, this->model_, this->solution_,
                     this->basis_, this->info_);
      double& max_integrality_violation = this->info_.max_integrality_violation;
      max_integrality_violation = 0;
      for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
        if (lp.integrality_[iCol] == HighsVarType::kInteger) {
          max_integrality_violation =
              std::max(fractionality(this->solution_.col_value[iCol]),
                       max_integrality_violation);
        }
      }
      highsLogUser(
          options_.log_options, HighsLogType::kWarning,
          "Postsolve performed for MIP, but model status cannot be known\n");
    } else {
      highsLogUser(options_.log_options, HighsLogType::kError,
                   "Postsolve return status is %d\n", (int)postsolve_status);
      setHighsModelStatusAndClearSolutionAndBasis(
          HighsModelStatus::kPostsolveError);
    }
  } else {
    // Postsolving an LP, or a MIP after solving the relaxation
    // (identified by passing a valid basis).
    //
    // If there are dual values, make sure that both vectors are the
    // right size
    const bool dual_supplied =
        presolve_.data_.recovered_solution_.col_dual.size() > 0 ||
        presolve_.data_.recovered_solution_.row_dual.size() > 0 ||
        presolve_.data_.recovered_solution_.dual_valid;
    if (dual_supplied) {
      if (!isDualSolutionRightSize(presolved_lp,
                                   presolve_.data_.recovered_solution_)) {
        highsLogUser(options_.log_options, HighsLogType::kError,
                     "Dual solution provided to postsolve is incorrect size\n");
        return HighsStatus::kError;
      }
      presolve_.data_.recovered_solution_.dual_valid = true;
    } else {
      presolve_.data_.recovered_solution_.dual_valid = false;
    }
    // Copy in the basis provided. It's already been checked for
    // consistency, so the basis is valid iff it was supplied
    presolve_.data_.recovered_basis_ = basis;
    presolve_.data_.recovered_basis_.valid = basis_supplied;

    HighsPostsolveStatus postsolve_status = runPostsolve();

    if (postsolve_status == HighsPostsolveStatus::kSolutionRecovered) {
      highsLogDev(options_.log_options, HighsLogType::kVerbose,
                  "Postsolve finished\n");
      // Set solution and its status
      solution_.clear();
      solution_ = presolve_.data_.recovered_solution_;
      assert(solution_.value_valid);
      if (!solution_.dual_valid) {
        solution_.col_dual.assign(model_.lp_.num_col_, 0);
        solution_.row_dual.assign(model_.lp_.num_row_, 0);
      }
      basis_ = presolve_.data_.recovered_basis_;
      // Validity of the solution and basis should be inherited
      //
      // solution_.value_valid = true;
      // solution_.dual_valid = true;
      //
      // Set basis and its status
      //
      // basis_.valid = true;
      // basis_.col_status = presolve_.data_.recovered_basis_.col_status;
      // basis_.row_status = presolve_.data_.recovered_basis_.row_status;
      basis_.debug_origin_name += ": after postsolve";
      if (basis_.valid) {
        // Save the options to allow the best simplex strategy to be
        // used
        HighsOptions save_options = options_;
        options_.simplex_strategy = kSimplexStrategyChoose;
        // Ensure that the parallel solver isn't used
        options_.simplex_min_concurrency = 1;
        options_.simplex_max_concurrency = 1;
        // Use any pivot threshold resulting from solving the presolved LP
        // if (factor_pivot_threshold > 0)
        //    options_.factor_pivot_threshold = factor_pivot_threshold;
        // The basis returned from postsolve is just basic/nonbasic
        // and EKK expects a refined basis, so set it up now
        HighsLp& incumbent_lp = model_.lp_;
        refineBasis(incumbent_lp, solution_, basis_);
        // Scrap the EKK data from solving the presolved LP
        ekk_instance_.invalidate();
        ekk_instance_.lp_name_ = "Postsolve LP";
        // Set up the timing record so that adding the corresponding
        // values after callSolveLp gives difference
        timer_.start(timer_.solve_clock);
        call_status = callSolveLp(
            incumbent_lp,
            "Solving the original LP from the solution after postsolve");
        // Determine the timing record
        timer_.stop(timer_.solve_clock);
        return_status = interpretCallStatus(options_.log_options, call_status,
                                            return_status, "callSolveLp");
        // Recover the options
        options_ = save_options;
        if (return_status == HighsStatus::kError) {
          // Set undo_mods = false, since passing models requiring
          // modification to Highs::presolve is illegal
          const bool undo_mods = false;
          return returnFromRun(return_status, undo_mods);
        }
      } else {
        basis_.clear();
        info_.objective_function_value =
            model_.lp_.objectiveValue(solution_.col_value);
        getLpKktFailures(options_, model_.lp_, solution_, basis_, info_);
        if (info_.num_primal_infeasibilities == 0 &&
            info_.num_dual_infeasibilities == 0) {
          model_status_ = HighsModelStatus::kOptimal;
        } else {
          model_status_ = HighsModelStatus::kUnknown;
        }
        highsLogUser(
            options_.log_options, HighsLogType::kInfo,
            "Pure postsolve yields primal %ssolution, but no basis: model "
            "status is %s\n",
            solution_.dual_valid ? "and dual " : "",
            modelStatusToString(model_status_).c_str());
      }
    } else {
      highsLogUser(options_.log_options, HighsLogType::kError,
                   "Postsolve return status is %d\n", (int)postsolve_status);
      setHighsModelStatusAndClearSolutionAndBasis(
          HighsModelStatus::kPostsolveError);
      // Set undo_mods = false, since passing models requiring
      // modification to Highs::presolve is illegal
      const bool undo_mods = false;
      return returnFromRun(HighsStatus::kError, undo_mods);
    }
  }
  call_status = highsStatusFromHighsModelStatus(model_status_);
  return_status =
      interpretCallStatus(options_.log_options, call_status, return_status,
                          "highsStatusFromHighsModelStatus");
  return return_status;
}

// End of public methods
void Highs::logHeader() {
  if (written_log_header) return;
  highsLogHeader(options_.log_options, options_.log_githash);
  written_log_header = true;
  return;
}

void Highs::reportModel(const HighsModel& model) {
  reportLp(options_.log_options, model.lp_, HighsLogType::kVerbose);
  if (model.hessian_.dim_) {
    const HighsInt dim = model.hessian_.dim_;
    reportHessian(options_.log_options, dim, model.hessian_.start_[dim],
                  model.hessian_.start_.data(), model.hessian_.index_.data(),
                  model.hessian_.value_.data());
  }
}

// Actions to take if there is a new Highs basis
void Highs::newHighsBasis() {
  // Clear any simplex basis
  ekk_instance_.updateStatus(LpAction::kNewBasis);
}

// Ensure that the HiGHS solution and basis have the same size as the
// model, and that the HiGHS basis is kept up-to-date with any solved
// basis
void Highs::forceHighsSolutionBasisSize() {
  // Ensure that the HiGHS solution vectors are the right size
  solution_.col_value.resize(model_.lp_.num_col_);
  solution_.row_value.resize(model_.lp_.num_row_);
  solution_.col_dual.resize(model_.lp_.num_col_);
  solution_.row_dual.resize(model_.lp_.num_row_);
  // Ensure that the HiGHS basis vectors are the right size,
  // invalidating the basis if they aren't
  if (basis_.col_status.size() != static_cast<size_t>(model_.lp_.num_col_)) {
    basis_.col_status.resize(model_.lp_.num_col_);
    basis_.valid = false;
  }
  if (basis_.row_status.size() != static_cast<size_t>(model_.lp_.num_row_)) {
    basis_.row_status.resize(model_.lp_.num_row_);
    basis_.valid = false;
  }
}

void Highs::setHighsModelStatusAndClearSolutionAndBasis(
    const HighsModelStatus model_status) {
  model_status_ = model_status;
  invalidateSolution();
  invalidateBasis();
  info_.valid = true;
}

void Highs::setBasisValidity() {
  if (basis_.valid) {
    assert(info_.basis_validity == kBasisValidityValid);
    info_.basis_validity = kBasisValidityValid;
  } else {
    assert(info_.basis_validity == kBasisValidityInvalid);
    info_.basis_validity = kBasisValidityInvalid;
  }
  info_.valid = true;
}

HighsStatus Highs::openWriteFile(const string filename,
                                 const string method_name, FILE*& file,
                                 HighsFileType& file_type) const {
  file_type = HighsFileType::kFull;
  if (filename == "") {
    // Empty file name: use stdout
    file = stdout;
  } else {
    file = fopen(filename.c_str(), "w");
    if (file == 0) {
      highsLogUser(options_.log_options, HighsLogType::kError,
                   "Cannot open writable file \"%s\" in %s\n", filename.c_str(),
                   method_name.c_str());
      return HighsStatus::kError;
    }
    const char* dot = strrchr(filename.c_str(), '.');
    if (dot && dot != filename) {
      if (strcmp(dot + 1, "mps") == 0) {
        file_type = HighsFileType::kMps;
      } else if (strcmp(dot + 1, "lp") == 0) {
        file_type = HighsFileType::kLp;
      } else if (strcmp(dot + 1, "md") == 0) {
        file_type = HighsFileType::kMd;
      }
    }
  }
  return HighsStatus::kOk;
}

// Always called when returning from Highs::writeSolution
HighsStatus Highs::returnFromWriteSolution(FILE* file,
                                           const HighsStatus return_status) {
  if (file != stdout) fclose(file);
  return return_status;
}

// Applies checks before returning from run()
HighsStatus Highs::returnFromRun(const HighsStatus run_return_status,
                                 const bool undo_mods) {
  assert(!called_return_from_run);
  HighsStatus return_status = highsStatusFromHighsModelStatus(model_status_);
  if (return_status != run_return_status) {
    printf(
        "Highs::returnFromRun: return_status = %d != %d = run_return_status "
        "For model_status_ = %s\n",
        int(return_status), int(run_return_status),
        modelStatusToString(model_status_).c_str());
  }
  assert(return_status == run_return_status);
  //  return_status = run_return_status;
  switch (model_status_) {
      // First consider the error returns
    case HighsModelStatus::kNotset:
    case HighsModelStatus::kLoadError:
    case HighsModelStatus::kModelError:
    case HighsModelStatus::kPresolveError:
    case HighsModelStatus::kSolveError:
    case HighsModelStatus::kPostsolveError:
    case HighsModelStatus::kMemoryLimit:
      // Don't clear the model status!
      //      invalidateUserSolverData();
      invalidateInfo();
      invalidateSolution();
      invalidateBasis();
      assert(return_status == HighsStatus::kError);
      break;

      // Then consider the OK returns
    case HighsModelStatus::kModelEmpty:
      invalidateInfo();
      invalidateSolution();
      invalidateBasis();
      assert(return_status == HighsStatus::kOk);
      break;

    case HighsModelStatus::kOptimal:
      // The following is an aspiration
      //
      // assert(info_.primal_solution_status == kSolutionStatusFeasible);
      //
      // assert(info_.dual_solution_status == kSolutionStatusFeasible);
      assert(model_status_ == HighsModelStatus::kNotset ||
             model_status_ == HighsModelStatus::kOptimal);
      assert(return_status == HighsStatus::kOk);
      break;

    case HighsModelStatus::kInfeasible:
    case HighsModelStatus::kUnbounded:
    case HighsModelStatus::kObjectiveBound:
    case HighsModelStatus::kObjectiveTarget:
      // For kInfeasible, will not have a basis, if infeasibility was
      // detected in presolve or by IPX without crossover
      assert(return_status == HighsStatus::kOk);
      break;

    case HighsModelStatus::kUnboundedOrInfeasible:
      if (options_.allow_unbounded_or_infeasible ||
          (options_.solver == kIpmString &&
           options_.run_crossover == kHighsOnString) ||
          (options_.solver == kPdlpString) || model_.isMip()) {
        assert(return_status == HighsStatus::kOk);
      } else {
        // This model status is not permitted unless IPM is run without
        // crossover, or if PDLP is used
        highsLogUser(
            options_.log_options, HighsLogType::kError,
            "returnFromHighs: HighsModelStatus::kUnboundedOrInfeasible is not "
            "permitted\n");
        assert(options_.allow_unbounded_or_infeasible);
        return_status = HighsStatus::kError;
      }
      break;

      // Finally consider the warning returns
    case HighsModelStatus::kTimeLimit:
    case HighsModelStatus::kIterationLimit:
    case HighsModelStatus::kSolutionLimit:
    case HighsModelStatus::kInterrupt:
    case HighsModelStatus::kUnknown:
      assert(return_status == HighsStatus::kWarning);
      break;
    default:
      // All cases should have been considered so assert on reaching here
      assert(1 == 0);
  }
  // Now to check what's available with each model status
  //
  const bool have_info = info_.valid;
  const bool have_primal_solution = solution_.value_valid;
  const bool have_dual_solution = solution_.dual_valid;
  // Can't have a dual solution without a primal solution
  assert(have_primal_solution || !have_dual_solution);
  //  const bool have_solution = have_primal_solution && have_dual_solution;
  const bool have_basis = basis_.valid;
  switch (model_status_) {
    case HighsModelStatus::kNotset:
    case HighsModelStatus::kLoadError:
    case HighsModelStatus::kModelError:
    case HighsModelStatus::kPresolveError:
    case HighsModelStatus::kSolveError:
    case HighsModelStatus::kPostsolveError:
    case HighsModelStatus::kModelEmpty:
    case HighsModelStatus::kMemoryLimit:
      // No info, primal solution or basis
      assert(have_info == false);
      assert(have_primal_solution == false);
      assert(have_basis == false);
      break;
    case HighsModelStatus::kOptimal:
    case HighsModelStatus::kInfeasible:
    case HighsModelStatus::kUnbounded:
    case HighsModelStatus::kObjectiveBound:
    case HighsModelStatus::kObjectiveTarget:
    case HighsModelStatus::kUnboundedOrInfeasible:
    case HighsModelStatus::kTimeLimit:
    case HighsModelStatus::kIterationLimit:
    case HighsModelStatus::kSolutionLimit:
    case HighsModelStatus::kInterrupt:
    case HighsModelStatus::kUnknown:
      // Have info and primal solution (unless infeasible). No primal solution
      // in some other case, too!
      assert(have_info == true);
      break;
    default:
      // All cases should have been considered so assert on reaching here
      assert(1 == 0);
  }
  if (have_primal_solution) {
    if (debugPrimalSolutionRightSize(options_, model_.lp_, solution_) ==
        HighsDebugStatus::kLogicalError)
      return_status = HighsStatus::kError;
  }
  if (have_dual_solution) {
    if (debugDualSolutionRightSize(options_, model_.lp_, solution_) ==
        HighsDebugStatus::kLogicalError)
      return_status = HighsStatus::kError;
  }
  if (have_basis) {
    if (debugBasisRightSize(options_, model_.lp_, basis_) ==
        HighsDebugStatus::kLogicalError)
      return_status = HighsStatus::kError;
  }
  if (have_primal_solution) {
    // Debug the Highs solution - needs primal values at least
    if (debugHighsSolution("Return from run()", options_, model_, solution_,
                           basis_, model_status_,
                           info_) == HighsDebugStatus::kLogicalError)
      return_status = HighsStatus::kError;
  }
  if (debugInfo(options_, model_.lp_, basis_, solution_, info_,
                model_status_) == HighsDebugStatus::kLogicalError)
    return_status = HighsStatus::kError;

  // Record that returnFromRun() has been called
  called_return_from_run = true;

  if (undo_mods) {
    // Restore any infinite costs
    this->restoreInfCost(return_status);

    // Unapply any modifications that have not yet been unapplied
    this->model_.lp_.unapplyMods();
    //    undo_mods = false;
  }

  // Unless solved as a MIP, report on the solution
  const bool solved_as_mip = !options_.solver.compare(kHighsChooseString) &&
                             model_.isMip() && !options_.solve_relaxation;
  if (!solved_as_mip) reportSolvedLpQpStats();
  return returnFromHighs(return_status);
}

HighsStatus Highs::returnFromHighs(HighsStatus highs_return_status) {
  // Applies checks before returning from HiGHS
  HighsStatus return_status = highs_return_status;

  forceHighsSolutionBasisSize();

  const bool consistent =
      debugHighsBasisConsistent(options_, model_.lp_, basis_) !=
      HighsDebugStatus::kLogicalError;
  if (!consistent) {
    highsLogUser(
        options_.log_options, HighsLogType::kError,
        "returnFromHighs: Supposed to be a HiGHS basis, but not consistent\n");
    assert(consistent);
    return_status = HighsStatus::kError;
  }
  // Check that any retained Ekk data - basis and NLA - are OK
  bool retained_ekk_data_ok = ekk_instance_.debugRetainedDataOk(model_.lp_) !=
                              HighsDebugStatus::kLogicalError;
  if (!retained_ekk_data_ok) {
    highsLogUser(options_.log_options, HighsLogType::kError,
                 "returnFromHighs: Retained Ekk data not OK\n");
    assert(retained_ekk_data_ok);
    return_status = HighsStatus::kError;
  }
  // Check that returnFromRun() has been called
  if (!called_return_from_run) {
    highsLogDev(
        options_.log_options, HighsLogType::kError,
        "Highs::returnFromHighs() called with called_return_from_run false\n");
    assert(called_return_from_run);
  }
  // Stop the HiGHS run clock if it is running
  if (timer_.runningRunHighsClock()) timer_.stopRunHighsClock();
  const bool dimensions_ok =
      lpDimensionsOk("returnFromHighs", model_.lp_, options_.log_options);
  if (!dimensions_ok) {
    highsLogDev(options_.log_options, HighsLogType::kError,
                "LP Dimension error in returnFromHighs()\n");
    return_status = HighsStatus::kError;
  }
  assert(dimensions_ok);
  if (ekk_instance_.status_.has_nla) {
    if (!ekk_instance_.lpFactorRowCompatible(model_.lp_.num_row_)) {
      highsLogDev(options_.log_options, HighsLogType::kWarning,
                  "Highs::returnFromHighs(): LP and HFactor have inconsistent "
                  "numbers of rows\n");
      // Clear Ekk entirely
      ekk_instance_.clear();
    }
  }
  return return_status;
}

void Highs::reportSolvedLpQpStats() {
  if (!options_.output_flag) return;
  HighsLogOptions& log_options = options_.log_options;
  if (this->model_.lp_.model_name_.length())
    highsLogUser(log_options, HighsLogType::kInfo, "Model name          : %s\n",
                 model_.lp_.model_name_.c_str());
  highsLogUser(log_options, HighsLogType::kInfo, "Model status        : %s\n",
               modelStatusToString(model_status_).c_str());
  if (info_.valid) {
    if (info_.simplex_iteration_count)
      highsLogUser(log_options, HighsLogType::kInfo,
                   "Simplex   iterations: %" HIGHSINT_FORMAT "\n",
                   info_.simplex_iteration_count);
    if (info_.ipm_iteration_count)
      highsLogUser(log_options, HighsLogType::kInfo,
                   "IPM       iterations: %" HIGHSINT_FORMAT "\n",
                   info_.ipm_iteration_count);
    if (info_.crossover_iteration_count)
      highsLogUser(log_options, HighsLogType::kInfo,
                   "Crossover iterations: %" HIGHSINT_FORMAT "\n",
                   info_.crossover_iteration_count);
    if (info_.pdlp_iteration_count)
      highsLogUser(log_options, HighsLogType::kInfo,
                   "PDLP      iterations: %" HIGHSINT_FORMAT "\n",
                   info_.pdlp_iteration_count);
    if (info_.qp_iteration_count)
      highsLogUser(log_options, HighsLogType::kInfo,
                   "QP ASM    iterations: %" HIGHSINT_FORMAT "\n",
                   info_.qp_iteration_count);
    highsLogUser(log_options, HighsLogType::kInfo,
                 "Objective value     : %17.10e\n",
                 info_.objective_function_value);
  }
  if (solution_.dual_valid && !this->model_.isQp()) {
    double dual_objective_value;
    HighsStatus status = this->getDualObjectiveValue(dual_objective_value);
    assert(status == HighsStatus::kOk);
    const double relative_primal_dual_gap =
        std::fabs(info_.objective_function_value - dual_objective_value) /
        std::max(1.0, std::fabs(info_.objective_function_value));
    highsLogUser(log_options, HighsLogType::kInfo,
                 "Relative P-D gap    : %17.10e\n", relative_primal_dual_gap);
  }
  double run_time = timer_.readRunHighsClock();
  highsLogUser(log_options, HighsLogType::kInfo,
               "HiGHS run time      : %13.2f\n", run_time);
}

HighsStatus Highs::crossover(const HighsSolution& user_solution) {
  HighsStatus return_status = HighsStatus::kOk;
  HighsLogOptions& log_options = options_.log_options;
  HighsLp& lp = model_.lp_;
  if (lp.isMip()) {
    highsLogUser(log_options, HighsLogType::kError,
                 "Cannot apply crossover to solve MIP\n");
    return_status = HighsStatus::kError;
  } else if (model_.isQp()) {
    highsLogUser(log_options, HighsLogType::kError,
                 "Cannot apply crossover to solve QP\n");
    return_status = HighsStatus::kError;
  } else {
    clearSolver();
    solution_ = user_solution;
    // Use IPX crossover to try to form a basic solution
    return_status = callCrossover(options_, model_.lp_, basis_, solution_,
                                  model_status_, info_, callback_);
    if (return_status == HighsStatus::kError) return return_status;
    // Get the objective and any KKT failures
    info_.objective_function_value =
        model_.lp_.objectiveValue(solution_.col_value);
    getLpKktFailures(options_, model_.lp_, solution_, basis_, info_);
  }
  return returnFromHighs(return_status);
}

HighsStatus Highs::openLogFile(const std::string& log_file) {
  highsOpenLogFile(options_.log_options, options_.records, log_file);
  return HighsStatus::kOk;
}

void Highs::resetGlobalScheduler(bool blocking) {
  HighsTaskExecutor::shutdown(blocking);
}
