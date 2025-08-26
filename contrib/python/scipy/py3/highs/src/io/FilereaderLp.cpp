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
/**@file io/FilereaderLp.cpp
 * @brief
 */

#include "io/FilereaderLp.h"

#include <cstdarg>
#include <cstdio>
#include <exception>
#include <map>

#include "../extern/filereaderlp/reader.hpp"
#include "lp_data/HighsLpUtils.h"

const bool original_double_format = false;
const bool allow_model_names = true;

FilereaderRetcode FilereaderLp::readModelFromFile(const HighsOptions& options,
                                                  const std::string filename,
                                                  HighsModel& model) {
  HighsLp& lp = model.lp_;
  HighsHessian& hessian = model.hessian_;
  try {
    Model m = readinstance(filename);

    if (!m.soss.empty()) {
      highsLogUser(options.log_options, HighsLogType::kError,
                   "SOS not supported by HiGHS\n");
      return FilereaderRetcode::kParserError;
    }

    // build variable index and gather variable information
    std::map<std::string, unsigned int> varindex;

    lp.num_col_ = m.variables.size();
    lp.num_row_ = m.constraints.size();
    lp.row_names_.resize(m.constraints.size());
    lp.integrality_.assign(lp.num_col_, HighsVarType::kContinuous);
    HighsInt num_continuous = 0;
    for (size_t i = 0; i < m.variables.size(); i++) {
      varindex[m.variables[i]->name] = i;
      lp.col_lower_.push_back(m.variables[i]->lowerbound);
      lp.col_upper_.push_back(m.variables[i]->upperbound);
      lp.col_names_.push_back(m.variables[i]->name);
      if (m.variables[i]->type == VariableType::BINARY ||
          m.variables[i]->type == VariableType::GENERAL) {
        lp.integrality_[i] = HighsVarType::kInteger;
      } else if (m.variables[i]->type == VariableType::SEMICONTINUOUS) {
        lp.integrality_[i] = HighsVarType::kSemiContinuous;
      } else if (m.variables[i]->type == VariableType::SEMIINTEGER) {
        lp.integrality_[i] = HighsVarType::kSemiInteger;
      } else {
        lp.integrality_[i] = HighsVarType::kContinuous;
        num_continuous++;
      }
    }
    // Clear lp.integrality_ if problem is pure LP
    if (static_cast<size_t>(num_continuous) == m.variables.size())
      lp.integrality_.clear();
    // get objective
    lp.objective_name_ = m.objective->name;
    // ToDo: Fix m.objective->offset and then use it here
    //
    lp.offset_ = m.objective->offset;
    lp.col_cost_.resize(lp.num_col_, 0.0);
    for (size_t i = 0; i < m.objective->linterms.size(); i++) {
      std::shared_ptr<LinTerm> lt = m.objective->linterms[i];
      lp.col_cost_[varindex[lt->var->name]] = lt->coef;
    }

    std::map<std::shared_ptr<Variable>, std::vector<std::shared_ptr<Variable>>>
        mat;
    std::map<std::shared_ptr<Variable>, std::vector<double>> mat2;
    for (std::shared_ptr<QuadTerm> qt : m.objective->quadterms) {
      if (qt->var1 != qt->var2) {
        mat[qt->var1].push_back(qt->var2);
        mat2[qt->var1].push_back(qt->coef / 2);
        mat[qt->var2].push_back(qt->var1);
        mat2[qt->var2].push_back(qt->coef / 2);
      } else {
        mat[qt->var1].push_back(qt->var2);
        mat2[qt->var1].push_back(qt->coef);
      }
    }

    // Determine whether there is a Hessian to set up by counting its
    // nonzero entries
    unsigned int qnnz = 0;
    for (std::shared_ptr<Variable> var : m.variables)
      for (size_t i = 0; i < mat[var].size(); i++)
        if (mat2[var][i]) qnnz++;
    if (qnnz) {
      hessian.dim_ = m.variables.size();
      qnnz = 0;
      // model_.hessian_ is initialised with start_[0] for fictitious
      // column 0, so have to clear this before pushing back start
      hessian.start_.clear();
      assert((int)hessian.start_.size() == 0);
      for (std::shared_ptr<Variable> var : m.variables) {
        hessian.start_.push_back(qnnz);
        for (size_t i = 0; i < mat[var].size(); i++) {
          double value = mat2[var][i];
          if (value) {
            hessian.index_.push_back(varindex[mat[var][i]->name]);
            hessian.value_.push_back(value);
            qnnz++;
          }
        }
      }
      hessian.start_.push_back(qnnz);
      hessian.format_ = HessianFormat::kSquare;
    } else {
      assert(hessian.dim_ == 0 && hessian.start_[0] == 0);
    }

    // handle constraints
    std::map<std::shared_ptr<Variable>, std::vector<unsigned int>>
        consofvarmap_index;
    std::map<std::shared_ptr<Variable>, std::vector<double>> consofvarmap_value;
    for (size_t i = 0; i < m.constraints.size(); i++) {
      std::shared_ptr<Constraint> con = m.constraints[i];
      lp.row_names_[i] = con->expr->name;
      for (size_t j = 0; j < con->expr->linterms.size(); j++) {
        std::shared_ptr<LinTerm> lt = con->expr->linterms[j];
        if (consofvarmap_index.count(lt->var) == 0) {
          consofvarmap_index[lt->var] = std::vector<unsigned int>();
          consofvarmap_value[lt->var] = std::vector<double>();
        }
        consofvarmap_index[lt->var].push_back(i);
        consofvarmap_value[lt->var].push_back(lt->coef);
      }

      lp.row_lower_.push_back(con->lowerbound);
      lp.row_upper_.push_back(con->upperbound);

      if (!con->expr->quadterms.empty()) {
        highsLogUser(options.log_options, HighsLogType::kError,
                     "Quadratic constraints not supported by HiGHS\n");
        return FilereaderRetcode::kParserError;
      }
    }

    // Check for empty row names, giving them a special name if possible
    bool highs_prefix_ok = true;
    bool used_highs_prefix = false;
    std::string highs_prefix = "HiGHS_R";
    for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
      // Look to see whether the name begins HiGHS_R
      if (strncmp(lp.row_names_[iRow].c_str(), highs_prefix.c_str(), 7) == 0) {
        printf("Name %s begins with \"HiGHS_R\"\n",
               lp.row_names_[iRow].c_str());
        highs_prefix_ok = false;
      } else if (lp.row_names_[iRow] == "") {
        // Make up a name beginning HiGHS_R
        lp.row_names_[iRow] = highs_prefix + std::to_string(iRow);
        used_highs_prefix = true;
      }
    }
    if (used_highs_prefix && !highs_prefix_ok) {
      // Have made up a name beginning HiGHS_R, but this occurs with
      // other "natural" rows, so abandon the row names
      lp.row_names_.clear();
      highsLogUser(options.log_options, HighsLogType::kWarning,
                   "Cannot create row name beginning \"HiGHS_R\" due to others "
                   "with same prefix: row names cleared\n");
    }

    HighsInt nz = 0;
    // lp.a_matrix_ is initialised with start_[0] for fictitious
    // column 0, so have to clear this before pushing back start
    lp.a_matrix_.start_.clear();
    assert((int)lp.a_matrix_.start_.size() == 0);
    for (HighsInt i = 0; i < lp.num_col_; i++) {
      std::shared_ptr<Variable> var = m.variables[i];
      lp.a_matrix_.start_.push_back(nz);
      for (size_t j = 0; j < consofvarmap_index[var].size(); j++) {
        double value = consofvarmap_value[var][j];
        if (value) {
          lp.a_matrix_.index_.push_back(consofvarmap_index[var][j]);
          lp.a_matrix_.value_.push_back(value);
          nz++;
        }
      }
    }
    lp.a_matrix_.start_.push_back(nz);
    lp.a_matrix_.format_ = MatrixFormat::kColwise;
    lp.sense_ = m.sense == ObjectiveSense::MIN ? ObjSense::kMinimize
                                               : ObjSense::kMaximize;
  } catch (std::invalid_argument& ex) {
    // lpassert in extern/filereaderlp/def.hpp throws
    // std::invalid_argument whatever the error. Hence, unless
    // something is done specially - here or elsewhere -
    // FilereaderRetcode::kParserError will be returned.
    //
    // This is misleading when the file isn't found, as it's not a
    // parser error
    FILE* file = fopen(filename.c_str(), "r");
    if (file == nullptr) return FilereaderRetcode::kFileNotFound;
    fclose(file);
    return FilereaderRetcode::kParserError;
  }
  lp.ensureColwise();
  return FilereaderRetcode::kOk;
}

void FilereaderLp::writeToFile(FILE* file, const char* format, ...) {
  va_list argptr;
  va_start(argptr, format);
  std::array<char, LP_MAX_LINE_LENGTH + 1> stringbuffer = {};
  HighsInt tokenlength =
      vsnprintf(stringbuffer.data(), stringbuffer.size(), format, argptr);
  va_end(argptr);
  if (this->linelength + tokenlength >= LP_MAX_LINE_LENGTH) {
    fprintf(file, "\n");
    fprintf(file, "%s", stringbuffer.data());
    this->linelength = tokenlength;
  } else {
    fprintf(file, "%s", stringbuffer.data());
    this->linelength += tokenlength;
  }
}

void FilereaderLp::writeToFileLineend(FILE* file) {
  fprintf(file, "\n");
  this->linelength = 0;
}

void FilereaderLp::writeToFileValue(FILE* file, const double value,
                                    const bool force_plus) {
  if (original_double_format) {
    this->writeToFile(file, " %+g", value);
  } else {
    // As for writeModelAsMps
    if (force_plus) {
      this->writeToFile(file, " %+.15g", value);
    } else {
      this->writeToFile(file, " %.15g", value);
    }
  }
}

void FilereaderLp::writeToFileVar(FILE* file, const HighsInt var_index) {
  this->writeToFile(file, " x%" HIGHSINT_FORMAT, var_index + 1);
}

void FilereaderLp::writeToFileVar(FILE* file, const std::string var_name) {
  this->writeToFile(file, " %s", var_name.c_str());
}

void FilereaderLp::writeToFileCon(FILE* file, const HighsInt con_index) {
  this->writeToFile(file, " con%" HIGHSINT_FORMAT, con_index + 1);
}

void FilereaderLp::writeToFileMatrixRow(FILE* file, const HighsInt iRow,
                                        const HighsSparseMatrix ar_matrix,
                                        const std::vector<string> col_names) {
  assert(ar_matrix.isRowwise());
  const bool has_col_names = allow_model_names && col_names.size() > 0;

  for (HighsInt iEl = ar_matrix.start_[iRow]; iEl < ar_matrix.start_[iRow + 1];
       iEl++) {
    HighsInt iCol = ar_matrix.index_[iEl];
    double coef = ar_matrix.value_[iEl];
    this->writeToFileValue(file, coef);
    if (has_col_names) {
      this->writeToFileVar(file, col_names[iCol]);
    } else {
      this->writeToFileVar(file, iCol);
    }
  }
}

HighsStatus FilereaderLp::writeModelToFile(const HighsOptions& options,
                                           const std::string filename,
                                           const HighsModel& model) {
  const HighsLp& lp = model.lp_;
  // Create a row-wise copy of the matrix
  HighsSparseMatrix ar_matrix = lp.a_matrix_;
  ar_matrix.ensureRowwise();

  const bool has_col_names =
      allow_model_names &&
      lp.col_names_.size() == static_cast<size_t>(lp.num_col_);
  const bool has_row_names =
      allow_model_names &&
      lp.row_names_.size() == static_cast<size_t>(lp.num_row_);
  FILE* file = fopen(filename.c_str(), "w");

  // write comment at the start of the file
  this->writeToFile(file, "\\ %s", LP_COMMENT_FILESTART);
  this->writeToFileLineend(file);

  // write objective
  this->writeToFile(file, "%s",
                    lp.sense_ == ObjSense::kMinimize ? "min" : "max");
  this->writeToFileLineend(file);
  this->writeToFile(file, " obj:");
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    double coef = lp.col_cost_[iCol];
    if (coef != 0.0) {
      this->writeToFileValue(file, coef);
      if (has_col_names) {
        this->writeToFileVar(file, lp.col_names_[iCol]);
      } else {
        this->writeToFileVar(file, iCol);
      }
    }
  }
  this->writeToFile(file,
                    " ");  // ToDo Unnecessary, but only to give empty diff
  if (model.isQp()) {
    this->writeToFile(file, "+ [");
    for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
      for (HighsInt iEl = model.hessian_.start_[iCol];
           iEl < model.hessian_.start_[iCol + 1]; iEl++) {
        HighsInt iRow = model.hessian_.index_[iEl];
        if (iCol <= iRow) {
          double coef = model.hessian_.value_[iEl];
          if (iCol != iRow) coef *= 2;
          if (coef != 0.0) {
            this->writeToFileValue(file, coef);
            if (has_col_names) {
              this->writeToFileVar(file, lp.col_names_[iCol]);
              this->writeToFile(file, " *");
              this->writeToFileVar(file, lp.col_names_[iRow]);
            } else {
              this->writeToFileVar(file, iCol);
              this->writeToFile(file, " *");
              this->writeToFileVar(file, iRow);
            }
          }
        }
      }
    }
    this->writeToFile(file,
                      "  ]/2 ");  // ToDo Surely needs only to be one space
  }
  double coef = lp.offset_;
  if (coef != 0) this->writeToFileValue(file, coef);
  this->writeToFileLineend(file);

  // write constraint section, lower & upper bounds are one constraint
  // each
  this->writeToFile(file, "st");
  this->writeToFileLineend(file);
  for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
    if (lp.row_lower_[iRow] == lp.row_upper_[iRow]) {
      // Equality constraint
      if (has_row_names) {
        this->writeToFileVar(file, lp.row_names_[iRow]);
      } else {
        this->writeToFileCon(file, iRow);
      }
      this->writeToFile(file, ":");
      this->writeToFileMatrixRow(file, iRow, ar_matrix, lp.col_names_);
      this->writeToFile(file, " =");
      this->writeToFileValue(file, lp.row_lower_[iRow], true);
      this->writeToFileLineend(file);
    } else {
      // Need to distinguish the names when writing out boxed
      // constraint row as two single-sided constraints
      const bool boxed =
          lp.row_lower_[iRow] > -kHighsInf && lp.row_upper_[iRow] < kHighsInf;
      if (lp.row_lower_[iRow] > -kHighsInf) {
        // Has a lower bound
        if (has_row_names) {
          this->writeToFileVar(file, lp.row_names_[iRow]);
        } else {
          this->writeToFileCon(file, iRow);
        }
        if (boxed) {
          this->writeToFile(file, "lo:");
        } else {
          this->writeToFile(file, ":");
        }
        this->writeToFileMatrixRow(file, iRow, ar_matrix, lp.col_names_);
        this->writeToFile(file, " >=");
        this->writeToFileValue(file, lp.row_lower_[iRow], true);
        this->writeToFileLineend(file);
      }
      if (lp.row_upper_[iRow] < kHighsInf) {
        // Has an upper bound
        if (has_row_names) {
          this->writeToFileVar(file, lp.row_names_[iRow]);
        } else {
          this->writeToFileCon(file, iRow);
        }
        if (boxed) {
          this->writeToFile(file, "up:");
        } else {
          this->writeToFile(file, ":");
        }
        this->writeToFileMatrixRow(file, iRow, ar_matrix, lp.col_names_);
        this->writeToFile(file, " <=");
        this->writeToFileValue(file, lp.row_upper_[iRow], true);
        this->writeToFileLineend(file);
      }
    }
  }

  // write bounds section
  this->writeToFile(file, "bounds");
  this->writeToFileLineend(file);
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    const bool default_bounds =
        lp.col_lower_[iCol] == 0 && lp.col_upper_[iCol] == kHighsInf;
    if (default_bounds) continue;
    if (lp.col_lower_[iCol] <= -kHighsInf && lp.col_upper_[iCol] >= kHighsInf) {
      // Free variable
      if (has_col_names) {
        this->writeToFileVar(file, lp.col_names_[iCol]);
      } else {
        this->writeToFileVar(file, iCol);
      }
      this->writeToFile(file, " free");
    } else if (lp.col_lower_[iCol] == lp.col_upper_[iCol]) {
      // Fixed variable
      if (has_col_names) {
        this->writeToFileVar(file, lp.col_names_[iCol]);
      } else {
        this->writeToFileVar(file, iCol);
      }
      this->writeToFile(file, " =");
      this->writeToFileValue(file, lp.col_upper_[iCol], false);
    } else {
      assert(!default_bounds);
      // Non-default bound
      if (lp.col_lower_[iCol] != 0) {
        // Nonzero lower bound
        this->writeToFileValue(file, lp.col_lower_[iCol], false);
        this->writeToFile(file, " <=");
      }
      if (has_col_names) {
        this->writeToFileVar(file, lp.col_names_[iCol]);
      } else {
        this->writeToFileVar(file, iCol);
      }
      if (lp.col_upper_[iCol] < kHighsInf) {
        // Finite upper bound
        this->writeToFile(file, " <=");
        this->writeToFileValue(file, lp.col_upper_[iCol], false);
      }
    }
    this->writeToFileLineend(file);
  }
  if (lp.integrality_.size() > 0) {
    // write binary section
    this->writeToFile(file, "bin");
    this->writeToFileLineend(file);
    for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
      if (lp.integrality_[iCol] == HighsVarType::kInteger) {
        if (lp.col_lower_[iCol] == 0.0 && lp.col_upper_[iCol] == 1.0) {
          if (has_col_names) {
            this->writeToFileVar(file, lp.col_names_[iCol]);
          } else {
            this->writeToFileVar(file, iCol);
          }
          this->writeToFileLineend(file);
        }
      }
    }

    // write general section
    this->writeToFile(file, "gen");
    this->writeToFileLineend(file);
    for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
      if (lp.integrality_[iCol] == HighsVarType::kInteger) {
        if (lp.col_lower_[iCol] != 0.0 || lp.col_upper_[iCol] != 1.0) {
          if (has_col_names) {
            this->writeToFileVar(file, lp.col_names_[iCol]);
          } else {
            this->writeToFileVar(file, iCol);
          }
          this->writeToFileLineend(file);
        }
      }
    }

    // write semi section
    this->writeToFile(file, "semi");
    this->writeToFileLineend(file);
    for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
      if (lp.integrality_[iCol] == HighsVarType::kSemiContinuous ||
          lp.integrality_[iCol] == HighsVarType::kSemiInteger) {
        if (has_col_names) {
          this->writeToFileVar(file, lp.col_names_[iCol]);
        } else {
          this->writeToFileVar(file, iCol);
        }
        this->writeToFileLineend(file);
      }
    }
  }
  // write end
  this->writeToFile(file, "end");
  this->writeToFileLineend(file);

  fclose(file);
  return HighsStatus::kOk;
}
