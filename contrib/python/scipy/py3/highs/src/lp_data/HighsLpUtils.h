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
/**@file lp_data/HighsLpUtils.h
 * @brief Class-independent utilities for HiGHS
 */
#ifndef LP_DATA_HIGHSLPUTILS_H_
#define LP_DATA_HIGHSLPUTILS_H_

#include <vector>

#include "lp_data/HConst.h"
#include "lp_data/HighsInfo.h"
#include "lp_data/HighsLp.h"
#include "lp_data/HighsStatus.h"
#include "util/HighsUtils.h"

// class HighsLp;
struct SimplexScale;
struct HighsBasis;
struct HighsSolution;
class HighsOptions;

using std::vector;

void writeBasisFile(FILE*& file, const HighsBasis& basis);

HighsStatus readBasisFile(const HighsLogOptions& log_options, HighsBasis& basis,
                          const std::string filename);
HighsStatus readBasisStream(const HighsLogOptions& log_options,
                            HighsBasis& basis, std::ifstream& in_file);

// Methods taking HighsLp as an argument
HighsStatus assessLp(HighsLp& lp, const HighsOptions& options);

bool lpDimensionsOk(std::string message, const HighsLp& lp,
                    const HighsLogOptions& log_options);

HighsStatus assessCosts(const HighsOptions& options, const HighsInt ml_col_os,
                        const HighsIndexCollection& index_collection,
                        vector<double>& cost, bool& has_infinite_cost,
                        const double infinite_cost);

HighsStatus assessBounds(const HighsOptions& options, const char* type,
                         const HighsInt ml_ix_os,
                         const HighsIndexCollection& index_collection,
                         vector<double>& lower, vector<double>& upper,
                         const double infinite_bound,
                         const HighsVarType* integrality = nullptr);

HighsStatus cleanBounds(const HighsOptions& options, HighsLp& lp);

bool boundScaleOk(const std::vector<double>& lower,
                  const std::vector<double>& upper, const HighsInt bound_scale,
                  const double infinite_bound);

bool costScaleOk(const std::vector<double>& cost, const HighsInt cost_scale,
                 const double infinite_cost);

HighsStatus assessSemiVariables(HighsLp& lp, const HighsOptions& options,
                                bool& made_semi_variable_mods);
void relaxSemiVariables(HighsLp& lp, bool& made_semi_variable_mods);

bool activeModifiedUpperBounds(const HighsOptions& options, const HighsLp& lp,
                               const std::vector<double> col_value);

bool considerScaling(const HighsOptions& options, HighsLp& lp);
void scaleLp(const HighsOptions& options, HighsLp& lp,
             const bool force_scaling = false);
bool equilibrationScaleMatrix(const HighsOptions& options, HighsLp& lp,
                              const HighsInt use_scale_strategy);
bool maxValueScaleMatrix(const HighsOptions& options, HighsLp& lp,
                         const HighsInt use_scale_strategy);

HighsStatus applyScalingToLpCol(HighsLp& lp, const HighsInt col,
                                const double colScale);

HighsStatus applyScalingToLpRow(HighsLp& lp, const HighsInt row,
                                const double rowScale);

void unscaleSolution(HighsSolution& solution, const HighsScale& scale);

void appendColsToLpVectors(HighsLp& lp, const HighsInt num_new_col,
                           const vector<double>& colCost,
                           const vector<double>& colLower,
                           const vector<double>& colUpper);

void appendRowsToLpVectors(HighsLp& lp, const HighsInt num_new_row,
                           const vector<double>& rowLower,
                           const vector<double>& rowUpper);

void deleteScale(vector<double>& scale,
                 const HighsIndexCollection& index_collection);

void changeLpMatrixCoefficient(HighsLp& lp, const HighsInt row,
                               const HighsInt col, const double new_value,
                               const bool zero_new_value);

void changeLpIntegrality(HighsLp& lp,
                         const HighsIndexCollection& index_collection,
                         const vector<HighsVarType>& new_integrality);

void changeLpCosts(HighsLp& lp, const HighsIndexCollection& index_collection,
                   const vector<double>& new_col_cost,
                   const double infinite_cost);

void changeLpColBounds(HighsLp& lp,
                       const HighsIndexCollection& index_collection,
                       const vector<double>& new_col_lower,
                       const vector<double>& new_col_upper);

void changeLpRowBounds(HighsLp& lp,
                       const HighsIndexCollection& index_collection,
                       const vector<double>& new_row_lower,
                       const vector<double>& new_row_upper);

void changeBounds(vector<double>& lower, vector<double>& upper,
                  const HighsIndexCollection& index_collection,
                  const vector<double>& new_lower,
                  const vector<double>& new_upper);

/**
 * @brief Report the data of an LP
 */
void reportLp(const HighsLogOptions& log_options,
              const HighsLp& lp,  //!< LP whose data are to be reported
              const HighsLogType report_level = HighsLogType::kInfo
              //!< INFO => scalar [dimensions];
              //!< DETAILED => vector[costs/bounds];
              //!< VERBOSE => vector+matrix
);
/**
 * @brief Report the brief data of an LP
 */
void reportLpBrief(const HighsLogOptions& log_options,
                   const HighsLp& lp  //!< LP whose data are to be reported
);
/**
 * @brief Report the data of an LP
 */
void reportLpDimensions(const HighsLogOptions& log_options,
                        const HighsLp& lp  //!< LP whose data are to be reported
);
/**
 * @brief Report the data of an LP
 */
void reportLpObjSense(const HighsLogOptions& log_options,
                      const HighsLp& lp  //!< LP whose data are to be reported
);
/**
 * @brief Report the data of an LP
 */
void reportLpColVectors(const HighsLogOptions& log_options,
                        const HighsLp& lp  //!< LP whose data are to be reported
);
/**
 * @brief Report the data of an LP
 */
void reportLpRowVectors(const HighsLogOptions& log_options,
                        const HighsLp& lp  //!< LP whose data are to be reported
);
/**
 * @brief Report the data of an LP
 */
void reportLpColMatrix(const HighsLogOptions& log_options,
                       const HighsLp& lp  //!< LP whose data are to be reported
);

void reportMatrix(const HighsLogOptions& log_options, const std::string message,
                  const HighsInt num_col, const HighsInt num_nz,
                  const HighsInt* start, const HighsInt* index,
                  const double* value);

// Get the number of integer-valued columns in the LP
HighsInt getNumInt(const HighsLp& lp);

// Get the costs for a contiguous set of columns
void getLpCosts(const HighsLp& lp, const HighsInt from_col,
                const HighsInt to_col, double* XcolCost);

// Get the bounds for a contiguous set of columns
void getLpColBounds(const HighsLp& lp, const HighsInt from_col,
                    const HighsInt to_col, double* XcolLower,
                    double* XcolUpper);

// Get the bounds for a contiguous set of rows
void getLpRowBounds(const HighsLp& lp, const HighsInt from_row,
                    const HighsInt to_row, double* XrowLower,
                    double* XrowUpper);

void getLpMatrixCoefficient(const HighsLp& lp, const HighsInt row,
                            const HighsInt col, double* val);
// Analyse the data in an LP problem
void analyseLp(const HighsLogOptions& log_options, const HighsLp& lp);

HighsStatus readSolutionFile(const std::string filename,
                             const HighsOptions& options, HighsLp& lp,
                             HighsBasis& basis, HighsSolution& solution,
                             const HighsInt style);

HighsStatus readSolutionFileErrorReturn(std::ifstream& in_file);
HighsStatus readSolutionFileReturn(const HighsStatus status,
                                   HighsSolution& solution, HighsBasis& basis,
                                   const HighsSolution& read_solution,
                                   const HighsBasis& read_basis,
                                   std::ifstream& in_file);
bool readSolutionFileIgnoreLineOk(std::ifstream& in_file);
bool readSolutionFileKeywordLineOk(std::string& keyword,
                                   std::ifstream& in_file);
bool readSolutionFileHashKeywordIntLineOk(std::string& keyword, HighsInt& value,
                                          std::ifstream& in_file);
bool readSolutionFileIdIgnoreLineOk(std::string& id, std::ifstream& in_file);
bool readSolutionFileIdDoubleLineOk(std::string& id, double& value,
                                    std::ifstream& in_file);
bool readSolutionFileIdDoubleIntLineOk(double& value, HighsInt& index,
                                       std::ifstream& in_file);

void assessColPrimalSolution(const HighsOptions& options, const double primal,
                             const double lower, const double upper,
                             const HighsVarType type, double& col_infeasibility,
                             double& integer_infeasibility);

HighsStatus assessLpPrimalSolution(const std::string message,
                                   const HighsOptions& options,
                                   const HighsLp& lp,
                                   const HighsSolution& solution, bool& valid,
                                   bool& integral, bool& feasible);

HighsStatus calculateRowValuesQuad(const HighsLp& lp,
                                   const std::vector<double>& col_value,
                                   std::vector<double>& row_value,
                                   const HighsInt report_row = -1);
HighsStatus calculateRowValuesQuad(const HighsLp& lp, HighsSolution& solution,
                                   const HighsInt report_row = -1);

HighsStatus calculateColDualsQuad(const HighsLp& lp, HighsSolution& solution);

bool isColDataNull(const HighsLogOptions& log_options,
                   const double* usr_col_cost, const double* usr_col_lower,
                   const double* usr_col_upper);
bool isRowDataNull(const HighsLogOptions& log_options,
                   const double* usr_row_lower, const double* usr_row_upper);
bool isMatrixDataNull(const HighsLogOptions& log_options,
                      const HighsInt* usr_matrix_start,
                      const HighsInt* usr_matrix_index,
                      const double* usr_matrix_value);

void reportPresolveReductions(const HighsLogOptions& log_options,
                              const HighsLp& lp, const HighsLp& presolve_lp);

void reportPresolveReductions(const HighsLogOptions& log_options,
                              const HighsLp& lp, const bool presolve_to_empty);

bool isLessInfeasibleDSECandidate(const HighsLogOptions& log_options,
                                  const HighsLp& lp);

HighsLp withoutSemiVariables(const HighsLp& lp, HighsSolution& solution,
                             const double primal_feasibility_tolerance);

void removeRowsOfCountOne(const HighsLogOptions& log_options, HighsLp& lp);

#endif  // LP_DATA_HIGHSLPUTILS_H_
