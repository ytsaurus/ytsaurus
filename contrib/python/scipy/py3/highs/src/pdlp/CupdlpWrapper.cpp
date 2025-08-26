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
/**@file pdlp/CupdlpWrapper.cpp
 * @brief
 * @author Julian Hall
 */
#include "pdlp/CupdlpWrapper.h"

void getUserParamsFromOptions(const HighsOptions& options,
                              cupdlp_bool* ifChangeIntParam,
                              cupdlp_int* intParam,
                              cupdlp_bool* ifChangeFloatParam,
                              cupdlp_float* floatParam);

void analysePdlpSolution(const HighsOptions& options, const HighsLp& lp,
                         const HighsSolution& highs_solution);

HighsStatus solveLpCupdlp(HighsLpSolverObject& solver_object) {
  return solveLpCupdlp(solver_object.options_, solver_object.timer_,
                       solver_object.lp_, solver_object.basis_,
                       solver_object.solution_, solver_object.model_status_,
                       solver_object.highs_info_, solver_object.callback_);
}

HighsStatus solveLpCupdlp(const HighsOptions& options, HighsTimer& timer,
                          const HighsLp& lp, HighsBasis& highs_basis,
                          HighsSolution& highs_solution,
                          HighsModelStatus& model_status, HighsInfo& highs_info,
                          HighsCallback& callback) {
  // Indicate that there is no valid primal solution, dual solution or basis
  highs_basis.valid = false;
  highs_solution.value_valid = false;
  highs_solution.dual_valid = false;
  // Indicate that no imprecise solution has (yet) been found
  resetModelStatusAndHighsInfo(model_status, highs_info);

  char* fp = nullptr;
  char* fp_sol = nullptr;

  int nCols;
  int nRows;
  int nEqs;
  int nCols_origin;
  cupdlp_bool ifSaveSol = false;
  cupdlp_bool ifPresolve = false;

  int nnz = 0;
  double* rhs = NULL;
  double* cost = NULL;

  cupdlp_float* lower = NULL;
  cupdlp_float* upper = NULL;

  // -------------------------
  int *csc_beg = NULL, *csc_idx = NULL;
  double* csc_val = NULL;
  double offset =
      0.0;  // true objVal = sig * c'x - offset, sig = 1 (min) or -1 (max)
  double sense_origin = 1;  // 1 (min) or -1 (max)
  int* constraint_new_idx = NULL;
  cupdlp_float* x_origin = cupdlp_NULL;
  cupdlp_float* y_origin = cupdlp_NULL;

  void* model = NULL;
  void* presolvedmodel = NULL;
  void* model2solve = NULL;

  // WIP on zbook?
  //
  //  HighsInt size_of_CUPDLPscaling = sizeof(CUPDLPscaling);
  //
  CUPDLPscaling* scaling = (CUPDLPscaling*)cupdlp_malloc(sizeof(CUPDLPscaling));

  // WIP on zbook?
  //
  //  printf("size_of_CUPDLPscaling = %d\n", size_of_CUPDLPscaling);
  //  scaling->ifRuizScaling = 1;
  //  printf("scaling->ifRuizScaling = %d\n", scaling->ifRuizScaling);

  // claim solvers variables
  // prepare pointers
  CUPDLP_MATRIX_FORMAT src_matrix_format = CSC;
  CUPDLP_MATRIX_FORMAT dst_matrix_format = CSR_CSC;
  CUPDLPcsc* csc_cpu = cupdlp_NULL;
  CUPDLPproblem* prob = cupdlp_NULL;

  // load parameters

  // set solver parameters
  cupdlp_bool ifChangeIntParam[N_INT_USER_PARAM] = {false};
  cupdlp_int intParam[N_INT_USER_PARAM] = {0};
  cupdlp_bool ifChangeFloatParam[N_FLOAT_USER_PARAM] = {false};
  cupdlp_float floatParam[N_FLOAT_USER_PARAM] = {0.0};

  // Transfer from options
  getUserParamsFromOptions(options, ifChangeIntParam, intParam,
                           ifChangeFloatParam, floatParam);

  std::vector<int> constraint_type(lp.num_row_);

  formulateLP_highs(lp, &cost, &nCols, &nRows, &nnz, &nEqs, &csc_beg, &csc_idx,
                    &csc_val, &rhs, &lower, &upper, &offset, &sense_origin,
                    &nCols_origin, &constraint_new_idx, constraint_type.data());

  const cupdlp_int local_log_level = getCupdlpLogLevel(options);
  if (local_log_level) cupdlp_printf("Solving with cuPDLP-C\n");

  H_Init_Scaling(local_log_level, scaling, nCols, nRows, cost, rhs);
  cupdlp_int ifScaling = 1;

  CUPDLPwork* w = cupdlp_NULL;
  cupdlp_init_work(w, 1);

  problem_create(&prob);

  // currently, only supprot that input matrix is CSC, and store both CSC and
  // CSR
  csc_create(&csc_cpu);
  csc_cpu->nRows = nRows;
  csc_cpu->nCols = nCols;
  csc_cpu->nMatElem = nnz;
  csc_cpu->colMatBeg = (int*)malloc((1 + nCols) * sizeof(int));
  csc_cpu->colMatIdx = (int*)malloc(nnz * sizeof(int));
  csc_cpu->colMatElem = (double*)malloc(nnz * sizeof(double));
  memcpy(csc_cpu->colMatBeg, csc_beg, (nCols + 1) * sizeof(int));
  memcpy(csc_cpu->colMatIdx, csc_idx, nnz * sizeof(int));
  memcpy(csc_cpu->colMatElem, csc_val, nnz * sizeof(double));

  cupdlp_float scaling_time = getTimeStamp();
  H_PDHG_Scale_Data_cuda(local_log_level, csc_cpu, ifScaling, scaling, cost,
                         lower, upper, rhs);
  scaling_time = getTimeStamp() - scaling_time;

  cupdlp_float alloc_matrix_time = 0.0;
  cupdlp_float copy_vec_time = 0.0;

  problem_alloc(prob, nRows, nCols, nEqs, cost, offset, sense_origin, csc_cpu,
                src_matrix_format, dst_matrix_format, rhs, lower, upper,
                &alloc_matrix_time, &copy_vec_time);

  w->problem = prob;
  w->scaling = scaling;
  PDHG_Alloc(w);
  w->timers->dScalingTime = scaling_time;
  w->timers->dPresolveTime = 0;  // presolve_time;
  cupdlp_copy_vec(w->rowScale, scaling->rowScale, cupdlp_float, nRows);
  cupdlp_copy_vec(w->colScale, scaling->colScale, cupdlp_float, nCols);

  // CUPDLP_CALL(LP_SolvePDHG(prob, cupdlp_NULL, cupdlp_NULL, cupdlp_NULL,
  // cupdlp_NULL));
  //   CUPDLP_CALL(LP_SolvePDHG(prob, ifChangeIntParam, intParam,
  //                               ifChangeFloatParam, floatParam, fp));

  cupdlp_init_double(x_origin, nCols_origin);
  cupdlp_init_double(y_origin, nRows);
  // Resize the highs_solution so cuPDLP-c can use it internally
  highs_solution.col_value.resize(lp.num_col_);
  highs_solution.row_value.resize(lp.num_row_);
  highs_solution.col_dual.resize(lp.num_col_);
  highs_solution.row_dual.resize(lp.num_row_);
  int value_valid = 0;
  int dual_valid = 0;
  int pdlp_model_status = 0;
  cupdlp_int pdlp_num_iter = 0;

  cupdlp_retcode retcode = LP_SolvePDHG(
      w, ifChangeIntParam, intParam, ifChangeFloatParam, floatParam, fp,
      nCols_origin, highs_solution.col_value.data(),
      highs_solution.col_dual.data(), highs_solution.row_value.data(),
      highs_solution.row_dual.data(), &value_valid, &dual_valid, ifSaveSol,
      fp_sol, constraint_new_idx, constraint_type.data(), &pdlp_model_status,
      &pdlp_num_iter);
  highs_info.pdlp_iteration_count = pdlp_num_iter;

  model_status = HighsModelStatus::kUnknown;
  if (retcode != RETCODE_OK) return HighsStatus::kError;

  highs_solution.value_valid = value_valid;
  highs_solution.dual_valid = dual_valid;

  if (pdlp_model_status == OPTIMAL) {
    model_status = HighsModelStatus::kOptimal;
  } else if (pdlp_model_status == INFEASIBLE) {
    model_status = HighsModelStatus::kInfeasible;
  } else if (pdlp_model_status == UNBOUNDED) {
    model_status = HighsModelStatus::kUnbounded;
  } else if (pdlp_model_status == INFEASIBLE_OR_UNBOUNDED) {
    model_status = HighsModelStatus::kUnboundedOrInfeasible;
  } else if (pdlp_model_status == TIMELIMIT_OR_ITERLIMIT) {
    model_status = pdlp_num_iter >= intParam[N_ITER_LIM] - 1
                       ? HighsModelStatus::kIterationLimit
                       : HighsModelStatus::kTimeLimit;
  } else if (pdlp_model_status == FEASIBLE) {
    assert(111 == 666);
    model_status = HighsModelStatus::kUnknown;
  } else {
    assert(111 == 777);
  }
#if CUPDLP_DEBUG
  analysePdlpSolution(options, lp, highs_solution);
#endif

  free(cost);
  free(lower);
  free(upper);
  free(csc_beg);
  free(csc_idx);
  free(csc_val);
  free(rhs);

  free(x_origin);
  free(y_origin);

  free(constraint_new_idx);

  free(prob->cost);
  free(prob->lower);
  free(prob->upper);
  free(prob->rhs);

  free(prob->hasLower);
  free(prob->hasUpper);

  free(prob->data->csr_matrix->rowMatBeg);
  free(prob->data->csr_matrix->rowMatIdx);
  free(prob->data->csr_matrix->rowMatElem);
  free(prob->data->csr_matrix);

  free(prob->data->csc_matrix->colMatBeg);
  free(prob->data->csc_matrix->colMatIdx);
  free(prob->data->csc_matrix->colMatElem);
  free(prob->data->csc_matrix);

  free(prob->data);

  free(prob);

  free(csc_cpu->colMatBeg);
  free(csc_cpu->colMatIdx);
  free(csc_cpu->colMatElem);

  free(csc_cpu);

  if (scaling->rowScale != nullptr) free(scaling->rowScale);
  if (scaling->colScale != nullptr) free(scaling->colScale);
  free(scaling);

  return HighsStatus::kOk;
}

int formulateLP_highs(const HighsLp& lp, double** cost, int* nCols, int* nRows,
                      int* nnz, int* nEqs, int** csc_beg, int** csc_idx,
                      double** csc_val, double** rhs, double** lower,
                      double** upper, double* offset, double* sense_origin,
                      int* nCols_origin, int** constraint_new_idx,
                      int* constraint_type) {
  int retcode = 0;

  // problem size for malloc
  int nCols_clp = lp.num_col_;
  int nRows_clp = lp.num_row_;
  int nnz_clp = lp.a_matrix_.start_[lp.num_col_];
  *nCols_origin = nCols_clp;
  *nRows = nRows_clp;    // need not recalculate
  *nCols = nCols_clp;    // need recalculate
  *nEqs = 0;             // need recalculate
  *nnz = nnz_clp;        // need recalculate
  *offset = lp.offset_;  // need not recalculate
  if (lp.sense_ == ObjSense::kMinimize) {
    *sense_origin = 1.0;
  } else if (lp.sense_ == ObjSense::kMaximize) {
    *sense_origin = -1.0;
  }

  const double* lhs_clp = lp.row_lower_.data();
  const double* rhs_clp = lp.row_upper_.data();
  const HighsInt* A_csc_beg = lp.a_matrix_.start_.data();
  const HighsInt* A_csc_idx = lp.a_matrix_.index_.data();
  const double* A_csc_val = lp.a_matrix_.value_.data();
  int has_lower, has_upper;

  cupdlp_init_int(*constraint_new_idx, *nRows);

  // recalculate nRows and nnz for Ax - z = 0
  for (int i = 0; i < nRows_clp; i++) {
    has_lower = lhs_clp[i] > -1e20;
    has_upper = rhs_clp[i] < 1e20;

    // count number of equations and rows
    if (has_lower && has_upper && lhs_clp[i] == rhs_clp[i]) {
      constraint_type[i] = EQ;
      (*nEqs)++;
    } else if (has_lower && !has_upper) {
      constraint_type[i] = GEQ;
    } else if (!has_lower && has_upper) {
      constraint_type[i] = LEQ;
    } else if (has_lower && has_upper) {
      constraint_type[i] = BOUND;
      (*nCols)++;
      (*nnz)++;
      (*nEqs)++;
    } else {
      // printf("Error: constraint %d has no lower and upper bound\n", i);
      // retcode = 1;
      // goto exit_cleanup;

      // what if regard free as bounded
      printf("Warning: constraint %d has no lower and upper bound\n", i);
      constraint_type[i] = BOUND;
      (*nCols)++;
      (*nnz)++;
      (*nEqs)++;
    }
  }

  // allocate memory
  cupdlp_init_double(*cost, *nCols);
  cupdlp_init_double(*lower, *nCols);
  cupdlp_init_double(*upper, *nCols);
  cupdlp_init_int(*csc_beg, *nCols + 1);
  cupdlp_init_int(*csc_idx, *nnz);
  cupdlp_init_double(*csc_val, *nnz);
  cupdlp_init_double(*rhs, *nRows);

  // cost, lower, upper
  for (int i = 0; i < nCols_clp; i++) {
    (*cost)[i] = lp.col_cost_[i] * (*sense_origin);
    (*lower)[i] = lp.col_lower_[i];

    (*upper)[i] = lp.col_upper_[i];
  }
  // slack costs
  for (int i = nCols_clp; i < *nCols; i++) {
    (*cost)[i] = 0.0;
  }
  // slack bounds
  for (int i = 0, j = nCols_clp; i < *nRows; i++) {
    if (constraint_type[i] == BOUND) {
      (*lower)[j] = lhs_clp[i];
      (*upper)[j] = rhs_clp[i];
      j++;
    }
  }

  for (int i = 0; i < *nCols; i++) {
    if ((*lower)[i] < -1e20) (*lower)[i] = -INFINITY;
    if ((*upper)[i] > 1e20) (*upper)[i] = INFINITY;
  }

  // permute LP rhs
  // EQ or BOUND first
  for (int i = 0, j = 0; i < *nRows; i++) {
    if (constraint_type[i] == EQ) {
      (*rhs)[j] = lhs_clp[i];
      (*constraint_new_idx)[i] = j;
      j++;
    } else if (constraint_type[i] == BOUND) {
      (*rhs)[j] = 0.0;
      (*constraint_new_idx)[i] = j;
      j++;
    }
  }
  // then LEQ or GEQ
  for (int i = 0, j = *nEqs; i < *nRows; i++) {
    if (constraint_type[i] == LEQ) {
      (*rhs)[j] = -rhs_clp[i];  // multiply -1
      (*constraint_new_idx)[i] = j;
      j++;
    } else if (constraint_type[i] == GEQ) {
      (*rhs)[j] = lhs_clp[i];
      (*constraint_new_idx)[i] = j;
      j++;
    }
  }

  // formulate and permute LP matrix
  // beg remains the same
  for (int i = 0; i < nCols_clp + 1; i++) (*csc_beg)[i] = A_csc_beg[i];
  for (int i = nCols_clp + 1; i < *nCols + 1; i++)
    (*csc_beg)[i] = (*csc_beg)[i - 1] + 1;

  // row idx changes
  for (int i = 0, k = 0; i < nCols_clp; i++) {
    // same order as in rhs
    // EQ or BOUND first
    for (int j = (*csc_beg)[i]; j < (*csc_beg)[i + 1]; j++) {
      if (constraint_type[A_csc_idx[j]] == EQ ||
          constraint_type[A_csc_idx[j]] == BOUND) {
        (*csc_idx)[k] = (*constraint_new_idx)[A_csc_idx[j]];
        (*csc_val)[k] = A_csc_val[j];
        k++;
      }
    }
    // then LEQ or GEQ
    for (int j = (*csc_beg)[i]; j < (*csc_beg)[i + 1]; j++) {
      if (constraint_type[A_csc_idx[j]] == LEQ) {
        (*csc_idx)[k] = (*constraint_new_idx)[A_csc_idx[j]];
        (*csc_val)[k] = -A_csc_val[j];  // multiply -1
        k++;
      } else if (constraint_type[A_csc_idx[j]] == GEQ) {
        (*csc_idx)[k] = (*constraint_new_idx)[A_csc_idx[j]];
        (*csc_val)[k] = A_csc_val[j];
        k++;
      }
    }
  }

  // slacks for BOUND
  for (int i = 0, j = nCols_clp; i < *nRows; i++) {
    if (constraint_type[i] == BOUND) {
      (*csc_idx)[(*csc_beg)[j]] = (*constraint_new_idx)[i];
      (*csc_val)[(*csc_beg)[j]] = -1.0;
      j++;
    }
  }

  return retcode;
}

cupdlp_retcode problem_create(CUPDLPproblem** prob) {
  cupdlp_retcode retcode = RETCODE_OK;

  cupdlp_init_problem(*prob, 1);

  return retcode;
}

// cupdlp_retcode csc_create(CUPDLPcsc **csc_cpu) {
//   cupdlp_retcode retcode = RETCODE_OK;
//
//   cupdlp_init_csc_cpu(*csc_cpu, 1);
//
//   return retcode;
// }

cupdlp_retcode data_alloc(CUPDLPdata* data, cupdlp_int nRows, cupdlp_int nCols,
                          void* matrix, CUPDLP_MATRIX_FORMAT src_matrix_format,
                          CUPDLP_MATRIX_FORMAT dst_matrix_format) {
  cupdlp_retcode retcode = RETCODE_OK;

  data->nRows = nRows;
  data->nCols = nCols;
  data->matrix_format = dst_matrix_format;
  data->dense_matrix = cupdlp_NULL;
  data->csr_matrix = cupdlp_NULL;
  data->csc_matrix = cupdlp_NULL;
  data->device = CPU;

  switch (dst_matrix_format) {
    case DENSE:
      dense_create(&data->dense_matrix);
      dense_alloc_matrix(data->dense_matrix, nRows, nCols, matrix,
                         src_matrix_format);
      break;
    case CSR:
      csr_create(&data->csr_matrix);
      csr_alloc_matrix(data->csr_matrix, nRows, nCols, matrix,
                       src_matrix_format);
      break;
    case CSC:
      csc_create(&data->csc_matrix);
      csc_alloc_matrix(data->csc_matrix, nRows, nCols, matrix,
                       src_matrix_format);
      break;
    case CSR_CSC:
      csc_create(&data->csc_matrix);
      csc_alloc_matrix(data->csc_matrix, nRows, nCols, matrix,
                       src_matrix_format);
      csr_create(&data->csr_matrix);
      csr_alloc_matrix(data->csr_matrix, nRows, nCols, matrix,
                       src_matrix_format);
      break;
    default:
      break;
  }
  // currently, only supprot that input matrix is CSC, and store both CSC and
  // CSR data->csc_matrix = matrix;

  return retcode;
}

cupdlp_retcode problem_alloc(
    CUPDLPproblem* prob, cupdlp_int nRows, cupdlp_int nCols, cupdlp_int nEqs,
    cupdlp_float* cost, cupdlp_float offset, cupdlp_float sense_origin,
    void* matrix, CUPDLP_MATRIX_FORMAT src_matrix_format,
    CUPDLP_MATRIX_FORMAT dst_matrix_format, cupdlp_float* rhs,
    cupdlp_float* lower, cupdlp_float* upper, cupdlp_float* alloc_matrix_time,
    cupdlp_float* copy_vec_time) {
  cupdlp_retcode retcode = RETCODE_OK;
  prob->nRows = nRows;
  prob->nCols = nCols;
  prob->nEqs = nEqs;
  prob->data = cupdlp_NULL;
  prob->cost = cupdlp_NULL;
  prob->offset = offset;
  prob->sense_origin = sense_origin;
  prob->rhs = cupdlp_NULL;
  prob->lower = cupdlp_NULL;
  prob->upper = cupdlp_NULL;

  cupdlp_float begin = getTimeStamp();

  cupdlp_init_data(prob->data, 1);
  cupdlp_init_vec_double(prob->cost, nCols);
  cupdlp_init_vec_double(prob->rhs, nRows);
  cupdlp_init_vec_double(prob->lower, nCols);
  cupdlp_init_vec_double(prob->upper, nCols);
  cupdlp_init_zero_vec_double(prob->hasLower, nCols);
  cupdlp_init_zero_vec_double(prob->hasUpper, nCols);

  data_alloc(prob->data, nRows, nCols, matrix, src_matrix_format,
             dst_matrix_format);
  *alloc_matrix_time = getTimeStamp() - begin;

  prob->data->csc_matrix->MatElemNormInf =
      infNorm(((CUPDLPcsc*)matrix)->colMatElem, ((CUPDLPcsc*)matrix)->nMatElem);

  begin = getTimeStamp();
  cupdlp_copy_vec(prob->cost, cost, cupdlp_float, nCols);
  cupdlp_copy_vec(prob->rhs, rhs, cupdlp_float, nRows);
  cupdlp_copy_vec(prob->lower, lower, cupdlp_float, nCols);
  cupdlp_copy_vec(prob->upper, upper, cupdlp_float, nCols);
  *copy_vec_time = getTimeStamp() - begin;

  // Keep
  cupdlp_haslb(prob->hasLower, prob->lower, -INFINITY, nCols);
  cupdlp_hasub(prob->hasUpper, prob->upper, +INFINITY, nCols);

  // TODO: cal dMaxCost, dMaxRhs, dMaxRowBound

  return retcode;
}

// ToDo: Why can linker not pick up infNorm, cupdlp_haslb and
// cupdlp_hasub from pdlp/cupdlp/cupdlp_linalg.c?
double infNorm(double* x, cupdlp_int n) {
  double norm = 0;
  for (HighsInt iX = 0; iX < n; iX++) norm = std::max(std::fabs(x[iX]), norm);
  return norm;
}
void cupdlp_haslb(cupdlp_float* haslb, const cupdlp_float* lb,
                  const cupdlp_float bound, const cupdlp_int len) {
  for (int i = 0; i < len; i++) {
    haslb[i] = lb[i] > bound ? 1.0 : 0.0;
  }
}

void cupdlp_hasub(cupdlp_float* hasub, const cupdlp_float* ub,
                  const cupdlp_float bound, const cupdlp_int len) {
  for (int i = 0; i < len; i++) {
    hasub[i] = ub[i] < bound ? 1.0 : 0.0;
  }
}

void getUserParamsFromOptions(const HighsOptions& options,
                              cupdlp_bool* ifChangeIntParam,
                              cupdlp_int* intParam,
                              cupdlp_bool* ifChangeFloatParam,
                              cupdlp_float* floatParam) {
  for (cupdlp_int i = 0; i < N_INT_USER_PARAM; ++i) ifChangeIntParam[i] = false;
  for (cupdlp_int i = 0; i < N_FLOAT_USER_PARAM; ++i)
    ifChangeFloatParam[i] = false;
  // Assume all PDLP-related options in HiGHS cause changes
  ifChangeIntParam[N_ITER_LIM] = true;
  // If HiGHS is using 64-bit integers, then the default value of
  // options.pdlp_iteration_limit is kHighsIInf, so copying this to
  // intParam[N_ITER_LIM] will overflow.
  intParam[N_ITER_LIM] = cupdlp_int(options.pdlp_iteration_limit > kHighsIInf32
                                        ? kHighsIInf32
                                        : options.pdlp_iteration_limit);
  //
  ifChangeIntParam[N_LOG_LEVEL] = true;
  intParam[N_LOG_LEVEL] = getCupdlpLogLevel(options);
  //
  ifChangeIntParam[IF_SCALING] = true;
  intParam[IF_SCALING] = options.pdlp_scaling ? 1 : 0;
  //
  ifChangeFloatParam[D_PRIMAL_TOL] = true;
  floatParam[D_PRIMAL_TOL] = options.primal_feasibility_tolerance;
  //
  ifChangeFloatParam[D_DUAL_TOL] = true;
  floatParam[D_DUAL_TOL] = options.dual_feasibility_tolerance;
  //
  ifChangeFloatParam[D_GAP_TOL] = true;
  floatParam[D_GAP_TOL] = options.pdlp_d_gap_tol;
  //
  ifChangeFloatParam[D_TIME_LIM] = true;
  floatParam[D_TIME_LIM] = options.time_limit;
  //
  ifChangeIntParam[E_RESTART_METHOD] = true;
  intParam[E_RESTART_METHOD] = int(options.pdlp_e_restart_method);
  //
  ifChangeIntParam[I_INF_NORM_ABS_LOCAL_TERMINATION] = true;
  intParam[I_INF_NORM_ABS_LOCAL_TERMINATION] = !options.pdlp_native_termination;
}

void analysePdlpSolution(const HighsOptions& options, const HighsLp& lp,
                         const HighsSolution& highs_solution) {
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++)
    printf("x[%2d] = %11.5g\n", int(iCol), highs_solution.col_value[iCol]);
  for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
    printf("y[%2d] = %11.5g\n", int(iRow), highs_solution.row_dual[iRow]);
  }

  HighsInt num_primal_infeasibility = 0;
  HighsInt num_dual_infeasibility = 0;
  double max_primal_infeasibility = 0;
  double max_dual_infeasibility = 0;
  double sum_primal_infeasibility = 0;
  double sum_dual_infeasibility = 0;
  const double primal_feasibility_tolerance =
      options.primal_feasibility_tolerance;
  const double dual_feasibility_tolerance = options.dual_feasibility_tolerance;
  double lower;
  double upper;
  double value;
  double dual;
  // lambda for computing infeasibilities
  auto updateInfeasibilities = [&]() {
    double primal_infeasibility = 0;
    double dual_infeasibility = 0;
    // @primal_infeasibility calculation
    if (value < lower - primal_feasibility_tolerance) {
      // Below lower
      primal_infeasibility = lower - value;
    } else if (value > upper + primal_feasibility_tolerance) {
      // Above upper
      primal_infeasibility = value - upper;
    }
    double value_residual =
        std::min(std::fabs(lower - value), std::fabs(value - upper));
    bool at_a_bound = value_residual <= primal_feasibility_tolerance;
    if (at_a_bound) {
      // At a bound
      double middle = (lower + upper) * 0.5;
      if (lower < upper) {
        // Non-fixed variable
        if (value < middle) {
          // At lower
          dual_infeasibility = std::max(-dual, 0.);
        } else {
          // At upper
          dual_infeasibility = std::max(dual, 0.);
        }
      } else {
        // Fixed variable
        dual_infeasibility = 0;
      }
    } else {
      // Off bounds (or free)
      dual_infeasibility = fabs(dual);
    }
    // Accumulate primal infeasibilities
    if (primal_infeasibility > primal_feasibility_tolerance)
      num_primal_infeasibility++;
    max_primal_infeasibility =
        std::max(primal_infeasibility, max_primal_infeasibility);
    sum_primal_infeasibility += primal_infeasibility;
    // Accumulate dual infeasibilities
    if (dual_infeasibility > dual_feasibility_tolerance)
      num_dual_infeasibility++;
    max_dual_infeasibility =
        std::max(dual_infeasibility, max_dual_infeasibility);
    sum_dual_infeasibility += dual_infeasibility;
  };

  // Apply the model sense, as PDLP will have done this
  for (HighsInt iCol = 0; iCol < lp.num_col_; iCol++) {
    lower = lp.col_lower_[iCol];
    upper = lp.col_upper_[iCol];
    value = highs_solution.col_value[iCol];
    dual = int(lp.sense_) * highs_solution.col_dual[iCol];
    updateInfeasibilities();
  }
  for (HighsInt iRow = 0; iRow < lp.num_row_; iRow++) {
    lower = lp.row_lower_[iRow];
    upper = lp.row_upper_[iRow];
    value = highs_solution.row_value[iRow];
    dual = int(lp.sense_) * highs_solution.row_dual[iRow];
    updateInfeasibilities();
  }
  //
  // Determine the sum of complementary violations
  double max_complementary_violation = 0;
  for (HighsInt iVar = 0; iVar < lp.num_col_ + lp.num_row_; iVar++) {
    const bool is_col = iVar < lp.num_col_;
    const HighsInt iRow = iVar - lp.num_col_;
    const double primal = is_col ? highs_solution.col_value[iVar]
                                 : highs_solution.row_value[iRow];
    const double dual =
        is_col ? highs_solution.col_dual[iVar] : highs_solution.row_dual[iRow];
    const double lower = is_col ? lp.col_lower_[iVar] : lp.row_lower_[iRow];
    const double upper = is_col ? lp.col_upper_[iVar] : lp.row_upper_[iRow];
    const double mid = (lower + upper) * 0.5;
    const double primal_residual =
        primal < mid ? std::fabs(lower - primal) : std::fabs(upper - primal);
    const double dual_residual = std::fabs(dual);
    const double complementary_violation = primal_residual * dual_residual;
    max_complementary_violation =
        std::max(complementary_violation, max_complementary_violation);
    printf(
        "%s %2d [%11.5g, %11.5g, %11.5g] has (primal_residual, dual) values "
        "(%11.6g, %11.6g) so complementary_violation = %11.6g\n",
        is_col ? "Column" : "Row   ", is_col ? int(iVar) : int(iRow), lower,
        primal, upper, primal_residual, dual_residual, complementary_violation);
  }
  printf("PDLP max complementary violation = %g\n",
         max_complementary_violation);
  printf("     primal infeasibilities (%d, %11.6g, %11.6g)\n",
         int(num_primal_infeasibility), sum_primal_infeasibility,
         max_primal_infeasibility);
  printf("     dual   infeasibilities (%d, %11.6g, %11.6g)\n",
         int(num_dual_infeasibility), sum_dual_infeasibility,
         max_dual_infeasibility);
}

cupdlp_int getCupdlpLogLevel(const HighsOptions& options) {
  if (options.output_flag) {
    if (options.log_dev_level) {
      return 2;
    } else {
      return 1;
    }
  }
  return 0;
}
