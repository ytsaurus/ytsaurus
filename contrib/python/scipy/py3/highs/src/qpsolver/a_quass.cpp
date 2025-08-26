#include "qpsolver/a_quass.hpp"

#include "qpsolver/a_asm.hpp"
#include "qpsolver/feasibility_bounded.hpp"
#include "qpsolver/feasibility_highs.hpp"

static QpAsmStatus quass2highs(Instance& instance, Settings& settings,
                               Statistics& stats,
                               QpModelStatus& qp_model_status,
                               QpSolution& qp_solution,
                               HighsModelStatus& highs_model_status,
                               HighsBasis& highs_basis,
                               HighsSolution& highs_solution) {
  settings.qp_model_status_log.fire(qp_model_status);
  QpAsmStatus qp_asm_return_status = QpAsmStatus::kError;
  switch (qp_model_status) {
    case QpModelStatus::kOptimal:
      highs_model_status = HighsModelStatus::kOptimal;
      qp_asm_return_status = QpAsmStatus::kOk;
      break;
    case QpModelStatus::kUnbounded:
      highs_model_status = HighsModelStatus::kUnbounded;
      qp_asm_return_status = QpAsmStatus::kOk;
      break;
    case QpModelStatus::kInfeasible:
      highs_model_status = HighsModelStatus::kInfeasible;
      qp_asm_return_status = QpAsmStatus::kOk;
      break;
    case QpModelStatus::kIterationLimit:
      highs_model_status = HighsModelStatus::kIterationLimit;
      qp_asm_return_status = QpAsmStatus::kWarning;
      break;
    case QpModelStatus::kTimeLimit:
      highs_model_status = HighsModelStatus::kTimeLimit;
      qp_asm_return_status = QpAsmStatus::kWarning;
      break;
    case QpModelStatus::kInterrupt:
      highs_model_status = HighsModelStatus::kInterrupt;
      qp_asm_return_status = QpAsmStatus::kWarning;
      break;
    case QpModelStatus::kUndetermined:
      highs_model_status = HighsModelStatus::kSolveError;
      qp_asm_return_status = QpAsmStatus::kError;
      return QpAsmStatus::kError;
    case QpModelStatus::kLargeNullspace:
      highs_model_status = HighsModelStatus::kSolveError;
      return QpAsmStatus::kError;
    case QpModelStatus::kError:
      highs_model_status = HighsModelStatus::kSolveError;
      return QpAsmStatus::kError;
    case QpModelStatus::kNotset:
      highs_model_status = HighsModelStatus::kNotset;
      return QpAsmStatus::kError;
    default:
      highs_model_status = HighsModelStatus::kNotset;
      return QpAsmStatus::kError;
  }

  assert(qp_asm_return_status != QpAsmStatus::kError);
  // extract variable values
  highs_solution.col_value.resize(instance.num_var);
  highs_solution.col_dual.resize(instance.num_var);
  for (HighsInt iCol = 0; iCol < instance.num_var; iCol++) {
    highs_solution.col_value[iCol] = qp_solution.primal.value[iCol];
    highs_solution.col_dual[iCol] =
        instance.sense * qp_solution.dualvar.value[iCol];
  }
  // extract constraint activity
  highs_solution.row_value.resize(instance.num_con);
  highs_solution.row_dual.resize(instance.num_con);
  // Negate the vector and Hessian
  for (HighsInt iRow = 0; iRow < instance.num_con; iRow++) {
    highs_solution.row_value[iRow] = qp_solution.rowactivity.value[iRow];
    highs_solution.row_dual[iRow] =
        instance.sense * qp_solution.dualcon.value[iRow];
  }
  highs_solution.value_valid = true;
  highs_solution.dual_valid = true;

  // extract basis status
  highs_basis.col_status.resize(instance.num_var);
  highs_basis.row_status.resize(instance.num_con);

  const bool debug_report =
      false;  // instance.num_var + instance.num_con < 100;
  for (HighsInt i = 0; i < instance.num_var; i++) {
    if (debug_report)
      printf("Column %2d: status %s\n", int(i),
             qpBasisStatusToString(qp_solution.status_var[i]).c_str());
    if (qp_solution.status_var[i] == BasisStatus::kActiveAtLower) {
      highs_basis.col_status[i] = HighsBasisStatus::kLower;
    } else if (qp_solution.status_var[i] == BasisStatus::kActiveAtUpper) {
      highs_basis.col_status[i] = HighsBasisStatus::kUpper;
    } else if (qp_solution.status_var[i] == BasisStatus::kInactiveInBasis) {
      highs_basis.col_status[i] = HighsBasisStatus::kNonbasic;
    } else {
      assert(qp_solution.status_var[i] == BasisStatus::kInactive);
      highs_basis.col_status[i] = HighsBasisStatus::kBasic;
    }
  }

  for (HighsInt i = 0; i < instance.num_con; i++) {
    if (debug_report)
      printf("Row    %2d: status %s\n", int(i),
             qpBasisStatusToString(qp_solution.status_con[i]).c_str());
    if (qp_solution.status_con[i] == BasisStatus::kActiveAtLower) {
      highs_basis.row_status[i] = HighsBasisStatus::kLower;
    } else if (qp_solution.status_con[i] == BasisStatus::kActiveAtUpper) {
      highs_basis.row_status[i] = HighsBasisStatus::kUpper;
    } else if (qp_solution.status_con[i] == BasisStatus::kInactiveInBasis) {
      highs_basis.row_status[i] = HighsBasisStatus::kNonbasic;
    } else {
      assert(qp_solution.status_con[i] == BasisStatus::kInactive);
      highs_basis.row_status[i] = HighsBasisStatus::kBasic;
    }
  }
  highs_basis.valid = true;
  highs_basis.alien = false;
  return qp_asm_return_status;
}

QpAsmStatus solveqp(Instance& instance, Settings& settings, Statistics& stats,
                    HighsModelStatus& highs_model_status,
                    HighsBasis& highs_basis, HighsSolution& highs_solution,
                    HighsTimer& qp_timer) {
  QpModelStatus qp_model_status = QpModelStatus::kUndetermined;

  QpSolution qp_solution(instance);

  // presolve

  // scale instance, store scaling factors

  // perturb instance, store perturbance information

  // regularize
  for (HighsInt i = 0; i < instance.num_var; i++) {
    for (HighsInt index = instance.Q.mat.start[i];
         index < instance.Q.mat.start[i + 1]; index++) {
      if (instance.Q.mat.index[index] == i) {
        instance.Q.mat.value[index] += settings.hessianregularizationfactor;
      }
    }
  }

  // compute initial feasible point
  QpHotstartInformation startinfo(instance.num_var, instance.num_con);
  if (instance.num_con == 0 && instance.num_var <= 15000) {
    computeStartingPointBounded(instance, settings, stats, qp_model_status,
                                startinfo, qp_timer);
    if (qp_model_status == QpModelStatus::kOptimal) {
      qp_solution.primal = startinfo.primal;
      return quass2highs(instance, settings, stats, qp_model_status,
                         qp_solution, highs_model_status, highs_basis,
                         highs_solution);
    }
    if (qp_model_status == QpModelStatus::kUnbounded) {
      return quass2highs(instance, settings, stats, qp_model_status,
                         qp_solution, highs_model_status, highs_basis,
                         highs_solution);
    }
  } else {
    computeStartingPointHighs(instance, settings, stats, qp_model_status,
                              startinfo, highs_model_status, highs_basis,
                              highs_solution, qp_timer);
    if (qp_model_status != QpModelStatus::kNotset) {
      return quass2highs(instance, settings, stats, qp_model_status,
                         qp_solution, highs_model_status, highs_basis,
                         highs_solution);
    }
  }

  // solve
  solveqp_actual(instance, settings, startinfo, stats, qp_model_status,
                 qp_solution, qp_timer);

  // undo perturbation and resolve

  // undo scaling and resolve

  // postsolve

  // Transform QP status and qp_solution to HiGHS highs_basis and highs_solution
  return quass2highs(instance, settings, stats, qp_model_status, qp_solution,
                     highs_model_status, highs_basis, highs_solution);
}
