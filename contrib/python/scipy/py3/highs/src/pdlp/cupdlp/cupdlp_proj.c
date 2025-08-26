//
// Created by chuwen on 23-11-28.
//

#include "cupdlp_proj.h"

#include "cupdlp_defs.h"
#include "cupdlp_linalg.h"
#include "cupdlp_restart.h"
// #include "cupdlp_scaling.h"
#include "cupdlp_solver.h"
#include "cupdlp_step.h"
#include "cupdlp_utils.h"
#include "glbopts.h"

// primal projection: project x to [lower, upper]
void PDHG_Project_Bounds(CUPDLPwork *work, cupdlp_float *r) {
  CUPDLPproblem *problem = work->problem;

  // cupdlp_projUpperBound(r, r, problem->upper, problem->nCols);
  // cupdlp_projLowerBound(r, r, problem->lower, problem->nCols);

  cupdlp_projub(r, problem->upper, problem->nCols);
  cupdlp_projlb(r, problem->lower, problem->nCols);
}

void PDHG_Project_Row_Duals(CUPDLPwork *work, cupdlp_float *r) {
  CUPDLPproblem *problem = work->problem;

  // cupdlp_projPositive(r + problem->nEqs, r + problem->nEqs, problem->nRows -
  // problem->nEqs);
  cupdlp_projPos(r + problem->nEqs, problem->nRows - problem->nEqs);
}

// void PDHG_Restart_Iterate(CUPDLPwork *pdhg)
// {
//     CUPDLPproblem *problem = pdhg->problem;
//     CUPDLPiterates *iterates = pdhg->iterates;
//     CUPDLPstepsize *stepsize = pdhg->stepsize;
//     CUPDLPtimers *timers = pdhg->timers;

//     // PDHG_Compute_Average_Iterate(pdhg);
//     PDHG_restart_choice restart_choice = PDHG_Check_Restart(pdhg);

//     if (restart_choice == PDHG_NO_RESTART)
//         return;

//     PDHG_Compute_Step_Size_Ratio(pdhg);

//     stepsize->dSumPrimalStep = 0.0;
//     stepsize->dSumDualStep = 0.0;
//     cupdlp_zero(iterates->xSum, cupdlp_float, problem->nCols);
//     cupdlp_zero(iterates->ySum, cupdlp_float, problem->nRows);

//     if (restart_choice == PDHG_RESTART_TO_AVERAGE)
//     {
//         cupdlp_copy(iterates->x, iterates->xAverage, cupdlp_float,
//         problem->nCols); cupdlp_copy(iterates->y, iterates->yAverage,
//         cupdlp_float, problem->nRows); cupdlp_copy(iterates->ax,
//         iterates->axAverage, cupdlp_float, problem->nRows);
//         cupdlp_copy(iterates->aty, iterates->atyAverage, cupdlp_float,
//         problem->nCols);
//     }
//     cupdlp_copy(iterates->xLastRestart, iterates->x, cupdlp_float,
//     problem->nCols); cupdlp_copy(iterates->yLastRestart, iterates->y,
//     cupdlp_float, problem->nRows);

//     iterates->iLastRestartIter = timers->nIter;

//     PDHG_Compute_Residuals(pdhg);
//     // cupdlp_printf("Recomputed stepsize ratio: %e,  sqrt(ratio)=%e",
//     stepsize->dBeta, sqrt(stepsize->dBeta));
// }

void PDHG_Restart_Iterate(CUPDLPwork *pdhg) {
  switch (pdhg->settings->eRestartMethod) {
    case PDHG_WITHOUT_RESTART:
      break;
    case PDHG_GPU_RESTART:
      PDHG_Restart_Iterate_GPU(pdhg);
      break;
    case PDHG_CPU_RESTART:
      // TODO: implement PDHG_Restart_Iterate_CPU(pdhg);
      break;
  }
}

void PDHG_Restart_Iterate_GPU(CUPDLPwork *pdhg) {
  CUPDLPproblem *problem = pdhg->problem;
  CUPDLPiterates *iterates = pdhg->iterates;
  CUPDLPstepsize *stepsize = pdhg->stepsize;
  CUPDLPresobj *resobj = pdhg->resobj;
  CUPDLPtimers *timers = pdhg->timers;

  // PDHG_Compute_Average_Iterate(pdhg);
  PDHG_restart_choice restart_choice = PDHG_Check_Restart_GPU(pdhg);

  if (restart_choice == PDHG_NO_RESTART) return;

  stepsize->dSumPrimalStep = 0.0;
  stepsize->dSumDualStep = 0.0;
  CUPDLP_ZERO_VEC(iterates->xSum, cupdlp_float, problem->nCols);
  CUPDLP_ZERO_VEC(iterates->ySum, cupdlp_float, problem->nRows);

  if (restart_choice == PDHG_RESTART_TO_AVERAGE) {
    resobj->dPrimalFeasLastRestart = resobj->dPrimalFeasAverage;
    resobj->dDualFeasLastRestart = resobj->dDualFeasAverage;
    resobj->dDualityGapLastRestart = resobj->dDualityGapAverage;

    // cupdlp_copy(iterates->x, iterates->xAverage, cupdlp_float,
    // problem->nCols); cupdlp_copy(iterates->y, iterates->yAverage,
    // cupdlp_float, problem->nRows); cupdlp_copy(iterates->ax,
    // iterates->axAverage, cupdlp_float, problem->nRows);
    // cupdlp_copy(iterates->aty, iterates->atyAverage, cupdlp_float,
    // problem->nCols);

    CUPDLP_COPY_VEC(iterates->x->data, iterates->xAverage->data, cupdlp_float,
                    problem->nCols);
    CUPDLP_COPY_VEC(iterates->y->data, iterates->yAverage->data, cupdlp_float,
                    problem->nRows);
    CUPDLP_COPY_VEC(iterates->ax->data, iterates->axAverage->data, cupdlp_float,
                    problem->nRows);
    CUPDLP_COPY_VEC(iterates->aty->data, iterates->atyAverage->data,
                    cupdlp_float, problem->nCols);
  } else {
    resobj->dPrimalFeasLastRestart = resobj->dPrimalFeas;
    resobj->dDualFeasLastRestart = resobj->dDualFeas;
    resobj->dDualityGapLastRestart = resobj->dDualityGap;
  }

  PDHG_Compute_Step_Size_Ratio(pdhg);

  // cupdlp_copy(iterates->xLastRestart, iterates->x, cupdlp_float,
  // problem->nCols); cupdlp_copy(iterates->yLastRestart, iterates->y,
  // cupdlp_float, problem->nRows);
  CUPDLP_COPY_VEC(iterates->xLastRestart, iterates->x->data, cupdlp_float,
                  problem->nCols);
  CUPDLP_COPY_VEC(iterates->yLastRestart, iterates->y->data, cupdlp_float,
                  problem->nRows);

  iterates->iLastRestartIter = timers->nIter;

  PDHG_Compute_Residuals(pdhg);
  // cupdlp_printf("Recomputed stepsize ratio: %e,  sqrt(ratio)=%e",
  // stepsize->dBeta, sqrt(stepsize->dBeta));
}
