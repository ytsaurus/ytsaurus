//
// Created by chuwen on 23-11-28.
//

#include "cupdlp_step.h"

#include "cupdlp_defs.h"
#include "cupdlp_linalg.h"
#include "cupdlp_proj.h"
// #include "cupdlp_scaling.h"
#include "cupdlp_solver.h"
#include "cupdlp_utils.h"
#include "glbopts.h"

// xUpdate = x^k - dPrimalStep * (c - A'y^k)
void PDHG_primalGradientStep(CUPDLPwork *work, cupdlp_float dPrimalStepSize) {
  CUPDLPiterates *iterates = work->iterates;
  CUPDLPproblem *problem = work->problem;

#if !defined(CUPDLP_CPU) & USE_KERNELS
  cupdlp_pgrad_cuda(iterates->xUpdate->data, iterates->x->data, problem->cost,
                    iterates->aty->data, dPrimalStepSize, problem->nCols);
#else

  // cupdlp_copy(iterates->xUpdate, iterates->x, cupdlp_float, problem->nCols);
  CUPDLP_COPY_VEC(iterates->xUpdate->data, iterates->x->data, cupdlp_float,
                  problem->nCols);

  // AddToVector(iterates->xUpdate, -dPrimalStepSize, problem->cost,
  // problem->nCols); AddToVector(iterates->xUpdate, dPrimalStepSize,
  // iterates->aty, problem->nCols);

  cupdlp_float alpha = -dPrimalStepSize;
  cupdlp_axpy(work, problem->nCols, &alpha, problem->cost,
              iterates->xUpdate->data);
  alpha = dPrimalStepSize;
  cupdlp_axpy(work, problem->nCols, &alpha, iterates->aty->data,
              iterates->xUpdate->data);
#endif
}

// yUpdate = y^k + dDualStep * (b - A * (2x^{k+1} - x^{k})
void PDHG_dualGradientStep(CUPDLPwork *work, cupdlp_float dDualStepSize) {
  CUPDLPiterates *iterates = work->iterates;
  CUPDLPproblem *problem = work->problem;

#if !defined(CUPDLP_CPU) & USE_KERNELS
  cupdlp_dgrad_cuda(iterates->yUpdate->data, iterates->y->data, problem->rhs,
                    iterates->ax->data, iterates->axUpdate->data, dDualStepSize,
                    problem->nRows);
#else

  // cupdlp_copy(iterates->yUpdate, iterates->y, cupdlp_float, problem->nRows);
  CUPDLP_COPY_VEC(iterates->yUpdate->data, iterates->y->data, cupdlp_float,
                  problem->nRows);

  // AddToVector(iterates->yUpdate, dDualStepSize, problem->rhs,
  // problem->nRows); AddToVector(iterates->yUpdate, -2.0 * dDualStepSize,
  // iterates->axUpdate, problem->nRows); AddToVector(iterates->yUpdate,
  // dDualStepSize, iterates->ax, problem->nRows);

  cupdlp_float alpha = dDualStepSize;
  cupdlp_axpy(work, problem->nRows, &alpha, problem->rhs,
              iterates->yUpdate->data);
  alpha = -2.0 * dDualStepSize;
  cupdlp_axpy(work, problem->nRows, &alpha, iterates->axUpdate->data,
              iterates->yUpdate->data);
  alpha = dDualStepSize;
  cupdlp_axpy(work, problem->nRows, &alpha, iterates->ax->data,
              iterates->yUpdate->data);
#endif
}

cupdlp_retcode PDHG_Power_Method(CUPDLPwork *work, cupdlp_float *lambda) {
  cupdlp_retcode retcode = RETCODE_OK;
  CUPDLPproblem *problem = work->problem;
  CUPDLPdata *lp = problem->data;
  CUPDLPiterates *iterates = work->iterates;

  if (work->settings->nLogLevel>0) 
    cupdlp_printf("Power Method:\n");

  cupdlp_float *q = work->buffer->data;

  cupdlp_initvec(q, 1.0, lp->nRows);

  double res = 0.0;
  for (cupdlp_int iter = 0; iter < 20; ++iter) {
    // z = A*A'*q
    ATy(work, iterates->aty, work->buffer);
    Ax(work, iterates->ax, iterates->aty);

    // q = z / norm(z)
    CUPDLP_COPY_VEC(q, iterates->ax->data, cupdlp_float, lp->nRows);
    cupdlp_float qNorm = 0.0;
    cupdlp_twoNorm(work, lp->nRows, q, &qNorm);
    cupdlp_scaleVector(work, 1.0 / qNorm, q, lp->nRows);

    ATy(work, iterates->aty, work->buffer);

    cupdlp_twoNormSquared(work, lp->nCols, iterates->aty->data, lambda);

    cupdlp_float alpha = -(*lambda);
    cupdlp_axpy(work, lp->nRows, &alpha, q, iterates->ax->data);

    cupdlp_twoNormSquared(work, lp->nCols, iterates->ax->data, &res);

    if (work->settings->nLogLevel>0) 
      cupdlp_printf("% d  %e  %.3f\n", iter, *lambda, res);
  }

exit_cleanup:
  return retcode;
}

void PDHG_Compute_Step_Size_Ratio(CUPDLPwork *pdhg) {
  CUPDLPproblem *problem = pdhg->problem;
  CUPDLPiterates *iterates = pdhg->iterates;
  CUPDLPstepsize *stepsize = pdhg->stepsize;
  cupdlp_float dMeanStepSize =
      sqrt(stepsize->dPrimalStep * stepsize->dDualStep);

  // cupdlp_float dDiffPrimal = cupdlp_diffTwoNorm(iterates->x,
  // iterates->xLastRestart, problem->nCols); cupdlp_float dDiffDual =
  // cupdlp_diffTwoNorm(iterates->y, iterates->yLastRestart, problem->nRows);

  cupdlp_float dDiffPrimal = 0.0;
  cupdlp_diffTwoNorm(pdhg, iterates->x->data, iterates->xLastRestart,
                     problem->nCols, &dDiffPrimal);
  cupdlp_float dDiffDual = 0.0;
  cupdlp_diffTwoNorm(pdhg, iterates->y->data, iterates->yLastRestart,
                     problem->nRows, &dDiffDual);

  if (fmin(dDiffPrimal, dDiffDual) > 1e-10) {
    cupdlp_float dBetaUpdate = dDiffDual / dDiffPrimal;
    cupdlp_float dLogBetaUpdate =
        0.5 * log(dBetaUpdate) + 0.5 * log(sqrt(stepsize->dBeta));
    stepsize->dBeta = exp(dLogBetaUpdate) * exp(dLogBetaUpdate);
  }

  stepsize->dPrimalStep = dMeanStepSize / sqrt(stepsize->dBeta);
  stepsize->dDualStep = stepsize->dPrimalStep * stepsize->dBeta;
  stepsize->dTheta = 1.0;
}

void PDHG_Update_Iterate_Constant_Step_Size(CUPDLPwork *pdhg) {
  //            CUPDLP_ASSERT(0);
  CUPDLPproblem *problem = pdhg->problem;
  CUPDLPiterates *iterates = pdhg->iterates;
  CUPDLPstepsize *stepsize = pdhg->stepsize;

  // Ax(pdhg, iterates->ax, iterates->x);
  // ATyCPU(pdhg, iterates->aty, iterates->y);
  Ax(pdhg, iterates->ax, iterates->x);
  ATy(pdhg, iterates->aty, iterates->y);

  // x^{k+1} = proj_{X}(x^k - dPrimalStep * (c - A'y^k))
  PDHG_primalGradientStep(pdhg, stepsize->dPrimalStep);

  PDHG_Project_Bounds(pdhg, iterates->xUpdate->data);
  // Ax(pdhg, iterates->axUpdate, iterates->xUpdate);
  Ax(pdhg, iterates->axUpdate, iterates->xUpdate);

  // y^{k+1} = y^k + dDualStep * (b - A * (2x^{k+1} - x^{k})
  PDHG_dualGradientStep(pdhg, stepsize->dDualStep);

  PDHG_Project_Row_Duals(pdhg, iterates->yUpdate->data);
  // ATyCPU(pdhg, iterates->atyUpdate, iterates->yUpdate);
  ATy(pdhg, iterates->atyUpdate, iterates->yUpdate);
}

void PDHG_Update_Iterate_Malitsky_Pock(CUPDLPwork *pdhg) {
  cupdlp_printf("Malitsky-Pock is not implemented\n");
  cupdlp_printf(" - use %d and %d instead", PDHG_FIXED_LINESEARCH,
                PDHG_ADAPTIVE_LINESEARCH);
  exit(-1);
}

cupdlp_retcode PDHG_Update_Iterate_Adaptive_Step_Size(CUPDLPwork *pdhg) {
  cupdlp_retcode retcode = RETCODE_OK;
  CUPDLPproblem *problem = pdhg->problem;
  CUPDLPiterates *iterates = pdhg->iterates;
  CUPDLPstepsize *stepsize = pdhg->stepsize;

  cupdlp_float dStepSizeUpdate =
      sqrt(stepsize->dPrimalStep * stepsize->dDualStep);

  cupdlp_bool isDone = false;
  // number of steps this round
  int stepIterThis = 0;
  while (!isDone) {
    ++stepsize->nStepSizeIter;
    ++stepIterThis;

    cupdlp_float dPrimalStepUpdate = dStepSizeUpdate / sqrt(stepsize->dBeta);
    cupdlp_float dDualStepUpdate = dStepSizeUpdate * sqrt(stepsize->dBeta);

    // x^{k+1} = proj_{X}(x^k - dPrimalStep * (cupdlp - A'y^k))
    PDHG_primalGradientStep(pdhg, dPrimalStepUpdate);

    PDHG_Project_Bounds(pdhg, iterates->xUpdate->data);
    Ax(pdhg, iterates->axUpdate, iterates->xUpdate);

    // y^{k+1} = proj_{Y}(y^k + dDualStep * (b - A * (2 * x^{k+1} - x^{k})))
    PDHG_dualGradientStep(pdhg, dDualStepUpdate);

    PDHG_Project_Row_Duals(pdhg, iterates->yUpdate->data);
    ATy(pdhg, iterates->atyUpdate, iterates->yUpdate);

    cupdlp_float dMovement = 0.0;
    cupdlp_float dInteraction = 0.0;

#if !defined(CUPDLP_CPU) & USE_KERNELS
    cupdlp_compute_interaction_and_movement(pdhg, &dMovement, &dInteraction);
#else
    cupdlp_float dX = 0.0;
    cupdlp_diffTwoNormSquared(pdhg, iterates->x->data, iterates->xUpdate->data,
                              problem->nCols, &dX);
    dX *= 0.5 * sqrt(stepsize->dBeta);

    cupdlp_float dY = 0.0;
    cupdlp_diffTwoNormSquared(pdhg, iterates->y->data, iterates->yUpdate->data,
                              problem->nRows, &dY);
    dY /= 2.0 * sqrt(stepsize->dBeta);
    dMovement = dX + dY;

    //      \Deltax' (A\Deltay)
    cupdlp_diffDotDiff(pdhg, iterates->x->data, iterates->xUpdate->data,
                       iterates->aty->data, iterates->atyUpdate->data,
                       problem->nCols, &dInteraction);
#endif

#if CUPDLP_DUMP_LINESEARCH_STATS & CUPDLP_DEBUG
    cupdlp_float dInteractiony = 0.0;
    //      \Deltay' (A\Deltax)
    cupdlp_diffDotDiff(pdhg, iterates->y->data, iterates->yUpdate->data,
                       iterates->ax->data, iterates->axUpdate->data,
                       problem->nRows, &dInteractiony);
#endif

    cupdlp_float dStepSizeLimit;
    if (dInteraction != 0.0) {
      dStepSizeLimit = dMovement / fabs(dInteraction);
    } else {
      dStepSizeLimit = INFINITY;
    }
    if (dStepSizeUpdate <= dStepSizeLimit) {
      isDone = true;
      // break;
    } else {
      CUPDLP_CHECK_TIMEOUT(pdhg);
    }

    cupdlp_float dFirstTerm = (1.0 - pow(stepsize->nStepSizeIter + 1.0,
                                         -PDHG_STEPSIZE_REDUCTION_EXP)) *
                              dStepSizeLimit;
    cupdlp_float dSecondTerm =
        (1.0 + pow(stepsize->nStepSizeIter + 1.0, -PDHG_STEPSIZE_GROWTH_EXP)) *
        dStepSizeUpdate;
    dStepSizeUpdate = fmin(dFirstTerm, dSecondTerm);
#if CUPDLP_DUMP_LINESEARCH_STATS & CUPDLP_DEBUG
    cupdlp_printf(" -- stepsize iteration %d: %f %f\n", stepIterThis,
                  dStepSizeUpdate, dStepSizeLimit);

    cupdlp_printf(" -- PrimalStep DualStep: %f %f\n", stepsize->dPrimalStep,
                  stepsize->dDualStep);
    cupdlp_printf(" -- FirstTerm SecondTerm: %f %f\n", dFirstTerm, dSecondTerm);
    cupdlp_printf(" -- nStepSizeIter: %d\n", stepsize->nStepSizeIter);
    cupdlp_printf(" -- RED_EXP GRO_EXP: %f %f\n", PDHG_STEPSIZE_REDUCTION_EXP,
                  PDHG_STEPSIZE_GROWTH_EXP);

    cupdlp_printf("     -- iteraction(x) interaction(y): %f %f\n", dInteraction,
                  dInteractiony);
    cupdlp_printf("     -- movement (scaled norm)  : %f\n", dMovement);
    cupdlp_printf("     -- movement (scaled norm)  : %f\n", dMovement);
    if (stepIterThis > 200) break;  // avoid unlimited runs due to bugs.
#endif
  }

  stepsize->dPrimalStep = dStepSizeUpdate / sqrt(stepsize->dBeta);
  stepsize->dDualStep = dStepSizeUpdate * sqrt(stepsize->dBeta);

exit_cleanup:
  return retcode;
}

cupdlp_retcode PDHG_Init_Step_Sizes(CUPDLPwork *pdhg) {
  cupdlp_retcode retcode = RETCODE_OK;

  CUPDLPproblem *problem = pdhg->problem;
  CUPDLPiterates *iterates = pdhg->iterates;
  CUPDLPstepsize *stepsize = pdhg->stepsize;

  if (stepsize->eLineSearchMethod == PDHG_FIXED_LINESEARCH) {
    CUPDLP_CALL(PDHG_Power_Method(pdhg, &stepsize->dPrimalStep));
    // PDLP Intial primal weight = norm(cost) / norm(rhs) = sqrt(beta)
    // cupdlp_float a = twoNormSquared(problem->cost, problem->nCols);
    // cupdlp_float b = twoNormSquared(problem->rhs, problem->nRows);
    cupdlp_float a = 0.0;
    cupdlp_float b = 0.0;
    cupdlp_twoNormSquared(pdhg, problem->nCols, problem->cost, &a);
    cupdlp_twoNormSquared(pdhg, problem->nRows, problem->rhs, &b);

    if (fmin(a, b) > 1e-6) {
      stepsize->dBeta = a / b;
    } else {
      stepsize->dBeta = 1.0;
    }

    stepsize->dPrimalStep = 0.8 / sqrt(stepsize->dPrimalStep);
    stepsize->dDualStep = stepsize->dPrimalStep;
    stepsize->dPrimalStep /= sqrt(stepsize->dBeta);
    stepsize->dDualStep *= sqrt(stepsize->dBeta);
  } else {
    stepsize->dTheta = 1.0;

    // PDLP Intial primal weight = norm(cost) / norm(rhs) = sqrt(beta)
    // cupdlp_float a = twoNormSquared(problem->cost, problem->nCols);
    // cupdlp_float b = twoNormSquared(problem->rhs, problem->nRows);
    cupdlp_float a = 0.0;
    cupdlp_float b = 0.0;
    cupdlp_twoNormSquared(pdhg, problem->nCols, problem->cost, &a);
    cupdlp_twoNormSquared(pdhg, problem->nRows, problem->rhs, &b);

    if (fmin(a, b) > 1e-6) {
      stepsize->dBeta = a / b;
    } else {
      stepsize->dBeta = 1.0;
    }
    // infNorm can be avoid by previously calculated infNorm of csc matrix
    stepsize->dPrimalStep =
        // (1.0 / infNorm(problem->data->csc_matrix->colMatElem,
        // problem->data->csc_matrix->nMatElem)) /
        (1.0 / problem->data->csc_matrix->MatElemNormInf) /
        sqrt(stepsize->dBeta);
    stepsize->dDualStep = stepsize->dPrimalStep * stepsize->dBeta;
    iterates->dLastRestartBeta = stepsize->dBeta;
  }

  iterates->iLastRestartIter = 0;
  stepsize->dSumPrimalStep = 0;
  stepsize->dSumDualStep = 0;

exit_cleanup:
  return retcode;
}

void PDHG_Compute_Average_Iterate(CUPDLPwork *work) {
  CUPDLPproblem *problem = work->problem;
  CUPDLPdata *lp = problem->data;
  CUPDLPstepsize *stepsize = work->stepsize;
  CUPDLPiterates *iterates = work->iterates;

  cupdlp_float dPrimalScale =
      stepsize->dSumPrimalStep > 0.0 ? 1.0 / stepsize->dSumPrimalStep : 1.0;
  cupdlp_float dDualScale =
      stepsize->dSumDualStep > 0.0 ? 1.0 / stepsize->dSumDualStep : 1.0;

  // cupdlp_scaleVector(iterates->xAverage, iterates->xSum, dPrimalScale,
  // lp->nCols); cupdlp_scaleVector(iterates->yAverage, iterates->ySum,
  // dDualScale, lp->nRows);

  CUPDLP_COPY_VEC(iterates->xAverage->data, iterates->xSum, cupdlp_float,
                  lp->nCols);
  CUPDLP_COPY_VEC(iterates->yAverage->data, iterates->ySum, cupdlp_float,
                  lp->nRows);
  cupdlp_scaleVector(work, dPrimalScale, iterates->xAverage->data, lp->nCols);
  cupdlp_scaleVector(work, dDualScale, iterates->yAverage->data, lp->nRows);

  // Ax(work, iterates->axAverage, iterates->xAverage);
  // ATyCPU(work, iterates->atyAverage, iterates->yAverage);
  Ax(work, iterates->axAverage, iterates->xAverage);
  ATy(work, iterates->atyAverage, iterates->yAverage);
}

void PDHG_Update_Average(CUPDLPwork *work) {
  CUPDLPproblem *problem = work->problem;
  CUPDLPdata *lp = problem->data;
  CUPDLPstepsize *stepsize = work->stepsize;
  CUPDLPiterates *iterates = work->iterates;

  // PDLP weighs average iterates in this way
  cupdlp_float dMeanStepSize =
      sqrt(stepsize->dPrimalStep * stepsize->dDualStep);
  // AddToVector(iterates->xSum, dMeanStepSize, iterates->xUpdate,
  // lp->nCols); AddToVector(iterates->ySum, dMeanStepSize,
  // iterates->yUpdate, lp->nRows);
  cupdlp_axpy(work, lp->nCols, &dMeanStepSize, iterates->xUpdate->data,
              iterates->xSum);
  cupdlp_axpy(work, lp->nRows, &dMeanStepSize, iterates->yUpdate->data,
              iterates->ySum);

  stepsize->dSumPrimalStep += dMeanStepSize;
  stepsize->dSumDualStep += dMeanStepSize;
}

cupdlp_retcode PDHG_Update_Iterate(CUPDLPwork *pdhg) {
  cupdlp_retcode retcode = RETCODE_OK;

#if PDHG_USE_TIMERS
  CUPDLPtimers *timers = pdhg->timers;
  ++timers->nUpdateIterateCalls;
  cupdlp_float dStartTime = getTimeStamp();
#endif

  CUPDLPproblem *problem = pdhg->problem;
  CUPDLPstepsize *stepsize = pdhg->stepsize;
  CUPDLPiterates *iterates = pdhg->iterates;

  switch (stepsize->eLineSearchMethod) {
    case PDHG_FIXED_LINESEARCH:
      PDHG_Update_Iterate_Constant_Step_Size(pdhg);
      break;
    case PDHG_MALITSKY_POCK_LINESEARCH:
      PDHG_Update_Iterate_Malitsky_Pock(pdhg);
      break;
    case PDHG_ADAPTIVE_LINESEARCH:
      CUPDLP_CALL(PDHG_Update_Iterate_Adaptive_Step_Size(pdhg));
      break;
  }

  PDHG_Update_Average(pdhg);

  CUPDLP_COPY_VEC(iterates->x->data, iterates->xUpdate->data, cupdlp_float,
                  problem->nCols);
  CUPDLP_COPY_VEC(iterates->y->data, iterates->yUpdate->data, cupdlp_float,
                  problem->nRows);
  CUPDLP_COPY_VEC(iterates->ax->data, iterates->axUpdate->data, cupdlp_float,
                  problem->nRows);
  CUPDLP_COPY_VEC(iterates->aty->data, iterates->atyUpdate->data, cupdlp_float,
                  problem->nCols);

#if PDHG_USE_TIMERS
  timers->dUpdateIterateTime += getTimeStamp() - dStartTime;
#endif

exit_cleanup:
  return RETCODE_OK;
}
