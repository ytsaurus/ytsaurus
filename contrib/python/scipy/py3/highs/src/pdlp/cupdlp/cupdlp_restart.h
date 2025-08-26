//
// Created by chuwen on 23-11-28.
//

#ifndef CUPDLP_CUPDLP_RESTART_H
#define CUPDLP_CUPDLP_RESTART_H

#include "cupdlp_defs.h"
#include "cupdlp_linalg.h"
#include "cupdlp_proj.h"
// #include "cupdlp_scaling.h"
#include "cupdlp_step.h"
#include "cupdlp_utils.h"
#include "glbopts.h"

typedef enum {
  PDHG_NO_RESTART = 0,
  PDHG_RESTART_TO_CURRENT,
  PDHG_RESTART_TO_AVERAGE
} PDHG_restart_choice;

cupdlp_bool PDHG_Check_Restart_Merit_Function(CUPDLPwork *work);

PDHG_restart_choice PDHG_Check_Restart_GPU(CUPDLPwork *work);

cupdlp_float PDHG_Restart_Score_GPU(cupdlp_float weightSquared,
                                    cupdlp_float dPrimalFeas,
                                    cupdlp_float dDualFeas,
                                    cupdlp_float dDualityGap);

#endif  // CUPDLP_CUPDLP_RESTART_H
