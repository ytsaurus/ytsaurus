//
// Created by LJS on 23-11-30.
//

#ifndef CUPDLP_SCALING_CUDA_H
#define CUPDLP_SCALING_CUDA_H

#include "cupdlp_defs.h"
#include "glbopts.h"
#ifdef __cplusplus
extern "C" {
#endif

cupdlp_retcode H_PDHG_Scale_Data_cuda(cupdlp_int log_level,
				      CUPDLPcsc *csc, cupdlp_int ifScaling,
				      CUPDLPscaling *scaling, cupdlp_float *cost,
				      cupdlp_float *lower, cupdlp_float *upper,
				      cupdlp_float *rhs);

cupdlp_retcode H_Init_Scaling(cupdlp_int log_level,
			      CUPDLPscaling *scaling, cupdlp_int ncols,
			      cupdlp_int nrows, cupdlp_float *cost,
			      cupdlp_float *rhs);
  
#ifdef __cplusplus
}
#endif
#endif  // CUPDLP_CUPDLP_SCALING_H
