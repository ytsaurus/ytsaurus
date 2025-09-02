#include "npy_cblas.h"
#include "fortran_defs.h"

void BLAS_FUNC(dlarfgp)(int *n, double *alpha, double *x, int *incx, double *tau) {}
void BLAS_FUNC(dlartgp)(double *f, double *g, double *cs, double *sn, double *r) {}
void BLAS_FUNC(zlarfgp)(int *n, npy_complex128 *alpha, npy_complex128 *x, int *incx, npy_complex128 *tau) {}
