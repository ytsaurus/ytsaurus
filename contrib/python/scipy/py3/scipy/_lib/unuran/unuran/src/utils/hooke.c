#include <unur_source.h>
#include "hooke_source.h"


/*
This implementation was removed due to compliance issues.
See gh-13: https://github.com/scipy/unuran/issues/13
*/
int _unur_hooke(struct unur_funct_vgeneric faux, 
           int dim, double *startpt, double *endpt, 
           double rho, double epsilon, long itermax)
{
	return 0;  // No-Op
}
