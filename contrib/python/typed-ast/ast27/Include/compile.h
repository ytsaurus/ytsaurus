
#ifndef Ta27_COMPILE_H
#define Ta27_COMPILE_H

#include "Python.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Public interface */
PyAPI_FUNC(PyFutureFeatures *) PyFuture_FromAST(struct _mod *, const char *);


#ifdef __cplusplus
}
#endif
#endif /* !Ta27_COMPILE_H */
