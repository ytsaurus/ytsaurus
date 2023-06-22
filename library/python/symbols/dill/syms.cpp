#define SYM(SYM_NAME) extern "C" void SYM_NAME();
SYM(PyCapsule_New)
SYM(PyCapsule_GetPointer)
SYM(PyCapsule_GetDestructor)
SYM(PyCapsule_GetContext)
SYM(PyCapsule_GetName)
SYM(PyCapsule_IsValid)
SYM(PyCapsule_SetContext)
SYM(PyCapsule_SetDestructor)
SYM(PyCapsule_SetName)
SYM(PyCapsule_SetPointer)
#undef SYM

#include <library/python/symbols/registry/syms.h>

BEGIN_SYMS("python")
SYM(PyCapsule_New)
SYM(PyCapsule_GetPointer)
SYM(PyCapsule_GetDestructor)
SYM(PyCapsule_GetContext)
SYM(PyCapsule_GetName)
SYM(PyCapsule_IsValid)
SYM(PyCapsule_SetContext)
SYM(PyCapsule_SetDestructor)
SYM(PyCapsule_SetName)
SYM(PyCapsule_SetPointer)
END_SYMS()
