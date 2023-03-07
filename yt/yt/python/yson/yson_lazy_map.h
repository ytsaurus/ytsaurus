#pragma once

#include "lazy_dict.h"

#include <Extensions.hxx> // pycxx
#include <Objects.hxx> // pycxx

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TLazyYsonMapBase
{
    PyObject_HEAD
    TLazyDict* Dict;
};

PyObject* LazyYsonMapBaseSubscript(PyObject* object, PyObject* key);
PyObject* LazyYsonMapBaseHasKey(PyObject* object, PyObject* key);
PyObject* LazyYsonMapBaseGet(TLazyYsonMapBase* self, PyObject* args, PyObject* kwargs);
PyObject* LazyYsonMapBaseClear(TLazyYsonMapBase* self);
PyObject* LazyYsonMapBaseSetDefault(TLazyYsonMapBase* self, PyObject* args, PyObject* kwargs);
PyObject* LazyYsonMapBaseNew(PyTypeObject* type, PyObject* args, PyObject* kwargs);

int LazyYsonMapBaseAssSubscript(PyObject* object, PyObject* key, PyObject* value);
int LazyYsonMapBaseContains(PyObject* object, PyObject* key);
int LazyYsonMapBaseInit(TLazyYsonMapBase* self, PyObject* args, PyObject* kwargs);

Py_ssize_t LazyYsonMapBaseLength(PyObject* object);

void LazyYsonMapBaseDealloc(TLazyYsonMapBase* self);

extern PyTypeObject* TLazyYsonMapBaseType;
void InitTLazyYsonMapType();

////////////////////////////////////////////////////////////////////////////////

struct TLazyYsonMap
{
    TLazyYsonMapBase super;
    PyObject* Attributes;
};

int LazyYsonMapInit(TLazyYsonMap* self, PyObject* args, PyObject* kwargs);

PyObject* LazyYsonMapNew(PyTypeObject* type, PyObject* args, PyObject* kwargs);

void LazyYsonMapDealloc(TLazyYsonMap* self);

extern PyTypeObject* TLazyYsonMapType;

////////////////////////////////////////////////////////////////////////////////

static NPython::TPythonClassObject YsonLazyMapBaseClass;
static NPython::TPythonClassObject YsonLazyMapClass;

bool IsYsonLazyMap(PyObject* object);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
