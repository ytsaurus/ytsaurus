#pragma once

#include <Python.h>

#include <util/generic/hash_set.h>

namespace NSJson {
    struct TLoaderOptions {
        TLoaderOptions() = default;
        bool InternKeys{};
        bool InternValues{};
        THashSet<TString> RootKeyBlackList{};
        THashSet<TString> RootKeyWhiteList{};
    };

    PyObject* LoadFromStream(PyObject* stream, const TLoaderOptions& options = {});
}
