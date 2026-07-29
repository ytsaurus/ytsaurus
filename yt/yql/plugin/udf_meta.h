#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

struct TUdfModuleMeta
    : public NYTree::TYsonStruct
{
    NYTree::IListNodePtr Functions;

    REGISTER_YSON_STRUCT(TUdfModuleMeta);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUdfModuleMeta)

////////////////////////////////////////////////////////////////////////////////

struct TUdfEntryMeta
    : public NYTree::TYsonStruct
{
    TString Alias;
    TString UpdatedAt;
    THashMap<TString, TIntrusivePtr<TUdfModuleMeta>> Modules;

    REGISTER_YSON_STRUCT(TUdfEntryMeta);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUdfEntryMeta)

////////////////////////////////////////////////////////////////////////////////

struct TUdfMeta
    : public NYTree::TYsonStruct
{
    THashMap<TString, TIntrusivePtr<TUdfEntryMeta>> Udfs;

    REGISTER_YSON_STRUCT(TUdfMeta);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUdfMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
