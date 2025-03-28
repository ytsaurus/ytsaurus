//
// JSON.h
//
// Library: JSON
// Package: JSON
// Module:  JSON
//
// Basic definitions for the Poco JSON library.
// This file must be the first file included by every other JSON
// header file.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_JSON_JSON_INCLUDED
#define DB_JSON_JSON_INCLUDED


#include "DBPoco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the JSON_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// JSON_API functions as being imported from a DLL, whereas this DLL sees symbols
// defined with this macro as being exported.
//


#if !defined(JSON_API)
#    if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined(__GNUC__) && (__GNUC__ >= 4)
#        define JSON_API __attribute__((visibility("default")))
#    else
#        define JSON_API
#    endif
#endif


//
// Automatically link JSON library.
//


#endif // DB_JSON_JSON_INCLUDED
