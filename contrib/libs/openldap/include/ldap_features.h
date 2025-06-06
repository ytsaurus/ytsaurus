/* include/ldap_features.h.  Generated from ldap_features.hin by configure.  */
/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2024 The OpenLDAP Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

/* 
 * LDAP Features
 */

#ifndef _LDAP_FEATURES_H
#define _LDAP_FEATURES_H 1

/* OpenLDAP API version macros */
#define LDAP_VENDOR_VERSION 20610
#define LDAP_VENDOR_VERSION_MAJOR 2
#define LDAP_VENDOR_VERSION_MINOR 6
#define LDAP_VENDOR_VERSION_PATCH 10

/*
** WORK IN PROGRESS!
**
** OpenLDAP reentrancy/thread-safeness should be dynamically
** checked using ldap_get_option().
**
** If built with thread support, the -lldap implementation is:
**		LDAP_API_FEATURE_THREAD_SAFE (basic thread safety)
**		LDAP_API_FEATURE_SESSION_THREAD_SAFE
**		LDAP_API_FEATURE_OPERATION_THREAD_SAFE
**
** The preprocessor flag LDAP_API_FEATURE_X_OPENLDAP_THREAD_SAFE
** can be used to determine if -lldap is thread safe at compile
** time.
**
*/

/* is -lldap reentrant or not */
#define LDAP_API_FEATURE_X_OPENLDAP_REENTRANT 1

/* is -lldap thread safe or not */
#define LDAP_API_FEATURE_X_OPENLDAP_THREAD_SAFE 1

/* LDAP v2 Referrals */
/* #undef LDAP_API_FEATURE_X_OPENLDAP_V2_REFERRALS */

#endif /* LDAP_FEATURES */
