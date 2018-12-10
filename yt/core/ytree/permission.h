#pragma once

#include "public.h"

#include <yt/core/misc/enum.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Defines permissions for various YT objects.
/*!
 *  Each permission corresponds to a unique bit of the mask.
 */
DEFINE_BIT_ENUM(EPermission,
    // Applies to: all objects
    ((Read)       (0x0001))

    // Applies to: all objects
    ((Write)      (0x0002))

    // Applies to: accounts
    ((Use)        (0x0004))

    // Applies to: all objects
    ((Administer) (0x0008))

    //! Applies to: schemas
    ((Create)     (0x0100))

    //! Applies to: all objects
    ((Remove)     (0x0200))

    //! Applies to: tables.
    ((Mount)      (0x0400))
);

//! An alias for EPermission denoting bitwise-or of atomic EPermission values.
/*!
 *  No strong type safety is provided.
 *  Use wherever it suits to distinguish permission sets from individual atomic permissions.
 */
using EPermissionSet = EPermission;

const EPermissionSet AllPermissions = EPermissionSet(0xffff);
const EPermissionSet NonePermissions = EPermissionSet(0x0000);

EPermissionSet ParsePermissions(const std::vector<TString>& items);

std::vector<TString> FormatPermissions(EPermissionSet permissions);

////////////////////////////////////////////////////////////////////////////////

//! Describes the set of objects for which permissions must be checked.
DEFINE_BIT_ENUM(EPermissionCheckScope,
    ((None)            (0x0000))
    ((This)            (0x0001))
    ((Parent)          (0x0002))
    ((Descendants)     (0x0004))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

