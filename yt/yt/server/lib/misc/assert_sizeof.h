#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// A helpful macro to ensure that the size of a type does not change unexpectedly.
// Note that this is just a sanity check, which may be a no-op in some exotic configurations.
#ifdef TSTRING_IS_STD_STRING
    #define YT_STATIC_ASSERT_SIZEOF_SANITY(type, size)
#else
    #define YT_STATIC_ASSERT_SIZEOF_SANITY(type, size) \
        static_assert(sizeof(type) == size, "sizeof(" #type ") != " #size)
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
