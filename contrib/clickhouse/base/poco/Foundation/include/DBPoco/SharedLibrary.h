//
// SharedLibrary.h
//
// Library: Foundation
// Package: SharedLibrary
// Module:  SharedLibrary
//
// Definition of the SharedLibrary class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_SharedLibrary_INCLUDED
#define DB_Foundation_SharedLibrary_INCLUDED


#include "DBPoco/Foundation.h"


#if defined(hpux) || defined(_hpux)
#    error #include "DBPoco/SharedLibrary_HPUX.h"
#elif defined(DB_POCO_OS_FAMILY_UNIX)
#    include "DBPoco/SharedLibrary_UNIX.h"
#endif


namespace DBPoco
{


class Foundation_API SharedLibrary : private SharedLibraryImpl
/// The SharedLibrary class dynamically
/// loads shared libraries at run-time.
{
public:
    enum Flags
    {
        SHLIB_GLOBAL = 1,
        /// On platforms that use dlopen(), use RTLD_GLOBAL. This is the default
        /// if no flags are given.
        ///
        /// This flag is ignored on platforms that do not use dlopen().

        SHLIB_LOCAL = 2
        /// On platforms that use dlopen(), use RTLD_LOCAL instead of RTLD_GLOBAL.
        ///
        /// Note that if this flag is specified, RTTI (including dynamic_cast and throw) will
        /// not work for types defined in the shared library with GCC and possibly other
        /// compilers as well. See http://gcc.gnu.org/faq.html#dso for more information.
        ///
        /// This flag is ignored on platforms that do not use dlopen().
    };

    SharedLibrary();
    /// Creates a SharedLibrary object.

    SharedLibrary(const std::string & path);
    /// Creates a SharedLibrary object and loads a library
    /// from the given path.

    SharedLibrary(const std::string & path, int flags);
    /// Creates a SharedLibrary object and loads a library
    /// from the given path, using the given flags.
    /// See the Flags enumeration for valid values.

    virtual ~SharedLibrary();
    /// Destroys the SharedLibrary. The actual library
    /// remains loaded.

    void load(const std::string & path);
    /// Loads a shared library from the given path.
    /// Throws a LibraryAlreadyLoadedException if
    /// a library has already been loaded.
    /// Throws a LibraryLoadException if the library
    /// cannot be loaded.

    void load(const std::string & path, int flags);
    /// Loads a shared library from the given path,
    /// using the given flags. See the Flags enumeration
    /// for valid values.
    /// Throws a LibraryAlreadyLoadedException if
    /// a library has already been loaded.
    /// Throws a LibraryLoadException if the library
    /// cannot be loaded.

    void unload();
    /// Unloads a shared library.

    bool isLoaded() const;
    /// Returns true iff a library has been loaded.

    bool hasSymbol(const std::string & name);
    /// Returns true iff the loaded library contains
    /// a symbol with the given name.

    void * getSymbol(const std::string & name);
    /// Returns the address of the symbol with
    /// the given name. For functions, this
    /// is the entry point of the function.
    /// Throws a NotFoundException if the symbol
    /// does not exist.

    const std::string & getPath() const;
    /// Returns the path of the library, as
    /// specified in a call to load() or the
    /// constructor.

    static std::string suffix();
    /// Returns the platform-specific filename suffix
    /// for shared libraries (including the period).
    /// In debug mode, the suffix also includes a
    /// "d" to specify the debug version of a library
    /// (e.g., "d.so", "d.dll") unless the library has
    /// been compiled with -DPOCO_NO_SHARED_LIBRARY_DEBUG_SUFFIX.

    static bool setSearchPath(const std::string & path);
    /// Adds the given path to the list of paths shared libraries
    /// are searched in.
    ///
    /// Returns true if the path was set, otherwise false.
    ///
    /// Currently only supported on Windows, where it calls
    /// SetDllDirectory(). On all other platforms, does not
    /// do anything and returns false.

private:
    SharedLibrary(const SharedLibrary &);
    SharedLibrary & operator=(const SharedLibrary &);
};


} // namespace DBPoco


#endif // DB_Foundation_SharedLibrary_INCLUDED
