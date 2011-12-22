#pragma once

class TOutputStream;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Prints current call stack to the specified output.
/*!
 * Currently, this method works only with GCC.
 * For your convenience, use macro PrintCallStack() instead of this one.
 *
 * \param output Output stream.
 * \param file Auxiliary information about caller file.
 * \param line Auxiliary information about caller line.
 */
void PrintCallStackAux(TOutputStream& output, const char* file, int line);

//! Prints current call stack to the standard error.
//! \see PrintCallStackAux
void PrintCallStack();

//! Breaks program execution and triggers debugger.
/*!
 * Currently, this method works only with GCC.
 */
void Break();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

