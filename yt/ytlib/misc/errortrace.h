#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Installs handler for the fatal signals (that is -- bad code errors):
// SIGILL, SIGFPE, SIGSEGV, SIGBUS and also SIGABRT.
// Handler will print on stderr possibly helpful backtrace and
// details of a current operating context (if it provided).
//
// Always return 0. Return value 1 is possible only if there is
// a bug in SetupErrorHandler implementation.
//
int SetupErrorHandler();

/*!
 *  TErrorContext is a base for user context object(s) used to store some
 *  context of the action being performed. TErrorContext object registers
 *  itself with signal handler on object creation (supposedly at action start)
 *  and deregisters on object destruction (supposedly at action end).
 *  
 *  ToString method must dump context into the supplied buffer as text.
 *  As long as there is a TErrorContext object registered, signal handler will
 *  output content of the buffer filled by ToString call to stderr
 *  along with signal info and backtrace.
 */
class TErrorContext
{
public:
    TErrorContext();
    virtual ~TErrorContext();

    virtual void ToString(char* buffer, int size) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT