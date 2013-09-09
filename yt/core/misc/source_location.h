#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSourceLocation
{
public:
    TSourceLocation();
    TSourceLocation(
        const void* instructionPointer,
        const char* function,
        const char* file,
        int line);


    inline const void* GetInstructionPointer() const
    {
        return InstructionPointer;
    }

    inline const char* GetFunctionName() const
    {
        return Function;
    }

    inline const char* GetFileName() const
    {
        return File;
    }

    inline int GetFileLine() const
    {
        return Line;
    }

private:
    const void* InstructionPointer;
    const char* Function;
    const char* File;
    int Line;

};

//! A function to get current instruction pointer.
const void* GetInstructionPointer();

//! Define a macro to record the current source location.
#ifdef __GNUC__
#define FROM_HERE FROM_HERE_WITH_EXPLICIT_FUNCTION(__PRETTY_FUNCTION__)
#else
#define FROM_HERE FROM_HERE_WITH_EXPLICIT_FUNCTION(__FUNCTION__)
#endif

#define FROM_HERE_WITH_EXPLICIT_FUNCTION(functionName) \
    ::NYT::TSourceLocation(::NYT::GetInstructionPointer(), \
        functionName, \
        __FILE__, \
        __LINE__)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
