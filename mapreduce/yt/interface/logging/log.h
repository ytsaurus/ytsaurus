#pragma once

#include "logger.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline void LogMessage(ILogger::ELevel level, const ::TSourceLocation& sourceLocation, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    GetLogger()->Log(level, sourceLocation, format, args);
    va_end(args);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define LOG_DEBUG(...) { NYT::LogMessage(NYT::ILogger::DEBUG, __LOCATION__, __VA_ARGS__); }
#define LOG_INFO(...)  { NYT::LogMessage(NYT::ILogger::INFO, __LOCATION__, __VA_ARGS__); }
#define LOG_ERROR(...) { NYT::LogMessage(NYT::ILogger::ERROR, __LOCATION__, __VA_ARGS__); }
