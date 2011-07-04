#pragma once

/*!
 * \file fs.h
 * \brief Function to work with file system
 */

#include "common.h"

namespace NYT {
namespace NFS {

////////////////////////////////////////////////////////////////////////////////

//! File suffix for temporary files.
const char* const TempFileSuffix = "~";

//! Removes file.
/*!
 * \param name File name
 * \return True when succeeds
 */
bool Remove(const char* name);

//! Renames file.
/*!
 * \param oldName Old name
 * \param newName New name
 * \return True when succeeds
 */
bool Rename(const char* oldName, const char* newName);

//! Returns name of file
/*!
 * \param filePath Path of file
 */
Stroka GetFileName(Stroka filePath);

//! Returns extension of file
/*!
 * \param filePath Path of file
 */
Stroka GetFileExtension(Stroka filePath);


//! Returns name of file without extension
/*!
 * \param filePath Path of file
 */
Stroka GetFileNameWithoutExtension(Stroka filePath);

//! Deletes all files in dir with extension #TempFileSuffix
/*!
 * \param location Location of dir
 */
void CleanTempFiles(Stroka location);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFS
} // namespace NYT
