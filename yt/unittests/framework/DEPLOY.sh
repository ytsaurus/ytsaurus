#!/bin/bash

set -e
set -x

if [[ -z $GTEST ]] ; then
    GTEST=$1
fi

if [[ -z $GMOCK ]] ; then
    GMOCK=$2
fi

if [[ -z $GTEST || ! -d $GTEST ]] ; then
    echo "Please, set \$GTEST variable to point to valid google-test tree." >&2
    exit 1
fi

if [[ -z $GMOCK || ! -d $GMOCK ]] ; then
    echo "Please, set \$GMOCK variable to point to valid google-test tree." >&2
    exit 1
fi

if [[ -z $TARGET ]] ; then
    TARGET=$(pwd)
fi

echo "\t  gtest: ${GTEST}"
echo "\t  gmock: ${GMOCK}"
echo "\t target: ${TARGET}"

# Respect the copyrights.
cp ${GTEST}/README ${TARGET}/README.gtest
cp ${GMOCK}/README ${TARGET}/README.gmock
cp ${GTEST}/COPYING ${TARGET}/LICENSE.gtest
cp ${GMOCK}/COPYING ${TARGET}/LICENSE.gmock

# gmock includes gtest as a subpart, hence no need to copy gtest.
rm -rf ${GMOCK}/fused
mkdir -p ${GMOCK}/fused
${GMOCK}/scripts/fuse_gmock_files.py ${GMOCK} ${GMOCK}/fused

# Copy the source and the headers.
mv ${GMOCK}/fused/gmock-gtest-all.cc ${TARGET}/framework.cpp
mv ${GMOCK}/fused/gmock/gmock.h ${TARGET}/gmock.h
mv ${GMOCK}/fused/gtest/gtest.h ${TARGET}/gtest.h

# The following commands should be executed from target directory.
cd ${TARGET}

# Correct include paths.
perl -pi -e 's!^#include "(gtest|gmock)/!#include "framework/!' {gtest,gmock}.h
# EXPECT_(TRUE|FALSE) are defined in arcadia/util to control branch prediction.
perl -pi -e 's!\bEXPECT_(TRUE|FALSE)\b!EXPECT_IS_\1!g' {gtest,gmock}.h framework.cpp

# This patch alters includes and fixes few compilation warnings.
patch -p1 <<EOP
--- a/framework.cpp
+++ b/framework.cpp
@@ -1,3 +1,14 @@
+// Set up initial configuration for YT.
+#include "framework.h"
+
+// Disable stack-protector warnings.
+// warning: frame size too large for reliable stack checking
+// warning: try reducing the number of local variables
+#ifdef __GNUC__
+# pragma GCC diagnostic ignored "-Wstack-protector"
+#endif
+
+//
 // Copyright 2008, Google Inc.
 // All rights reserved.
 //
@@ -34,10 +45,6 @@
 // Sometimes it's desirable to build Google Test by compiling a single file.
 // This file serves this purpose.
 
-// This line ensures that gtest.h can be compiled on its own, even
-// when it's fused.
-#include "gtest/gtest.h"
-
 // The following lines pull in the real gtest *.cc files.
 // Copyright 2005, Google Inc.
 // All rights reserved.
@@ -7042,7 +7049,7 @@
 class Arguments {
  public:
   Arguments() {
-    args_.push_back(NULL);
+    args_.push_back((char*)NULL);
   }
 
   ~Arguments() {
@@ -9153,10 +9160,6 @@
 // purpose is to allow a user to build Google Mock by compiling this
 // file alone.
 
-// This line ensures that gmock.h can be compiled on its own, even
-// when it's fused.
-#include "gmock/gmock.h"
-
 // The following lines pull in the real gmock *.cc files.
 // Copyright 2007, Google Inc.
 // All rights reserved.
EOP

# This patch fixes obscure bug caused by the mismatch of allocators used by libc
# and our program.
patch -p1 <<EOP
--- a/gtest.h
+++ b/gtest.h
@@ -2654,7 +2654,13 @@
 inline int StrCaseCmp(const char* s1, const char* s2) {
   return strcasecmp(s1, s2);
 }
-inline char* StrDup(const char* src) { return strdup(src); }
+inline char* StrDup(const char* src) {
+    size_t size = 1 + strlen(src);
+    char* target = (char*)malloc(size);
+    memcpy(target, src, size - 1);
+    target[size - 1] = 0;
+    return target;
+}
 inline int RmDir(const char* dir) { return rmdir(dir); }
 inline bool IsDir(const StatStruct& st) { return S_ISDIR(st.st_mode); }
EOP
 
echo "Voila!"
echo "Please, don't forget to update README and DEPLOY.sh after update."
