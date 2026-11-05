// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package insecuresecretdataaccess provides the definition of a token
// used to control and track access to secret data.
package insecuresecretdataaccess

// Token is a required parameter for Tink APIs that return secret data.
//
// Users who need access to secret data must hold a value of this type.
// Within Google, this token is used in conjunction with the build system to
// restrict access to functions that return secret data.
type Token struct{}
