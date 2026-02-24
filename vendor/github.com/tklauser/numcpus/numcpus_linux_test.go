// Copyright 2018 Tobias Klauser
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package numcpus

import (
	"reflect"
	"testing"
)

func TestCPURange(t *testing.T) {
	testCases := []struct {
		str       string
		wantCount int
		wantList  []int
		wantErr   bool
	}{
		{
			str:       "",
			wantCount: 0,
			wantList:  []int{},
		},
		{
			str:       "\n",
			wantCount: 0,
			wantList:  []int{},
		},
		{
			str:       "0",
			wantCount: 1,
			wantList:  []int{0},
		},
		{
			str:       "0-1",
			wantCount: 2,
			wantList:  []int{0, 1},
		},
		{
			str:       "1-1",
			wantCount: 1,
			wantList:  []int{1},
		},
		{
			str:       "0-7",
			wantCount: 8,
			wantList:  []int{0, 1, 2, 3, 4, 5, 6, 7},
		},
		{
			str:       "1-7",
			wantCount: 7,
			wantList:  []int{1, 2, 3, 4, 5, 6, 7},
		},
		{
			str:       "1-15",
			wantCount: 15,
			wantList:  []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		},
		{
			str:       "0-3,7",
			wantCount: 5,
			wantList:  []int{0, 1, 2, 3, 7},
		},
		{
			str:       "0,2-4",
			wantCount: 4,
			wantList:  []int{0, 2, 3, 4},
		},
		{
			str:       "0,2-4,7",
			wantCount: 5,
			wantList:  []int{0, 2, 3, 4, 7},
		},
		{
			str:       "0,2-4,7-15",
			wantCount: 13,
			wantList:  []int{0, 2, 3, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		},
		{
			str:       "0,2-4,6,8-10,31",
			wantCount: 9,
			wantList:  []int{0, 2, 3, 4, 6, 8, 9, 10, 31},
		},
		{
			str:       " 0,2-7\n",
			wantCount: 7,
			wantList:  []int{0, 2, 3, 4, 5, 6, 7},
		},
		{
			str:     "invalid",
			wantErr: true,
		},
		{
			str:     "-",
			wantErr: true,
		},
		{
			str:     ",",
			wantErr: true,
		},
		{
			str:     ",1",
			wantErr: true,
		},
		{
			str:     "0,",
			wantErr: true,
		},
		{
			str:     "0-",
			wantErr: true,
		},
		{
			str:     "-15",
			wantErr: true,
		},
		{
			str:     "0-,1",
			wantErr: true,
		},
		{
			str:     "0,-3,5",
			wantErr: true,
		},
		{
			str:     "42-0",
			wantErr: true,
		},
		{
			str:     "0,5-3",
			wantErr: true,
		},
		{
			str:     "0, 5 - 3",
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		count, err := countCPURange(tc.str)
		if !tc.wantErr && err != nil {
			t.Errorf("countCPURange(%q) = %v, expected no error", tc.str, err)
		} else if tc.wantErr && err == nil {
			t.Errorf("countCPURange(%q) expected error", tc.str)
		}

		if count != tc.wantCount {
			t.Errorf("countCPURange(%q) = %d, expected %d", tc.str, count, tc.wantCount)
		}

		list, err := listCPURange(tc.str)
		if !tc.wantErr && err != nil {
			t.Errorf("listCPURange(%q) = %v, expected no error", tc.str, err)
		} else if tc.wantErr && err == nil {
			t.Errorf("listCPURange(%q) expected error", tc.str)
		}

		if !reflect.DeepEqual(list, tc.wantList) {
			t.Errorf("listCPURange(%q) = %d, expected %d", tc.str, list, tc.wantList)
		}
	}
}

func TestGetFromCPUAffinity(t *testing.T) {
	nAffinity, err := getFromCPUAffinity()
	if err != nil {
		t.Fatalf("getFromCPUAffinity: %v", err)
	}

	cpus := "online"
	nSysfs, err := readCPURangeWith(cpus, countCPURange)
	if err != nil {
		t.Fatalf("counting CPU ranges from %q failed: %v", cpus, err)
	}

	if nAffinity != nSysfs {
		t.Errorf("getFromCPUAffinity() = %d, readCPURange(%q) = %d, want the same return value", nAffinity, cpus, nSysfs)
	}
}
