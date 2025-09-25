package resourceusage

import (
	"context"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/stretchr/testify/assert"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
)

func TestGetFieldsDiffs(t *testing.T) {
	schemaCache := lru.NewLRU[ypath.YPath, *schema.Schema](1024, nil, 0)
	featuresCache := lru.NewLRU[ypath.YPath, *ResourceUsageTableFeatures](1024, nil, 0)

	tests := []struct {
		name string

		rut1         *ResourceUsageTable
		rut1schema   *schema.Schema
		rut1features *ResourceUsageTableFeatures

		rut2         *ResourceUsageTable
		rut2schema   *schema.Schema
		rut2features *ResourceUsageTableFeatures

		expectedIntersection map[string]struct{}
		expectedOnlyA        map[string]struct{}
		expectedOnlyB        map[string]struct{}
		expectError          bool
		errorContains        string
	}{
		{
			name: "all_intersects",
			rut1: &ResourceUsageTable{
				Path:                 ypath.Path("//basic_diff_1"),
				ClusterSchemaCache:   schemaCache,
				ClusterFeaturesCache: featuresCache,
			},
			rut1schema: &schema.Schema{
				Columns: []schema.Column{
					{Name: "column1"},
					{Name: "column2"},
				},
			},
			rut1features: &ResourceUsageTableFeatures{
				RecursiveVersionedResourceUsage: 0,
			},
			rut2: &ResourceUsageTable{
				Path:                 ypath.Path("//basic_diff_2"),
				ClusterSchemaCache:   schemaCache,
				ClusterFeaturesCache: featuresCache,
			},
			rut2schema: &schema.Schema{
				Columns: []schema.Column{
					{Name: "column1"},
					{Name: "column2"},
				},
			},
			rut2features: &ResourceUsageTableFeatures{
				RecursiveVersionedResourceUsage: 0,
			},
			expectedIntersection: map[string]struct{}{"column1": {}, "column2": {}},
			expectedOnlyA:        map[string]struct{}{},
			expectedOnlyB:        map[string]struct{}{},
			expectError:          false,
			errorContains:        "",
		},
		{
			name: "one_intersection",
			rut1: &ResourceUsageTable{
				Path:                 ypath.Path("//one_intersection_1"),
				ClusterSchemaCache:   schemaCache,
				ClusterFeaturesCache: featuresCache,
			},
			rut1schema: &schema.Schema{
				Columns: []schema.Column{
					{Name: "column1"},
					{Name: "column2"},
					{Name: "column3"},
				},
			},
			rut1features: &ResourceUsageTableFeatures{
				RecursiveVersionedResourceUsage: 0,
			},
			rut2: &ResourceUsageTable{
				Path:                 ypath.Path("//one_intersection_2"),
				ClusterSchemaCache:   schemaCache,
				ClusterFeaturesCache: featuresCache,
			},
			rut2schema: &schema.Schema{
				Columns: []schema.Column{
					{Name: "column3"},
					{Name: "column4"},
					{Name: "column5"},
				},
			},
			rut2features: &ResourceUsageTableFeatures{
				RecursiveVersionedResourceUsage: 0,
			},
			expectedIntersection: map[string]struct{}{"column3": {}},
			expectedOnlyA:        map[string]struct{}{"column1": {}, "column2": {}},
			expectedOnlyB:        map[string]struct{}{"column4": {}, "column5": {}},
			expectError:          false,
			errorContains:        "",
		},
		{
			name: "no_intersections",
			rut1: &ResourceUsageTable{
				Path:                 ypath.Path("//no_intersections_1"),
				ClusterSchemaCache:   schemaCache,
				ClusterFeaturesCache: featuresCache,
			},
			rut1schema: &schema.Schema{
				Columns: []schema.Column{
					{Name: "column1"},
					{Name: "column2"},
				},
			},
			rut1features: &ResourceUsageTableFeatures{
				RecursiveVersionedResourceUsage: 0,
			},
			rut2: &ResourceUsageTable{
				Path:                 ypath.Path("//no_intersections_2"),
				ClusterSchemaCache:   schemaCache,
				ClusterFeaturesCache: featuresCache,
			},
			rut2schema: &schema.Schema{
				Columns: []schema.Column{
					{Name: "column4"},
					{Name: "column5"},
				},
			},
			rut2features: &ResourceUsageTableFeatures{
				RecursiveVersionedResourceUsage: 0,
			},
			expectedIntersection: map[string]struct{}{},
			expectedOnlyA:        map[string]struct{}{"column1": {}, "column2": {}},
			expectedOnlyB:        map[string]struct{}{"column4": {}, "column5": {}},
			expectError:          false,
			errorContains:        "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.rut1.ClusterSchemaCache.Add(tt.rut1.Path, tt.rut1schema)
			tt.rut1.ClusterFeaturesCache.Add(tt.rut1.Path, tt.rut1features)
			tt.rut1.ClusterSchemaCache.Add(tt.rut2.Path, tt.rut2schema)
			tt.rut1.ClusterFeaturesCache.Add(tt.rut2.Path, tt.rut2features)
			intersection, onlyA, onlyB, err := tt.rut1.GetFieldsDiffs(context.Background(), tt.rut2)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedIntersection, intersection)
				assert.Equal(t, tt.expectedOnlyA, onlyA)
				assert.Equal(t, tt.expectedOnlyB, onlyB)
			}
		})
	}
}

func TestBuildSelector(t *testing.T) {
	schemaCache := lru.NewLRU[ypath.YPath, *schema.Schema](1024, nil, 0)
	featuresCache := lru.NewLRU[ypath.YPath, *ResourceUsageTableFeatures](1024, nil, 0)

	tests := []struct {
		name          string
		rut           *ResourceUsageTable
		rutSchema     *schema.Schema
		rutFeatures   *ResourceUsageTableFeatures
		expected      string
		expectError   bool
		errorContains string
	}{
		{
			name: "two_columns",
			rut: &ResourceUsageTable{
				Path:                 ypath.Path("//two_columns"),
				ClusterSchemaCache:   schemaCache,
				ClusterFeaturesCache: featuresCache,
			},
			rutSchema: &schema.Schema{
				Columns: []schema.Column{
					{Name: "column1"},
					{Name: "column2"},
				},
			},
			rutFeatures: &ResourceUsageTableFeatures{
				RecursiveVersionedResourceUsage: 0,
			},
			expected: "[column1], [column2]",
		},
		{
			name: "one_column",
			rut: &ResourceUsageTable{
				Path:                 ypath.Path("//one_column"),
				ClusterSchemaCache:   schemaCache,
				ClusterFeaturesCache: featuresCache,
			},
			rutSchema: &schema.Schema{
				Columns: []schema.Column{
					{Name: "column1"},
				},
			},
			rutFeatures: &ResourceUsageTableFeatures{
				RecursiveVersionedResourceUsage: 0,
			},
			expected: "[column1]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.rut.ClusterSchemaCache.Add(tt.rut.Path, tt.rutSchema)
			tt.rut.ClusterFeaturesCache.Add(tt.rut.Path, tt.rutFeatures)
			selector := tt.rut.buildSelector(context.Background())
			assert.Equal(t, tt.expected, selector)
		})
	}
}

func TestBuildSelectorDiff(t *testing.T) {
	schemaCache := lru.NewLRU[ypath.YPath, *schema.Schema](1024, nil, 0)
	featuresCache := lru.NewLRU[ypath.YPath, *ResourceUsageTableFeatures](1024, nil, 0)

	tests := []struct {
		name             string
		rut              *ResourceUsageTable
		rutSchema        *schema.Schema
		rutFeatures      *ResourceUsageTableFeatures
		newerRUT         *ResourceUsageTable
		newerRUTSchema   *schema.Schema
		newerRUTFeatures *ResourceUsageTableFeatures
		expectedParts    []string
	}{
		{
			name: "two_columns",
			rut: &ResourceUsageTable{
				Path:                 ypath.Path("//two_columns"),
				ClusterSchemaCache:   schemaCache,
				ClusterFeaturesCache: featuresCache,
			},
			rutSchema: &schema.Schema{
				Columns: []schema.Column{
					{Name: "access_time"},
					{Name: "account"},
					{Name: "approximate_row_count"},
					{Name: "chunk_count"},
					{Name: "creation_time"},
					{Name: "depth"},
					{Name: "direct_child_count"},
					{Name: "disk_space"},
					{Name: "dynamic"},
					{Name: "medium:default"},
					{Name: "modification_time"},
					{Name: "node_count"},
					{Name: "owner"},
					{Name: "path"},
					{Name: "path_patched"},
					{Name: "primary_medium"},
					{Name: "tablet_count"},
					{Name: "tablet_static_memory"},
					{Name: "type"},
					{Name: "medium:older"},
				},
			},
			rutFeatures: &ResourceUsageTableFeatures{
				RecursiveVersionedResourceUsage: 0,
			},
			newerRUT: &ResourceUsageTable{
				Path:                 ypath.Path("//two_columns"),
				ClusterSchemaCache:   schemaCache,
				ClusterFeaturesCache: featuresCache,
			},
			newerRUTSchema: &schema.Schema{
				Columns: []schema.Column{
					{Name: "access_time"},
					{Name: "account"},
					{Name: "approximate_row_count"},
					{Name: "chunk_count"},
					{Name: "creation_time"},
					{Name: "depth"},
					{Name: "direct_child_count"},
					{Name: "disk_space"},
					{Name: "dynamic"},
					{Name: "medium:default"},
					{Name: "modification_time"},
					{Name: "node_count"},
					{Name: "owner"},
					{Name: "path"},
					{Name: "path_patched"},
					{Name: "primary_medium"},
					{Name: "tablet_count"},
					{Name: "tablet_static_memory"},
					{Name: "type"},
					{Name: "medium:newer"},
				},
			},
			newerRUTFeatures: &ResourceUsageTableFeatures{
				RecursiveVersionedResourceUsage: 0,
			},
			expectedParts: []string{
				"if(NOT is_null(newer.[account]) AND NOT is_null(older.[account]), if(newer.[creation_time] != older.[creation_time], \"recreated\", \"untouched\"), if(is_null(newer.[account]), \"removed\", \"created\")) AS [recreation_status]",
				"newer.[access_time] AS [access_time]",
				"newer.[account] AS [account]",
				"newer.[creation_time] AS [creation_time]",
				"newer.[depth] AS [depth]",
				"newer.[dynamic] AS [dynamic]",
				"newer.[modification_time] AS [modification_time]",
				"newer.[owner] AS [owner]",
				"newer.[path] AS [path]",
				"newer.[path_patched] AS [path_patched]",
				"newer.[primary_medium] AS [primary_medium]",
				"newer.[type] AS [type]",
				"if(is_null(newer.[approximate_row_count]), 0, newer.[approximate_row_count]) - if(is_null(older.[approximate_row_count]), 0, older.[approximate_row_count]) AS [approximate_row_count]",
				"if(is_null(newer.[chunk_count]), 0, newer.[chunk_count]) - if(is_null(older.[chunk_count]), 0, older.[chunk_count]) AS [chunk_count]",
				"if(is_null(newer.[disk_space]), 0, newer.[disk_space]) - if(is_null(older.[disk_space]), 0, older.[disk_space]) AS [disk_space]",
				"if(is_null(newer.[medium:default]), 0, newer.[medium:default]) - if(is_null(older.[medium:default]), 0, older.[medium:default]) AS [medium:default]",
				"if(is_null(newer.[medium:newer]), 0, newer.[medium:newer]) - if(is_null(older.[medium:newer]), 0, older.[medium:newer]) AS [medium:newer]",
				"if(is_null(newer.[node_count]), 0, newer.[node_count]) - if(is_null(older.[node_count]), 0, older.[node_count]) AS [node_count]",
				"if(is_null(newer.[tablet_count]), 0, newer.[tablet_count]) - if(is_null(older.[tablet_count]), 0, older.[tablet_count]) AS [tablet_count]",
				"if(is_null(newer.[tablet_static_memory]), 0, newer.[tablet_static_memory]) - if(is_null(older.[tablet_static_memory]), 0, older.[tablet_static_memory]) AS [tablet_static_memory]",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.rut.ClusterSchemaCache.Add(tt.rut.Path, tt.rutSchema)
			tt.rut.ClusterFeaturesCache.Add(tt.rut.Path, tt.rutFeatures)
			tt.newerRUT.ClusterSchemaCache.Add(tt.newerRUT.Path, tt.newerRUTSchema)
			tt.newerRUT.ClusterFeaturesCache.Add(tt.newerRUT.Path, tt.newerRUTFeatures)
			selector := tt.rut.buildSelectorDiff(context.Background(), tt.newerRUT)
			for _, part := range tt.expectedParts {
				assert.Contains(t, selector, part)
			}
		})
	}
}
