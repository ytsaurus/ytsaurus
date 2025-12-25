package resourceusage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

func (rut *ResourceUsageTable) GetFieldsDiffs(ctx context.Context, rutB *ResourceUsageTable) (intersection, onlyA, onlyB map[string]struct{}, err error) {

	fieldsA, err := rut.GetClearedFields(ctx)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error getting fields", log.Error(err))
		return nil, nil, nil, err
	}
	fieldsMapA := map[string]struct{}{}
	for _, field := range fieldsA {
		fieldsMapA[field] = struct{}{}
	}

	fieldsB, err := rutB.GetClearedFields(ctx)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error getting newer fields", log.Error(err))
		return nil, nil, nil, err
	}
	fieldsMapB := map[string]struct{}{}
	for _, field := range fieldsB {
		fieldsMapB[field] = struct{}{}
	}

	intersection = map[string]struct{}{}
	for field := range fieldsMapA {
		if _, ok := fieldsMapB[field]; ok {
			intersection[field] = struct{}{}
		}
	}

	onlyA = map[string]struct{}{}
	for field := range fieldsMapA {
		if _, ok := fieldsMapB[field]; !ok {
			onlyA[field] = struct{}{}
		}
	}

	onlyB = map[string]struct{}{}
	for field := range fieldsMapB {
		if _, ok := fieldsMapA[field]; !ok {
			onlyB[field] = struct{}{}
		}
	}

	return intersection, onlyA, onlyB, nil
}

func (rut *ResourceUsageTable) SelectRowsDiff(ctx context.Context, input selectRowsDiffInput) (*[]Item, string, error) {
	paginationWhereClauses, placeholderValues, paginationOffsetClause, err := rut.validateAndPreparePagination(*input.pageSelector, input.sortOrders)
	if err != nil {
		return nil, "", err
	}

	query := strings.Join([]string{
		rut.buildSelectorDiff(ctx, input.newerRUT),
		"FROM",
		fmt.Sprintf("[%s]", input.newerRUT.Path),
		"AS newer",
		"LEFT JOIN",
		fmt.Sprintf("[%s]", rut.Path),
		"AS older",
		"ON (newer.account, newer.path) = (older.account, older.path)",
		"WHERE",
		rut.buildWhereClauseDiff(ctx, input.account, *input.filter, input.newerRUT, paginationWhereClauses),
		rut.buildSortClauseDiff(ctx, input.sortOrders, input.newerRUT),
		paginationOffsetClause,
		"LIMIT",
		fmt.Sprintf("%d", input.pageSelector.Size),
	}, " ")

	return rut.executeSelectQueryAndProcessResults(ctx, query, placeholderValues, input.pageSelector)
}

func (rut *ResourceUsageTable) executeSelectQueryAndProcessResults(
	ctx context.Context,
	query string,
	placeholderValues map[string]any,
	pageSelector *PageSelector,
) (*[]Item, string, error) {
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:  rut.Proxy,
		Logger: rut.l,
		Token:  ytmsvc.TokenFromEnvVariable(rut.TokenEnvVariable),
	})
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error creating yt client", log.Error(err))
		return nil, "", err
	}

	r, err := yc.SelectRows(ctx, query, &yt.SelectRowsOptions{
		InputRowLimit:     ptr.Int(6_000_000),
		OutputRowLimit:    ptr.Int(6_000_000),
		PlaceholderValues: placeholderValues,
	})
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error selecting rows", log.Error(err), log.String("query", query))
		return nil, "", err
	}
	defer r.Close()
	var item Item
	items := &[]Item{}
	for r.Next() {
		if err := r.Scan(&item); err != nil {
			ctxlog.Error(ctx, rut.l.Logger(), "error scanning row", log.Error(err), log.String("query", query))
			return nil, "", err
		}
		*items = append(*items, item)
	}

	if err := r.Err(); err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error iterating over rows", log.Error(err), log.String("query", query))
		return nil, "", err
	}

	var continuationToken string
	if pageSelector.EnableContinuationToken && len(*items) > 0 {
		continuationToken, err = rut.continuationTokenFromItem(ctx, item)
		if err != nil {
			ctxlog.Error(ctx, rut.l.Logger(), "error generating continuation token", log.Error(err), log.String("query", query), log.Any("item", item))
			return nil, "", err
		}
	}

	return items, continuationToken, nil
}

func (rut *ResourceUsageTable) buildSelector(ctx context.Context) string {
	fields, err := rut.GetFields(ctx)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error getting fields", log.Error(err))
		return ""
	}
	for i := range fields {
		fields[i] = fmt.Sprintf("[%s]", fields[i])
	}
	return strings.Join(fields, ", ")
}

func (rut *ResourceUsageTable) buildSelectorDiff(ctx context.Context, newerRUT *ResourceUsageTable) string {
	intersection, olderOnly, newerOnly, err := rut.GetFieldsDiffs(ctx, newerRUT)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error getting fields diffs", log.Error(err))
		return ""
	}

	staticFields := map[string]struct{}{
		"account": {}, "path": {}, "type": {}, "dynamic": {}, "owner": {},
		"depth": {}, "path_patched": {}, "primary_medium": {},
		"creation_time": {}, "access_time": {}, "modification_time": {},
	}

	removedFields := map[string]struct{}{
		"direct_child_count": {}, "versioned_resource_usage": {}, "resource_usage": {},
	}

	isExcluded := func(field string) bool {
		_, inStatic := staticFields[field]
		_, inRemoved := removedFields[field]
		return inStatic || inRemoved
	}

	parts := []string{
		`if(NOT is_null(newer.[account]) AND NOT is_null(older.[account]), ` +
			`if(newer.[creation_time] != older.[creation_time], "recreated", "untouched"), ` +
			`if(is_null(newer.[account]), "removed", "created")) AS [recreation_status]`,
	}

	for field := range staticFields {
		parts = append(parts, fmt.Sprintf("newer.[%s] AS [%s]", field, field))
	}

	for field := range intersection {
		if isExcluded(field) {
			continue
		}
		parts = append(parts, fmt.Sprintf("if(is_null(newer.[%s]), 0, newer.[%s]) - if(is_null(older.[%s]), 0, older.[%s]) AS [%s]",
			field, field, field, field, field))
	}

	for field := range olderOnly {
		if isExcluded(field) {
			continue
		}
		parts = append(parts, fmt.Sprintf("-if(is_null(older.[%s]), 0, older.[%s]) AS [%s]",
			field, field, field))
	}

	for field := range newerOnly {
		if isExcluded(field) {
			continue
		}
		parts = append(parts, fmt.Sprintf("if(is_null(newer.[%s]), 0, newer.[%s]) AS [%s]",
			field, field, field))
	}
	return strings.Join(parts, ", ")
}

func (rut *ResourceUsageTable) buildWhereClause(ctx context.Context, account string, filter Filter, additionalClauses []string) string {
	whereClause := []string{fmt.Sprintf("account = '%s'", account)}
	if filter.Owner != "" {
		whereClause = append(whereClause, fmt.Sprintf("owner = '%s'", filter.Owner))
	}
	schema, err := rut.GetSchema(ctx)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error getting schema", log.Error(err))
		return ""
	}
	columnNames := []string{}
	for _, column := range schema.Columns {
		columnNames = append(columnNames, column.Name)
	}
	if basePath := filter.BasePath; basePath != "" {
		if ok := slices.Contains(columnNames, "account_parent"); ok {
			whereClause = append(whereClause, fmt.Sprintf("is_prefix(account_parent, '%s')", basePath))
			whereClause = append(whereClause, fmt.Sprintf("is_prefix('%s/', path)", basePath))
			if pathRegexp := filter.PathRegexp; pathRegexp != "" {
				dirname := basePath + "/"
				whereClause = append(whereClause, fmt.Sprintf("regex_partial_match('%s', regex_replace_first('^%s', path, ''))", pathRegexp, dirname))
				filter.PathRegexp = ""
			}
		} else {
			dirname := basePath + "/"
			whereClause = append(whereClause, fmt.Sprintf("is_prefix('%s', path)", dirname))
			baseDepth := strings.Count(dirname, "/")
			whereClause = append(whereClause, fmt.Sprintf("depth = %d", baseDepth))
			if pathRegexp := filter.PathRegexp; pathRegexp != "" {
				whereClause = append(whereClause, fmt.Sprintf("regex_partial_match('%s', regex_replace_first('^%s', path, ''))", pathRegexp, dirname))
				filter.PathRegexp = ""
			}
		}
	}
	if excludeMapNodes := filter.ExcludeMapNodes; excludeMapNodes {
		whereClause = append(whereClause, "type != 'map_node'")
	}
	if pathRegexp := filter.PathRegexp; pathRegexp != "" {
		whereClause = append(whereClause, fmt.Sprintf("regex_partial_match('%s', path)", pathRegexp))
	}
	if tableType := filter.TableType; tableType != "" {
		if tableType == "dynamic" {
			whereClause = append(whereClause, "dynamic = %true")
		} else {
			whereClause = append(whereClause, "dynamic = %false")
		}
	}
	if fieldFilters := filter.FieldFilters; fieldFilters != nil {
		for _, fieldFilter := range fieldFilters {
			whereClause = append(whereClause, fmt.Sprintf("[%s] %s %d", fieldFilter.Field, fieldFilter.Comparison, fieldFilter.Value))
		}
	}
	whereClause = append(whereClause, additionalClauses...)
	return strings.Join(whereClause, " AND ")
}

func (rut *ResourceUsageTable) buildWhereClauseDiff(ctx context.Context, account string, filter Filter, newerRUT *ResourceUsageTable, additionalClauses []string) string {
	intersection, olderOnly, newerOnly, err := rut.GetFieldsDiffs(ctx, newerRUT)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error getting fields diffs", log.Error(err))
		return ""
	}
	whereClause := []string{fmt.Sprintf("newer.account = '%s'", account)}
	if filter.Owner != "" {
		whereClause = append(whereClause, fmt.Sprintf("newer.owner = '%s'", filter.Owner))
	}
	if filter.BasePath != "" {
		dirname := filter.BasePath + "/"
		whereClause = append(whereClause, fmt.Sprintf("is_prefix('%s', newer.path)", dirname))

		baseDepth := strings.Count(dirname, "/")
		whereClause = append(whereClause, fmt.Sprintf("newer.depth = %d", baseDepth))
		if pathRegexp := filter.PathRegexp; pathRegexp != "" {
			whereClause = append(whereClause, fmt.Sprintf("regex_partial_match('%s', regex_replace_first('^%s', newer.path, ''))", pathRegexp, dirname))
			filter.PathRegexp = ""
		}
	}
	if filter.ExcludeMapNodes {
		whereClause = append(whereClause, "newer.type != 'map_node'")
	}
	if filter.PathRegexp != "" {
		whereClause = append(whereClause, fmt.Sprintf("regex_partial_match('%s', newer.path)", filter.PathRegexp))
	}
	if filter.TableType != "" {
		if filter.TableType == "dynamic" {
			whereClause = append(whereClause, "newer.dynamic = %true")
		} else {
			whereClause = append(whereClause, "newer.dynamic = %false")
		}
	}
	if filter.FieldFilters != nil {
		for _, fieldFilter := range filter.FieldFilters {
			if _, ok := intersection[fieldFilter.Field]; ok {
				whereClause = append(
					whereClause,
					fmt.Sprintf(
						"if(is_null(newer.[%s]), 0, newer.[%s]) - if(is_null(older.[%s]), 0, older.[%s]) %s %d",
						fieldFilter.Field,
						fieldFilter.Field,
						fieldFilter.Field,
						fieldFilter.Field,
						fieldFilter.Comparison,
						fieldFilter.Value,
					),
				)
			}
			if _, ok := olderOnly[fieldFilter.Field]; ok {
				whereClause = append(whereClause, fmt.Sprintf("-if(is_null(older.[%s]), 0, older.[%s]) %s %d", fieldFilter.Field, fieldFilter.Field, fieldFilter.Comparison, fieldFilter.Value))
			}
			if _, ok := newerOnly[fieldFilter.Field]; ok {
				whereClause = append(whereClause, fmt.Sprintf("if(is_null(newer.[%s]), 0, newer.[%s]) %s %d", fieldFilter.Field, fieldFilter.Field, fieldFilter.Comparison, fieldFilter.Value))
			}
		}
	}
	whereClause = append(whereClause, additionalClauses...)
	return strings.Join(whereClause, " AND ")
}

func (rut *ResourceUsageTable) buildSortClause(sortOrders []*SortOrder) string {
	if len(sortOrders) == 0 {
		return ""
	}
	sortClause := []string{}
	for _, partial := range sortOrders {
		field := partial.Field
		order := "ASC"
		if partial.Desc {
			order = "DESC"
		}
		sortClause = append(sortClause, fmt.Sprintf("[%s] %s", field, order))
	}
	return "ORDER BY " + strings.Join(sortClause, ", ")
}

func (rut *ResourceUsageTable) SelectRows(ctx context.Context, input selectRowsInput) (*[]Item, string, error) {
	paginationWhereClauses, placeholderValues, paginationOffsetClause, err := rut.validateAndPreparePagination(*input.pageSelector, input.sortOrders)
	if err != nil {
		return nil, "", err
	}

	query := strings.Join([]string{
		rut.buildSelector(ctx),
		"FROM",
		fmt.Sprintf("[%s]", rut.Path),
		"WHERE",
		rut.buildWhereClause(ctx, input.account, *input.filter, paginationWhereClauses),
		rut.buildSortClause(input.sortOrders),
		paginationOffsetClause,
		"LIMIT",
		fmt.Sprintf("%d", input.pageSelector.Size),
	}, " ")

	return rut.executeSelectQueryAndProcessResults(ctx, query, placeholderValues, input.pageSelector)
}

func (rut *ResourceUsageTable) CountRows(ctx context.Context, input countRowsInput) (int, error) {
	if input.pageSelector != nil && input.pageSelector.EnableContinuationToken || input.pageSelector.ContinuationToken != "" {
		return 0, nil
	}

	query := strings.Join([]string{
		"SUM(1) AS count",
		"FROM",
		fmt.Sprintf("[%s]", rut.Path),
		"WHERE",
		rut.buildWhereClause(ctx, input.account, *input.filter, nil),
		"GROUP BY 1",
	}, " ")

	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:  rut.Proxy,
		Logger: rut.l,
		Token:  ytmsvc.TokenFromEnvVariable(rut.TokenEnvVariable),
	})
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error creating yt client", log.Error(err))
		return 0, err
	}

	r, err := yc.SelectRows(ctx, query, &yt.SelectRowsOptions{
		InputRowLimit:  ptr.Int(6_000_000),
		OutputRowLimit: ptr.Int(6_000_000),
	})
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error selecting rows", log.Error(err), log.String("query", query))
		return 0, err
	}
	defer r.Close()

	type countResult struct {
		Count int `yson:"count"`
	}
	var result countResult
	if r.Next() {
		if err := r.Scan(&result); err != nil {
			ctxlog.Error(ctx, rut.l.Logger(), "error scanning row", log.Error(err), log.String("query", query))
			return 0, err
		}
	}
	r.Next()
	return result.Count, nil
}

func (rut *ResourceUsageTable) GetSchema(ctx context.Context) (*schema.Schema, error) {
	if schema, ok := rut.ClusterSchemaCache.Get(rut.Path); ok {
		return schema, nil
	}
	schema := &schema.Schema{}
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:  rut.Proxy,
		Logger: rut.l,
		Token:  ytmsvc.TokenFromEnvVariable(rut.TokenEnvVariable),
	})
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error creating yt client", log.Error(err))
		return nil, err
	}
	err = yc.GetNode(ctx, rut.Path.YPath().Attr("schema"), &schema, nil)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error getting schema", log.Error(err))
		return nil, err
	}
	if schema == nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error getting schema: schema is nil")
		return nil, fmt.Errorf("schema is nil")
	}
	rut.ClusterSchemaCache.Add(rut.Path, schema)
	return schema, nil
}

func (rut *ResourceUsageTable) GetFeatures(ctx context.Context) (*ResourceUsageTableFeatures, error) {
	if features, ok := rut.ClusterFeaturesCache.Get(rut.Path); ok {
		return features, nil
	}
	features := &ResourceUsageTableFeatures{}
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:  rut.Proxy,
		Logger: rut.l,
		Token:  ytmsvc.TokenFromEnvVariable(rut.TokenEnvVariable),
	})
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error creating yt client", log.Error(err))
		return features, err
	}
	err = yc.GetNode(ctx, rut.Path.YPath().Attr("_features"), &features, nil)
	if err != nil {
		error := yterrors.FromError(err)
		if yterrors.ContainsErrorCode(error, yterrors.ErrorCode(500)) {
			rut.ClusterFeaturesCache.Add(rut.Path, features)
			return features, nil
		}
		ctxlog.Error(ctx, rut.l.Logger(), "error getting features", log.Error(err))
		rut.ClusterFeaturesCache.Add(rut.Path, features)
		return features, err
	}
	rut.ClusterFeaturesCache.Add(rut.Path, features)
	return features, nil
}

func (rut *ResourceUsageTable) GetSanitizedColumns(ctx context.Context) (*[]schema.Column, error) {
	talbeSchema, err := rut.GetSchema(ctx)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "failed to get schema", log.Error(err))
		return nil, err
	}

	features, err := rut.GetFeatures(ctx)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "failed to get features", log.Error(err))
		return nil, err
	}

	columns := []schema.Column{}

	for _, column := range talbeSchema.Columns {
		if column.Name != "recursive_versioned_resource_usage" && (features.RecursiveVersionedResourceUsage > 0 || !strings.HasPrefix(column.Name, "versioned:")) {
			columns = append(columns, column)
		}
	}
	return &columns, nil
}

func (rut *ResourceUsageTable) GetFields(ctx context.Context) ([]string, error) {
	sanitizedColumns, err := rut.GetSanitizedColumns(ctx)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "failed to get sanitized columns", log.Error(err))
		return nil, err
	}
	columns := []string{}
	for _, column := range *sanitizedColumns {
		columns = append(columns, column.Name)
	}
	return columns, nil
}

func (rut *ResourceUsageTable) GetClearedFields(ctx context.Context) ([]string, error) {
	allFields, err := rut.GetFields(ctx)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error getting fields", log.Error(err))
		return nil, err
	}
	fields := []string{}
	for _, field := range allFields {
		if strings.HasPrefix(field, "versioned:") {
			continue
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func (rut *ResourceUsageTable) GetMediums(ctx context.Context) ([]string, error) {
	sanitizedColumns, err := rut.GetSanitizedColumns(ctx)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "failed to get sanitized columns", log.Error(err))
		return nil, err
	}
	mediums := []string{}
	for _, column := range *sanitizedColumns {
		if strings.HasPrefix(column.Name, "medium:") {
			medium := strings.TrimPrefix(column.Name, "medium:")
			mediums = append(mediums, medium)
		}
	}
	return mediums, nil
}

func (rut *ResourceUsageTable) CountRowsDiff(ctx context.Context, input countRowsDiffInput) (int, error) {
	if input.pageSelector != nil && input.pageSelector.EnableContinuationToken || input.pageSelector.ContinuationToken != "" {
		return 0, nil
	}

	query := strings.Join([]string{
		"SUM(1) AS count",
		"FROM",
		fmt.Sprintf("[%s]", input.newerRUT.Path),
		"AS newer",
		"LEFT JOIN",
		fmt.Sprintf("[%s]", rut.Path),
		"AS older",
		"ON (newer.account, newer.path) = (older.account, older.path)",
		"WHERE",
		rut.buildWhereClauseDiff(ctx, input.account, *input.filter, input.newerRUT, nil),
		"GROUP BY 1",
	}, " ")
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:  rut.Proxy,
		Logger: rut.l,
		Token:  ytmsvc.TokenFromEnvVariable(rut.TokenEnvVariable),
	})
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error creating yt client", log.Error(err))
		return 0, err
	}
	r, err := yc.SelectRows(ctx, query, &yt.SelectRowsOptions{
		InputRowLimit:  ptr.Int(6_000_000),
		OutputRowLimit: ptr.Int(6_000_000),
	})
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error selecting rows", log.Error(err), log.String("query", query))
		return 0, err
	}
	defer r.Close()
	type countResult struct {
		Count int `yson:"count"`
	}
	var result countResult
	if r.Next() {
		if err := r.Scan(&result); err != nil {
			ctxlog.Error(ctx, rut.l.Logger(), "error scanning row", log.Error(err), log.String("query", query))
			return 0, err
		}
	}
	r.Next()
	return result.Count, nil
}

func (rut *ResourceUsageTable) buildSortClauseDiff(ctx context.Context, sortOrder []*SortOrder, newerRUT *ResourceUsageTable) string {
	staticFields := map[string]struct{}{
		"account": {}, "path": {}, "type": {}, "dynamic": {}, "owner": {},
		"depth": {}, "creation_time": {}, "modification_time": {},
	}
	if len(sortOrder) == 0 {
		return ""
	}
	_, olderOnly, newerOnly, err := rut.GetFieldsDiffs(ctx, newerRUT)
	if err != nil {
		ctxlog.Error(ctx, rut.l.Logger(), "error getting fields diffs", log.Error(err))
		return ""
	}
	sortClause := []string{}
	for _, partial := range sortOrder {
		field := partial.Field
		order := "ASC"
		if partial.Desc {
			order = "DESC"
		}
		if _, ok := staticFields[field]; ok {
			if _, ok := olderOnly[field]; ok {
				sortClause = append(sortClause, fmt.Sprintf("older.[%s] %s", field, order))
			} else {
				sortClause = append(sortClause, fmt.Sprintf("newer.[%s] %s", field, order))
			}
		} else if _, ok := olderOnly[field]; ok {
			sortClause = append(sortClause, fmt.Sprintf("-if(is_null(older.[%s]), 0, older.[%s]) %s", field, field, order))
		} else if _, ok := newerOnly[field]; ok {
			sortClause = append(sortClause, fmt.Sprintf("if(is_null(newer.[%s]), 0, newer.[%s]) %s", field, field, order))
		} else {
			sortClause = append(sortClause, fmt.Sprintf("if(is_null(newer.[%s]), 0, newer.[%s]) - if(is_null(older.[%s]), 0, older.[%s]) %s", field, field, field, field, order))
		}
	}
	return "ORDER BY " + strings.Join(sortClause, ", ")
}

func (rut *ResourceUsageTable) validateAndPreparePagination(pageSelector PageSelector, sortOrders []*SortOrder) ([]string, map[string]any, string, error) {
	if (pageSelector.EnableContinuationToken || pageSelector.ContinuationToken != "") && len(sortOrders) > 0 {
		return nil, nil, "", fmt.Errorf("you cannot use sort_order and continuation_token at the same time")
	}

	if pageSelector.ContinuationToken != "" {
		continuationToken, err := rut.ContinuationTokenSerializer.Decode(pageSelector.ContinuationToken)
		if err != nil {
			return nil, nil, "", fmt.Errorf("error while decoding continuation_token: %w", err)
		}
		continuationTokenExpression, continuationTokenPlaceholderValues := continuationToken.GetExpressionAndPlaceholderValues()
		return []string{continuationTokenExpression}, continuationTokenPlaceholderValues, "", nil
	}

	return nil, nil, fmt.Sprintf("OFFSET %d", pageSelector.Index*pageSelector.Size), nil
}

func (rut *ResourceUsageTable) continuationTokenFromItem(ctx context.Context, item Item) (string, error) {
	continuationToken := &ContinuationToken{}

	var err error
	if continuationToken.Features, err = rut.GetFeatures(ctx); err != nil {
		return "", err
	}

	var ok bool
	if continuationToken.Account, ok = item["account"].(string); !ok {
		return "", errors.New("`account` field is not present in last item")
	}

	if continuationToken.Depth, ok = item["depth"].(int64); !ok {
		return "", errors.New("`depth` field is not present in last item")
	}

	if continuationToken.Path, ok = item["path"].(string); !ok {
		return "", errors.New("`path` field is not present in last item")
	}

	if continuationToken.Features.TypeInKey > 0 {
		if continuationToken.Type, ok = item["type"].(string); !ok {
			return "", errors.New("`type` field is not present in last item")
		}
	}

	return rut.ContinuationTokenSerializer.Encode(*continuationToken)
}
