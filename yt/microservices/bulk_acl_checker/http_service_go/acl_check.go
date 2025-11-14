package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"net/http"
	"strings"

	"go.ytsaurus.tech/yt/go/yt"
	bac_lib "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/lib_go"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

type Subjects []string

type ACL []yt.ACE

type CompressedACL map[int]Subjects

func Hash(acl CompressedACL) string {
	var b bytes.Buffer
	ytmsvc.Must0(gob.NewEncoder(&b).Encode(acl))
	return b.String()
}

type CompressedACLValue struct {
	Action          yt.SecurityAction
	InheritanceMode string
}

type ACLDumpMap map[string]*ACLDump

type ACLDump struct {
	Paths ACLDumpMap
	ACL   CompressedACL
}

var indexToACLArr = []CompressedACLValue{
	{yt.ActionAllow, yt.InheritanceModeObjectOnly},
	{yt.ActionAllow, yt.InheritanceModeObjectAndDescendants},
	{yt.ActionAllow, yt.InheritanceModeDescendantsOnly},
	{yt.ActionAllow, yt.InheritanceModeImmediateDescendantsOnly},
	{yt.ActionDeny, yt.InheritanceModeObjectOnly},
	{yt.ActionDeny, yt.InheritanceModeObjectAndDescendants},
	{yt.ActionDeny, yt.InheritanceModeDescendantsOnly},
	{yt.ActionDeny, yt.InheritanceModeImmediateDescendantsOnly},
}

var aclToIndexMap = func() map[CompressedACLValue]int {
	result := make(map[CompressedACLValue]int)
	for index, value := range indexToACLArr {
		result[value] = index
	}
	return result
}()

var inheritanceModeEvolutionMap = func() map[yt.InheritanceMode]yt.InheritanceMode {
	result := make(map[yt.InheritanceMode]yt.InheritanceMode)
	result[yt.InheritanceModeObjectOnly] = yt.InheritanceModeNone
	result[yt.InheritanceModeObjectAndDescendants] = yt.InheritanceModeObjectAndDescendants
	result[yt.InheritanceModeDescendantsOnly] = yt.InheritanceModeObjectAndDescendants
	result[yt.InheritanceModeImmediateDescendantsOnly] = yt.InheritanceModeObjectOnly
	return result
}()

func IndexToACL(index int) (yt.SecurityAction, yt.InheritanceMode) {
	if index < 0 || index >= len(indexToACLArr) {
		return yt.ActionDeny, yt.InheritanceModeNone
	}
	value := indexToACLArr[index]
	return value.Action, value.InheritanceMode
}

func ACLToIndex(action yt.SecurityAction, inheritanceMode yt.InheritanceMode) int {
	result, ok := aclToIndexMap[CompressedACLValue{action, inheritanceMode}]
	if !ok {
		return -1
	}
	return result
}

func evoluteInheritanceMode(inheritanceModeEvolutionMap map[yt.InheritanceMode]yt.InheritanceMode, inheritanceMode yt.InheritanceMode) yt.InheritanceMode {
	return inheritanceModeEvolutionMap[inheritanceMode]
}

func CheckCompressedACLLocal(groups Groups, acl CompressedACL) (result yt.SecurityAction) {
	if subjects, ok := acl[4]; ok { //4->{yt.ActionDeny, yt.InheritanceModeObjectOnly},
		for _, subject := range subjects {
			if _, ok = groups[subject]; ok {
				return yt.ActionDeny
			}
		}
	}
	if subjects, ok := acl[5]; ok { //5->{yt.ActionDeny, yt.InheritanceModeObjectAndDescendants}
		for _, subject := range subjects {
			if _, ok = groups[subject]; ok {
				return yt.ActionDeny
			}
		}
	}
	if subjects, ok := acl[0]; ok { //0->{yt.ActionAllow, yt.InheritanceModeObjectOnly}
		for _, subject := range subjects {
			if _, ok = groups[subject]; ok {
				return yt.ActionAllow
			}
		}
	}
	if subjects, ok := acl[1]; ok { //1->{yt.ActionAllow, yt.InheritanceModeObjectAndDescendants}
		for _, subject := range subjects {
			if _, ok = groups[subject]; ok {
				return yt.ActionAllow
			}
		}
	}
	//Skip descendants ACL
	//2->{yt.ActionAllow, yt.InheritanceModeDescendantsOnly}
	//3->{yt.ActionAllow, yt.InheritanceModeImmediateDescendantsOnly}
	//6->{yt.ActionDeny, yt.InheritanceModeDescendantsOnly}
	//7->{yt.ActionDeny, yt.InheritanceModeImmediateDescendantsOnly}
	return yt.ActionDeny
}

func BulkCheckACL(ctx context.Context, req bac_lib.CheckACLRequest, actor string) (result []yt.SecurityAction, err error) {
	result = []yt.SecurityAction{} // Initialize empty slise to avoid serialization to null
	metrics := ActorMetrics{}
	aclCache := Cache.Get(req.Cluster)
	if aclCache == nil {
		err = fmt.Errorf("unknown cluster %s", req.Cluster)
		return
	}
	path2hash := make(map[string]string)
	hash2ACL := make(map[string]ACL)
	hash2action := make(map[string]yt.SecurityAction)
	var groups Groups
	if aclCache.UsersExport != nil {
		groups = aclCache.UsersExport[req.Subject]
		if groups == nil {
			for range req.Paths {
				result = append(result, yt.ActionDeny)
			}
			return
		}
	}
	for _, path := range req.Paths {
		var compressedACL CompressedACL
		compressedACL, err = getCompressedACL(aclCache.ACLDump, path)
		if err != nil {
			return
		}
		hash := Hash(compressedACL)
		path2hash[path] = hash
		lruKey := LRUCacheKey{
			Version: aclCache.Version,
			Subject: req.Subject,
			ACLHash: hash,
		}
		if groups != nil {
			metrics.SuccessChecks += 1
			hash2action[hash] = CheckCompressedACLLocal(groups, compressedACL)
		} else if action, ok := Cache.LRU.Get(lruKey); ok {
			metrics.SuccessChecks += 1
			metrics.CacheHit += 1
			hash2action[hash] = action
		} else {
			acl := unpackACL(compressedACL)
			hash2ACL[hash] = acl
		}
	}
	results := ParallelCheckPermissionByACL(ctx, aclCache.YtClient, &hash2ACL, req.Subject)
	for range len(hash2ACL) {
		answer := <-results
		if answer.Err != nil {
			metrics.FailChecks += 1
			hash2action[answer.Hash] = yt.SecurityAction("error")
			continue
		}
		hash2action[answer.Hash] = answer.Action
		lruKey := LRUCacheKey{
			Version: aclCache.Version,
			Subject: req.Subject,
			ACLHash: answer.Hash,
		}
		metrics.SuccessChecks += 1
		Cache.LRU.Add(lruKey, answer.Action)
	}
	Metrics.Update(actor, metrics)
	for _, path := range req.Paths {
		result = append(result, hash2action[path2hash[path]])
	}
	return
}

type ParallelCheckPermissionByACLResult struct {
	Hash   string
	Action yt.SecurityAction
	Err    error
}

func ParallelCheckPermissionByACL(ctx context.Context, ytClient yt.Client, hash2ACL *map[string]ACL, subject string) (result chan (ParallelCheckPermissionByACLResult)) {
	result = make(chan ParallelCheckPermissionByACLResult)
	for hash, acl := range *hash2ACL {
		go func() {
			options := yt.CheckPermissionByACLOptions{
				MasterReadOptions: &yt.MasterReadOptions{
					ReadFrom: yt.ReadFromCache,
				},
			}
			ret, err := ytClient.CheckPermissionByACL(ctx, subject, yt.PermissionRead, acl, &options)
			if err != nil {
				result <- ParallelCheckPermissionByACLResult{
					Hash:   hash,
					Action: yt.ActionDeny,
					Err:    err,
				}
			} else {
				result <- ParallelCheckPermissionByACLResult{
					Hash:   hash,
					Action: ret.Action,
					Err:    nil,
				}
			}
		}()
	}
	return
}

func EvolveCompressedACL(compressedACL CompressedACL, depth int) (CompressedACL, error) {
	if depth == 0 || compressedACL == nil {
		return compressedACL, nil
	}

	evolutionMap := make(map[yt.InheritanceMode]yt.InheritanceMode)
	if depth == 1 {
		evolutionMap = inheritanceModeEvolutionMap
	} else {
		for key, value := range inheritanceModeEvolutionMap {
			evolutionMap[key] = evoluteInheritanceMode(inheritanceModeEvolutionMap, value)
		}
	}

	newCompressedACL := CompressedACL{}
	for inheritanceModeIndex, subjects := range compressedACL {
		action, inheritanceMode := IndexToACL(inheritanceModeIndex)
		newInheritanceMode := evoluteInheritanceMode(evolutionMap, inheritanceMode)
		if newInheritanceMode != yt.InheritanceModeNone {
			newCompressedACL[ACLToIndex(action, newInheritanceMode)] = subjects
		}
	}
	return newCompressedACL, nil
}

func getCompressedACL(ACL *ACLDump, path string) (CompressedACL, error) {
	currentRoot := ACL
	lastNonTrivial := ACL.ACL
	depthFromLast := 0
	for _, part := range strings.Split(path, "/")[1:] {
		depthFromLast += 1
		if currentRoot != nil {
			currentRoot = currentRoot.Paths[part]
			if currentRoot != nil && currentRoot.ACL != nil {
				lastNonTrivial = currentRoot.ACL
				depthFromLast = 0
			}
		}
	}
	return EvolveCompressedACL(lastNonTrivial, depthFromLast)
}

func unpackACL(compressedACL CompressedACL) (result ACL) {
	for aclIndex, subjects := range compressedACL {
		action, inheritanceMode := IndexToACL(aclIndex)
		result = append(result, yt.ACE{
			Action:          yt.SecurityAction(action),
			Subjects:        subjects,
			Permissions:     []string{yt.PermissionRead},
			InheritanceMode: inheritanceMode,
		})
	}
	return
}

func ClickHouseCheckACL(w http.ResponseWriter, httpReq *http.Request) (result []bac_lib.ClickHouseDictResponse, err error) {
	//TODO: check access
	var requests []bac_lib.ClickHouseDictRequest
	requests, err = ReadClickhouseDictRequest(httpReq.Body)
	if err != nil {
		return
	}
	groupedRequests := make(map[string]map[string][]string)
	for _, request := range requests {
		if _, ok := groupedRequests[request.Cluster]; !ok {
			groupedRequests[request.Cluster] = make(map[string][]string)
		}
		groupedRequests[request.Cluster][request.Subject] = append(groupedRequests[request.Cluster][request.Subject], request.Path)
	}
	for cluster, subjectAndPath := range groupedRequests {
		for subject, paths := range subjectAndPath {
			checkACLReq := bac_lib.CheckACLRequest{
				Cluster: cluster,
				Subject: subject,
				Paths:   paths,
			}
			var checkResult []yt.SecurityAction
			checkResult, err = BulkCheckACL(httpReq.Context(), checkACLReq, "clickhouse/"+subject)
			if err != nil {
				return
			}
			for i, action := range checkResult {
				response := bac_lib.ClickHouseDictResponse{
					Cluster: cluster,
					Subject: subject,
					Path:    paths[i],
					Action:  string(action),
				}
				result = append(result, response)
			}
		}
	}
	return
}
