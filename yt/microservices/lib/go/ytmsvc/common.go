package ytmsvc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

var Logger *zap.Logger

func Must0(err error) {
	if err != nil {
		panic(err)
	}
}

func Must[T any](obj T, err error) T {
	if err != nil {
		panic(err)
	}
	return obj
}

func Must2[T1, T2 any](obj1 T1, obj2 T2, err error) (T1, T2) {
	if err != nil {
		panic(err)
	}
	return obj1, obj2
}

func MustNewYTClient(proxy string, tokenEnvVariable string) yt.Client {
	reqTimeout := time.Duration(1 * time.Second)
	ytConfig := yt.Config{
		Proxy:               proxy,
		Logger:              Logger,
		LightRequestTimeout: &reqTimeout,
		Token:               TokenFromEnvVariable(tokenEnvVariable),
	}
	return Must(ythttp.NewClient(&ytConfig))
}

func GetRequestSomeID(req *http.Request) string {
	for _, header := range []string{
		"X-Yt-Request-Id",
		"X-Yt-Trace-Id",
		"X-Yt-Correlation-Id",
	} {
		if values, ok := req.Header[header]; ok {
			return values[0]
		}
	}
	return uuid.New().String()
}

func getClustersFromClusterDirectory(ctx context.Context, ytClient yt.Client) map[string]struct{} {
	var clusters = make(map[string]any)
	err := ytClient.GetNode(ctx, ypath.Path("//sys/clusters"), &clusters, nil)
	if err != nil {
		Logger.Errorf("Can't get clusters list from cluster: %v", err.Error())
		return nil
	}
	result := make(map[string]struct{})
	for cluster := range clusters {
		result[cluster] = struct{}{}
	}
	return result
}

func GetClusters(ctx context.Context, ytClient yt.Client, includeArr []string, excludeArr []string, useClusterDirectory bool) map[string]struct{} {
	clusters := make(map[string]struct{})
	if useClusterDirectory {
		clusters = getClustersFromClusterDirectory(ctx, ytClient)
		if clusters == nil {
			return nil
		}
	}
	for _, ic := range includeArr {
		if _, ok := clusters[ic]; !ok {
			clusters[ic] = struct{}{}
		}
	}
	for _, ec := range excludeArr {
		delete(clusters, ec)
	}
	return clusters
}

func SetToSlice[T comparable](set map[T]struct{}) []T {
	i := 0
	slice := make([]T, len(set))
	for key := range set {
		slice[i] = key
		i++
	}
	return slice
}

func SliceToSet[T comparable](slice []T) map[T]struct{} {
	set := make(map[T]struct{})
	for _, key := range slice {
		set[key] = struct{}{}
	}
	return set
}

func GetUserIP(req *http.Request) string {
	for _, header := range []string{
		"X-Forwarded-For-Y",
		"X-Forwarded-For",
		"Http_X_Forwarded_For",
		"Http_X_Real_Ip",
	} {
		Logger.Debugf("Checking header %v", header)
		if val, ok := req.Header[header]; ok {
			Logger.Debugf("Found %v with values: %v", header, val)
			return val[len(val)-1]
		}
	}
	return strings.Split(req.RemoteAddr, ":")[0]
}

func TokenFromEnvVariable(tokenEnvVariable string) string {
	if tokenEnvVariable == "" {
		tokenEnvVariable = "YT_TOKEN"
	}
	return os.Getenv(tokenEnvVariable)
}

func RandHexString(length int) string {
	bytes := make([]byte, (length+1)/2)
	_, _ = rand.Read(bytes)
	hexStr := hex.EncodeToString(bytes)
	return hexStr[:length]
}
