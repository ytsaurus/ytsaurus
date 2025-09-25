package access

func parent(path string) string {
	if path == "/" {
		return path
	}
	return path[:len(path)-1]
}
