package app

import "os"

func getStrawberryToken(token string) string {
	if token == "" {
		return os.Getenv("STRAWBERRY_TOKEN")
	}
	return token
}
