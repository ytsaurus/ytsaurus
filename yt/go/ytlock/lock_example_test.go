package ytlock_test

import (
	"context"
	"fmt"
	"time"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/ytlock"
)

var lockOptions = ytlock.Options{
	OnConflict: ytlock.Fail,
	LockPath:   ypath.Path("//home/prime/locks/example"),
}

func doWork(ctx context.Context) error {
	select {
	case <-time.After(time.Hour):
		fmt.Println("work is done")

	case <-ctx.Done():
		fmt.Println("lock is lost, exiting immediately")
	}

	return nil
}

func ExampleWith() {
	var yc yt.Client

	if err := ytlock.With(context.Background(), yc, lockOptions, doWork); err != nil {
		if winner := ytlock.FindConflictWinner(err); winner != nil {
			fmt.Printf("lock is held by another process: %v\n", winner)
		} else {
			fmt.Printf("error occured in doWork: %+v\n", err)
		}
	}
}
