package mapreduce

import (
	"context"
	"time"

	"a.yandex-team.ru/yt/go/yt"
)

type Operation interface {
	ID() yt.OperationID

	Wait() error
}

type operation struct {
	c   yt.Client
	ctx context.Context

	opID yt.OperationID
}

func (o *operation) ID() yt.OperationID {
	return o.opID
}

func (o *operation) Wait() error {
	for {
		status, err := o.c.GetOperation(o.ctx, o.opID, nil)
		if err != nil {
			return err
		}

		if status.State.IsFinished() {
			if status.Result.Error != nil && status.Result.Error.Code != 0 {
				return status.Result.Error
			}

			return nil
		}

		time.Sleep(time.Second * 5)
	}
}
