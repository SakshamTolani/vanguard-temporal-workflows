package vanguardworkflows

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"
)

func HelloActivity(ctx context.Context, name string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("HelloActivity invoked", "name", name)

	return fmt.Sprintf("Hello, %s", name), nil
}