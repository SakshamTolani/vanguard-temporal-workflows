package vanguardworkflows

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

func HelloWorkflow(ctx workflow.Context, name string) (string, error) {
	// ActivityOptions tell Temporal HOW to execute the activity:
	// retry policy, timeouts, task queue (defaults to the workflow's queue).
	// StartToCloseTimeout is the max time a single activity attempt may run.
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	var result string
	err := workflow.ExecuteActivity(ctx, HelloActivity, name).Get(ctx, &result)
	if err != nil {
		return "", err
	}
	return result, nil
}