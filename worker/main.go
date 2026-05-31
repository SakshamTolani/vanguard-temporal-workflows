// Worker binary for the Vanguard Temporal workflows.
//
// Connects to Temporal, registers HelloWorkflow + HelloActivity on the
// "hello-task-queue" task queue, and polls forever. When someone starts
// a HelloWorkflow execution on this task queue, this worker picks it up
// and runs the orchestration + activity code.
//
// In Kubernetes, this runs as a long-lived Deployment. Multiple replicas
// can poll the same task queue safely — Temporal distributes work across
// available workers automatically.
package main

import (
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	vanguardworkflows "github.com/SakshamTolani/vanguard-temporal-workflows"
)

const (
	// specifies this queue when starting a workflow; only workers polling
	// the same queue will pick it up. Production systems typically have
	// one queue per service or per workflow type.
	TaskQueue = "hello-task-queue"

	// Namespace defaults to "default". Real production setups have a
	// namespace per environment / team / tenant.
	Namespace = "default"
)

func main() {
	// HostPort is the Temporal frontend gRPC address. When running inside
	// the cluster, the in-cluster DNS resolves this. Override via env var
	// for local development (e.g., when running the binary on a laptop
	// against a port-forwarded frontend).
	hostPort := os.Getenv("TEMPORAL_HOST_PORT")
	if hostPort == "" {
		hostPort = "temporal-frontend.temporal.svc.cluster.local:7233"
	}

	log.Printf("Connecting to Temporal frontend at %s", hostPort)

	// client.Dial establishes the gRPC connection to Temporal. The connection
	// is reused for all subsequent operations; don't dial per-operation.
	c, err := client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: Namespace,
	})
	if err != nil {
		log.Fatalf("Unable to create Temporal client: %v", err)
	}
	defer c.Close()

	log.Printf("Registering worker on task queue %q in namespace %q", TaskQueue, Namespace)

	// w is a Worker instance. It encapsulates the polling logic and a
	// registry of which workflows and activities this worker knows how
	// to execute.
	w := worker.New(c, TaskQueue, worker.Options{})

	// Register the workflow and activity so the worker can dispatch
	// incoming tasks to them. Temporal identifies workflows/activities
	// by their function names by default ("HelloWorkflow", "HelloActivity").
	w.RegisterWorkflow(vanguardworkflows.HelloWorkflow)
	w.RegisterActivity(vanguardworkflows.HelloActivity)

	// Run blocks forever, polling the task queue. It returns only on
	// fatal error or when the process receives SIGTERM/SIGINT (worker.Run
	// handles signal trapping internally; no need to manage signals here).
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker run failed: %v", err)
	}
}