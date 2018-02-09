package replicated

import (
	"time"

	"github.com/docker/go-events"
	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/log"
	"github.com/docker/swarmkit/manager/orchestrator"
	"github.com/docker/swarmkit/manager/orchestrator/taskinit"
	"github.com/docker/swarmkit/manager/state/store"
	"github.com/docker/swarmkit/protobuf/ptypes"
	"golang.org/x/net/context"
)

// This file provides task-level orchestration. It observes changes to task
// and node state and kills/recreates tasks if necessary. This is distinct from
// service-level reconciliation, which observes changes to services and creates
// and/or kills tasks to match the service definition.

func (r *Orchestrator) initTasks(ctx context.Context, readTx store.ReadTx) error {
	return taskinit.CheckTasks(ctx, r.store, readTx, r, r.restarts)
}

func (r *Orchestrator) handleTaskEvent(ctx context.Context, event events.Event) {
	switch v := event.(type) {
	case api.EventDeleteNode:
		r.restartTasksByNodeID(ctx, v.Node.ID)
		// eyz START: allow orphaned Swarm tasks assigned to a deleted node to start elseware
		r.restartOrphanedTasksByNodeID(ctx, v.Node.ID)
		// eyz STOP: allow orphaned Swarm tasks assigned to a deleted node to start elseware
	case api.EventCreateNode:
		r.handleNodeChange(ctx, v.Node)
	case api.EventUpdateNode:
		r.handleNodeChange(ctx, v.Node)
	case api.EventDeleteTask:
		if v.Task.DesiredState <= api.TaskStateRunning {
			service := r.resolveService(ctx, v.Task)
			if !orchestrator.IsReplicatedService(service) {
				return
			}
			r.reconcileServices[service.ID] = service
		}
		r.restarts.Cancel(v.Task.ID)
	case api.EventUpdateTask:
		r.handleTaskChange(ctx, v.Task)
	case api.EventCreateTask:
		r.handleTaskChange(ctx, v.Task)
	}
}

func (r *Orchestrator) tickTasks(ctx context.Context) {
	if len(r.restartTasks) > 0 {
		err := r.store.Batch(func(batch *store.Batch) error {
			for taskID := range r.restartTasks {
				err := batch.Update(func(tx store.Tx) error {
					// TODO(aaronl): optimistic update?
					t := store.GetTask(tx, taskID)
					if t != nil {
						if t.DesiredState > api.TaskStateRunning {
							return nil
						}

						service := store.GetService(tx, t.ServiceID)
						if !orchestrator.IsReplicatedService(service) {
							return nil
						}

						// Restart task if applicable
						if err := r.restarts.Restart(ctx, tx, r.cluster, service, *t); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					log.G(ctx).WithError(err).Errorf("Orchestrator task reaping transaction failed")
				}
			}
			return nil
		})

		if err != nil {
			log.G(ctx).WithError(err).Errorf("orchestrator task removal batch failed")
		}

		r.restartTasks = make(map[string]struct{})
	}
}

// eyz START: set Swarm tasks to orphaned state if node becomes unavailable
func (r *Orchestrator) restartTasksByNodeID(ctx context.Context, nodeID string) {
	err := r.store.Batch(func(batch *store.Batch) error {
		return batch.Update(func(tx store.Tx) error {
			var tasks []*api.Task
			var err error
			tasks, err = store.FindTasks(tx, store.ByNodeID(nodeID))
			if err != nil {
				return err
			}

			// Testing (Isaac, 20171106)
			var n *api.Node
			n = store.GetNode(tx, nodeID)
			nBlockRestartNodeDown := (n != nil && n.Spec.Availability == api.NodeAvailabilityActive && n.Status.State == api.NodeStatus_DOWN)

			for _, t := range tasks {
				if t.DesiredState > api.TaskStateRunning {
					continue
				}

				// Testing (Isaac, 20171106)
				//if nBlockRestartNodeDown && t != nil && t.Status.State >= api.TaskStateAssigned && t.Status.State <= api.TaskStateRunning {
				if nBlockRestartNodeDown && t.Status.State >= api.TaskStateAssigned {
					//t.DesiredState = api.TaskStateOrphaned
					t.Status.State = api.TaskStateOrphaned
					t.Status.Timestamp = ptypes.MustTimestampProto(time.Now())
					store.UpdateTask(tx, t)
					continue
				}

				service := store.GetService(tx, t.ServiceID)
				if orchestrator.IsReplicatedService(service) {
					r.restartTasks[t.ID] = struct{}{}
				}
			}

			return nil
		})
	})
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to list and/or update tasks to restart")
	}
}

// eyz STOP: set Swarm tasks to orphaned state if node becomes unavailable

// eyz START: allow orphaned Swarm tasks assigned to a deleted node to start elseware
func (r *Orchestrator) restartOrphanedTasksByNodeID(ctx context.Context, nodeID string) {
	err := r.store.Batch(func(batch *store.Batch) error {
		return batch.Update(func(tx store.Tx) error {
			var tasks []*api.Task
			var err error
			tasks, err = store.FindTasks(tx, store.ByNodeID(nodeID))
			if err != nil {
				return err
			}

			for _, t := range tasks {
				if t.Status.State == api.TaskStateOrphaned {
					service := store.GetService(tx, t.ServiceID)
					if orchestrator.IsReplicatedService(service) {
						t.DesiredState = api.TaskStateRunning
						t.Status.State = api.TaskStateShutdown
						t.Status.Timestamp = ptypes.MustTimestampProto(time.Now())
						store.UpdateTask(tx, t)
						r.restartTasks[t.ID] = struct{}{}
					}
				}
			}

			return nil
		})
	})
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to list tasks and/or restart orphans")
	}
}

// eyz STOP: allow orphaned Swarm tasks assigned to a deleted node to start elseware

func (r *Orchestrator) handleNodeChange(ctx context.Context, n *api.Node) {
	if !orchestrator.InvalidNode(n) {
		return
	}

	r.restartTasksByNodeID(ctx, n.ID)
}

// handleTaskChange defines what orchestrator does when a task is updated by agent.
func (r *Orchestrator) handleTaskChange(ctx context.Context, t *api.Task) {
	// If we already set the desired state past TaskStateRunning, there is no
	// further action necessary.
	if t.DesiredState > api.TaskStateRunning {
		return
	}

	var (
		n       *api.Node
		service *api.Service
	)
	r.store.View(func(tx store.ReadTx) {
		if t.NodeID != "" {
			n = store.GetNode(tx, t.NodeID)
		}
		if t.ServiceID != "" {
			service = store.GetService(tx, t.ServiceID)
		}
	})

	if !orchestrator.IsReplicatedService(service) {
		return
	}

	if t.Status.State > api.TaskStateRunning ||
		(t.NodeID != "" && orchestrator.InvalidNode(n)) {
		r.restartTasks[t.ID] = struct{}{}
	}
}

// FixTask validates a task with the current cluster settings, and takes
// action to make it conformant. it's called at orchestrator initialization.
func (r *Orchestrator) FixTask(ctx context.Context, batch *store.Batch, t *api.Task) {
	// If we already set the desired state past TaskStateRunning, there is no
	// further action necessary.
	if t.DesiredState > api.TaskStateRunning {
		return
	}

	var (
		n       *api.Node
		service *api.Service
	)
	batch.Update(func(tx store.Tx) error {
		if t.NodeID != "" {
			n = store.GetNode(tx, t.NodeID)
		}
		if t.ServiceID != "" {
			service = store.GetService(tx, t.ServiceID)
		}
		return nil
	})

	if !orchestrator.IsReplicatedService(service) {
		return
	}

	if t.Status.State > api.TaskStateRunning ||
		(t.NodeID != "" && orchestrator.InvalidNode(n)) {
		r.restartTasks[t.ID] = struct{}{}
		return
	}
}
