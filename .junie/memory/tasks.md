# Project Tasks

## Roadmap / Pending Tasks

### Task Scheduling

Design a durable MongoDB-backed task system that replaces in-memory async callbacks for workflows that must survive service restarts.

#### Goals
- Support the current workflow where the web process schedules work and independent worker processes execute it.
- Survive service restarts without losing queued or running tasks.
- Support both one-time and recurring task execution.
- Allow adding a new task type by adding a module with a defined structure, without changing scheduler or worker core code.
- Move async classification and detailization side effects into task handlers, because independent workers cannot return values to the original caller.

#### Module-based task types
- Task implementations should live in modules under a dedicated package, for example `app/scheduler/tasks/`.
- Each task module should define one task class inherited from `BaseTask` and `SingletonMixin`.
- `BaseTask` should define the standard task interface:
  - `taskType`: unique string identifier used in task documents.
  - `execute(payload)`: executes the task and updates all affected DB state itself. It must be a monad: no return value is used by the scheduler or worker.
- Worker startup should discover task classes dynamically and build a registry from `taskType` to task singleton.
- Queue/worker core should store and route by `taskType`, but should not contain hardcoded knowledge of concrete tasks like classification or detailization.

Example module layout:

```text
app/
  scheduler/
    task_interface.py          # task DB state transitions: claim, complete, fail, archive/delete
    task_queue.py              # queue entry point that starts and manages workers and monitor
    task_worker.py             # worker process code and task dispatch for both single and recurring tasks
    task_monitor.py            # monitor code used by queue core to detect/handle hung workers
    tasks/
      __init__.py              # module discovery / registry loading
      classify_ds.py           # ClassifyDsTask.taskType = "classifyDs"
      detailize_col.py         # DetailizeColTask.taskType = "detailizeCol"
      ...
```

No separate recurring scheduler is needed for the first implementation. Recurring tasks can stay in the same `tasks` collection and be claimed by the same worker loop; only the status transition after completion differs from `single` tasks.

The queue should be started through a dedicated queue entry point, not through Flask and not as fully independent worker/monitor scripts. The queue core process owns worker and monitor subprocesses: it launches workers, records their process IDs in `runBy`, starts the monitor loop, and can terminate/restart a worker when the monitor detects that a task exceeded its timeout.

#### Task document model
- Store concrete work items in a `task_active` collection.
- Suggested fields:
  - `taskType`: key of the task class.
  - `executionType`: either `single` or `recurring`.
  - `executionInterval`: interval in seconds for recurring tasks.
  - `payload`: JSON-like task input.
  - `status`: status from the workflow matching `executionType`.
  - `error`: last exception trace while the failed or timed-out execution is still in `task_active` before archival.
  - `runAt`: last attempted execution time.
  - `runBy`: worker process ID that claimed the task.
  - `timeout`: maximum execution duration in seconds, set when the task is created.
  - `_createdAt`, `_updatedAt` are added by the hook in `db.py`. 

Finished `single` tasks should not stay in the operational `task_active` collection forever. On success, delete the task document after the domain data is updated. On failure, move the task document with `error`, `failedAt`, and the latest worker metadata to a separate `task_archive` collection, then delete it from `task_active`.

#### Statuses
- `pending`: task is queued and can be claimed by a worker
  - if `executionType` is `single` – when it exists in the collection;
  - if `executionType` is `recurring` – when `runAt` is not set yet or `runAt + executionInterval <= now`.
- `running`: worker has claimed the task and recorded `runBy`/`runAt`.
- `waiting`: recurring task is not due yet: it has already run and `runAt + executionInterval > now`.
- `paused`: recurring task is intentionally disabled but can be resumed.
- `timedOut`: task was detected as hung by the timeout monitor.

There is no permanent `done` status in `task_active` for the first implementation: completed `single` tasks are deleted, while completed `recurring` tasks go back to `waiting`. There is also no permanent `failed` status in `task_active`: failed tasks are moved to `task_archive` for manual analysis.

#### `single` task status workflow
| Event | From status | To status | Comment |
| --- | --- | --- | --- |
| Enqueue | none | `pending` | A process or script creates a one-time task. |
| Claim | `pending` | `running` | Worker atomically sets `runBy` and `runAt`. |
| Complete | `running` | none | Worker updates domain DB state and deletes the task from `task_active`. |
| Fail | `running` | none | Worker calls `onFailure(...)` if defined, copies the task to `task_archive`, and deletes it from `task_active`. No automatic retries for now. |
| Timeout detected | `running` | `timedOut` | Monitor marks the task as timed out and handles the hung worker process. |
| Timeout archived | `timedOut` | none | Monitor or operator copies the task to `task_archive` and deletes it from `task_active`. |

#### `recurring` task status workflow
| Event | From status | To status | Comment |
| --- | --- | --- | --- |
| Create | none | `pending` | A process or script creates a recurring task with `executionInterval`; because `runAt` is not set yet, the first execution is due immediately unless the task is created as `paused`. |
| Due | `waiting` | `pending` | Worker or queue core treats the task as executable when `runAt + executionInterval <= now`. This can be an explicit update or part of the claim query. |
| Claim | `pending` | `running` | Worker atomically claims the due recurring task and sets `runBy` and `runAt`. |
| Complete | `running` | `waiting` | Worker updates domain DB state and clears lock/error fields. The next due time is calculated from `runAt + executionInterval`. |
| Fail | `running` | `waiting` | Worker calls `onFailure(...)` if defined, copies execution details to `task_archive`, clears the lock, and schedules the next run. No automatic retries for now. |
| Pause | `waiting`/`pending` | `paused` | Operator disables future runs without deleting the task definition. |
| Resume | `paused` | `waiting`/`pending` | Operator re-enables the task; if `runAt` is missing or `runAt + executionInterval <= now`, it is due immediately, otherwise it waits. |
| Timeout detected | `running` | `timedOut` | Monitor marks the current execution as timed out and handles the hung worker process. |
| Timeout archived | `timedOut` | `waiting` | Monitor/operator copies timeout details to `task_archive`, clears the lock, and schedules the next run. |

#### Timeout handling
- Workers should only claim tasks by atomically changing `status` from `pending` to `running` and setting `runBy`/`runAt`; they do not decide when the task expires.
- Task timeout should be configured on task creation in the `timeout` field, so different task types or individual tasks can have different execution limits.
- The queue core owns timeout handling by running monitor code in the same process tree as the workers. The monitor periodically finds `running` tasks where `runAt + timeout <= now`, marks them as `timedOut`, records them in `task_archive`, and asks the queue core to terminate/restart the worker whose process ID is stored in `runBy`.
- Because the queue core launches the workers itself, killing/restarting a timed-out worker can be implemented with normal OS process control while keeping ownership local to the queue entry point.

#### Architecture
- Task producers, such as Flask handlers, scripts, or task modules:
  - create `single` task documents and `recurring` task definitions in `task_active`;
  - set initial fields like `taskType`, `executionType`, `executionInterval`, `payload`, `status`, and `timeout`;
- Queue core:
  - runs as the separate queue entry point, for example `scripts/run_task_queue.py`;
  - loads task modules dynamically and passes the registry to worker processes;
  - starts and supervises a configured number of worker subprocesses;
  - starts monitor logic in the same process tree;
  - atomically claim a suitable task from the DB;
  - complete tasks by deleting `single` tasks or rescheduling `recurring` tasks;
  - fail tasks by archiving them to `task_archive` and applying the correct next-state logic;
  - terminates and restarts worker subprocesses that exceeded task timeout;
- Worker:
  - run as a subprocess managed by the queue core;
  - claim a task through the queue core;
  - execute the task;
  - update the task record in the DB through the queue core;
- Monitor:
  - run under the queue core, not as an unrelated standalone service;
  - detect timed-out running tasks;
  - archive timed-out executions to `task_archive`;
  - notify the queue core which worker process must be terminated and replaced;

#### Updating classification and detailization
- Current `call_classify_cells(...)` and `call_get_details_for_cells(...)` rely on in-process `Future` callbacks for failure handling.
- Durable workers cannot return values to the original web process, so task handlers must update MongoDB directly.
- `classify_ds` task handler should:
  - set `ds_list.classification.status = "in progress"` when execution starts;
  - run the existing `classify_cells(...)` logic;
  - write classification rows to `ds_classification[ds_id]`;
  - set `ds_list.classification.status = "finished"` on success;
  - set `ds_list.classification.status = "failed"` with an error on permanent failure;
  - enqueue follow-up detailization tasks after successful classification.
- `detailize_col` task handler should:
  - set `ds_list.detailization.<col>.status = "in progress"` when execution starts;
  - run the existing detailization logic for one column/detailizer;
  - write `details` back to `ds_classification[ds_id]` records;
  - set `ds_list.detailization.<col>.status = "finished"` and store labels on success;
  - set `ds_list.detailization.<col>.status = "failed"` with an error on permanent failure.
- `process_in_parallel(...)` can remain as a local optimization inside task handlers, but it should not be used as the durable scheduling layer.

#### Important implementation details
- Task payloads must be JSON-serializable and backward-compatible across deployments; do not store Python callables or other runtime-dependent objects.
- Handlers should be idempotent because a worker can crash after doing DB writes but before marking the task completed.
- Keep task core generic; concrete workflow chaining belongs in task modules, not in the scheduler.

## Completed Tasks

### Image Uploading
- **Endpoints:**
  - `POST /image/<image_type>`: Upload an image. Expects file in `file` or `<image_type>` field.
  - `GET /image/<image_type>/<filename>`: Retrieve an uploaded image.
- **Use cases:**
  - Avatars
  - Images in data sheets (not implemented yet)
- **Logic:**
  - Validates image properties (size, dimensions) based on `IMAGE_CONFIG` in `app/image.py`.
  - Checks permissions based on `IMAGE_CONFIG`.
  - Saves files to MongoDB GridFS with a unique ID appended to the filename.
- **Files:** `app/image_api.py`, `app/image.py`

### User Signup
- **Endpoints:**
  - `POST /user/signup`: Initiates user registration.
  - `GET /user/confirm/<confirmation_hash>`: Confirms registration via email link.
- **Logic:**
  - Implements a two-step registration process.
  - `signup` adds data to `toConfirm` field in the database and sends a confirmation email.
  - `confirm` moves data from `toConfirm` to the main user record and removes the confirmation hash.
  - Uses MD5 hashing with a salt for local passwords.
  - Prevents hijacking existing logins but allows restarting registration if email is not confirmed.
- **Files:** `app/user.py`
