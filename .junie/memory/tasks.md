# Project Tasks

## Roadmap / Pending Tasks

### DS update notifications

Allow users to subscribe to updates of a certain DS.

#### Subscription model

User may subscribe to a given DS by name. Once a new DS with that name is uploaded, a task checks if it has new data missing in the latest previous DS with the same name and `status = "old"`. DSes with `failed` and other irrelevant statuses are ignored for comparison.

New `ds_subscription` collection contains:
- `dsName`: name of the DS;
- `query`: MongoDB query to filter documents in both old and new DS collections. For the first implementation, only simple `{key: value}` equality queries are supported;
- `fields`: a list of one or more fields, by which we consider uniqueness. If there is a tuple of these fields for some document in the new DS that is missing in the old one, DS is considered updated and this document is considered new;
- `message`: a message to send to subscribers. Only one message is sent per DS update. It should be formatted against a dict that contains key-values of any new document and special values:
  - `$#`: count of new documents;
  - `$url`: link to the new uploaded DS in format `<base_url>/?id=<ds_id>`;
- `userIds`: a list of user IDs subscribed to this subscription.

If a message template references a missing field, the task should raise the standard `KeyError`.

We may also want to update `app_user` collection to add something like `{"settings": {"notificationChannels": ["email"]}}`.

#### Endpoints

- `POST /subscribe`: create a new subscription and subscribe current user to it. Only for authorized users.

  Request body:
  ```json
  {
    "dsName": "prices.csv",
    "query": {"category": "books"},
    "fields": ["sku", "shop"],
    "message": "Found {$#} new prices, including {sku}: {$url}"
  }
  ```

  Response body:
  ```json
  {
    "item": {
      "_id": "subscription_id",
      "dsName": "prices.csv",
      "query": {"category": "books"},
      "fields": ["sku", "shop"],
      "message": "Found {$#} new prices, including {sku}: {$url}",
      "subscribed": true
    },
    "success": true
  }
  ```

- `POST /subscribe/<subscription_id>`: subscribe current user to an existing subscription. Only for authorized users.

  Response body:
  ```json
  {
    "success": true
  }
  ```

- `POST /unsubscribe`: unsubscribe current user from all subscriptions. Only for authorized users.

  Response body:
  ```json
  {
    "success": true
  }
  ```

- `POST /unsubscribe/<subscription_id>`: unsubscribe current user from the given subscription. Only for authorized users.

  Response body:
  ```json
  {
    "success": true
  }
  ```

- `GET /ds/subscriptions?ds_name=...`: get subscriptions by DS name in format: `_id`, `dsName`, `query`, `fields`, `message`, `subscribed` (if current user is subscribed).

  Response body:
  ```json
  {
    "list": [
      {
        "_id": "subscription_id",
        "dsName": "prices.csv",
        "query": {"category": "books"},
        "fields": ["sku", "shop"],
        "message": "Found {$#} new prices, including {sku}: {$url}",
        "subscribed": true
      }
    ],
    "success": true
  }
  ```

- `GET /subscriptions`: get all current user subscriptions in format: `_id`, `dsName`, `query`, `fields`, `message`.

  Response body:
  ```json
  {
    "list": [
      {
        "_id": "subscription_id",
        "dsName": "prices.csv",
        "query": {"category": "books"},
        "fields": ["sku", "shop"],
        "message": "Found {$#} new prices, including {sku}: {$url}"
      }
    ],
    "success": true
  }
  ```

- `POST /subscriptions/<subsciption_id>/test`: test sending a message for the subscriptions.
  - Instead of matching changing documents, take the first document in the active DS by name.
  - If DS doesn't exist or empty, return `{"error": "DS is missing or empty: {name}"}`, 404.
  - Proceed with sending a subscription message for the current user only.
  - If there is no exception, respond with `{"success": true}`.
  - If there is an exception, the response will be handled with the Flask exception handler.

  Response body:
  ```json
  {
    "success": true
  }
  ```

#### DS processing logic

- When a new DS is uploaded, create a task that will do the following:
  - find the latest previous DS with the same `name` and `status = "old"`;
  - iterate subscriptions by DS name;
  - run the subscription `query` on both old and new DS collections;
  - for each subscription, check if DS is updated in terms of this subscription, by the logic described above;
  - if it is updated, send one message to the subscribers via their configured notification channels, default is `email`.

## Completed Tasks

### Task Scheduling

Implemented a durable MongoDB-backed task system that replaces in-memory async callbacks for workflows that must survive service restarts.

#### Goals
- Supports the current workflow where the web process schedules work and independent worker processes execute it.
- Survives service restarts without losing queued or running tasks.
- Supports both one-time and recurring task execution.
- Allows adding a new task type by adding a module with a defined structure, without changing scheduler or worker core code.
- Moves async classification and detailization side effects into task handlers, because independent workers cannot return values to the original caller.

#### Module-based task types
- Task implementations live in modules under `app/scheduler/tasks/`.
- Each task module defines one task class inherited from `Task`.
- `Task` defines the standard task interface:
  - `validate(payload)`: validates payload before a task document is created.
  - `execute(payload)`: executes the task and updates all affected DB state itself. No return value is used by the scheduler or worker.
- Worker startup discovers task classes dynamically and uses the `Task.sub(...)` / `Task.get(...)` registry from `SingletonMixin`.
- Queue/worker core should store and route by `taskType`, but should not contain hardcoded knowledge of concrete tasks like classification or detailization.

Example module layout:

```text
app/
  scheduler/
    task_interface.py          # task API, task DB state transitions, workflows, archive/delete
    task_queue.py              # queue entry point that starts and manages workers and monitor
    task_worker.py             # worker process code and task dispatch for both single and recurring tasks
    task_monitor.py            # monitor code used by queue core to detect/handle hung workers
    tasks/
      __init__.py              # module discovery / registry loading
      classify_ds.py           # @Task.sub("classify_ds")
      detailize_cols.py        # @Task.sub("detailize_cols")
      cleanup_old_ds.py        # @Task.sub("cleanup_old_ds")
      ...
```

No separate recurring scheduler is needed for the first implementation. Recurring tasks stay in the same `task_active` collection and are claimed by the same worker loop; only the status transition after completion differs from `single` tasks.

The queue is started through `run_task_queue.py`, not through Flask and not as fully independent worker/monitor scripts. The queue core process owns worker and monitor subprocesses: it launches workers, records their process IDs in `runBy`, starts the monitor loop, and can terminate/restart a worker when the monitor detects that a task exceeded its timeout.

#### Task document model
- Store concrete work items in a `task_active` collection.
- Suggested fields:
  - `taskType`: key of the task class.
  - `executionType`: either `single` or `recurring`.
  - `executionInterval`: interval in seconds for recurring tasks.
  - `payload`: JSON-like task input.
  - `status`: status from the workflow matching `executionType`.
  - `error`: last exception trace while the failed or timed-out execution is still in `task_active` before archival, or on a recurring task until the next claim clears it.
  - `runAt`: last attempted execution time.
  - `runBy`: worker process ID that claimed the task.
  - `timeout`: maximum execution duration in seconds, set when the task is created.
  - `_createdAt`, `_updatedAt` are added by the hook in `db.py`. 

Finished `single` tasks do not stay in the operational `task_active` collection forever. On success, the task document is deleted after the domain data is updated. On failure or timeout, the task document is updated with terminal execution data, moved to `task_archive`, and deleted from `task_active`.

#### Statuses
- `pending`: task is queued and can be claimed by a worker
  - if `executionType` is `single` – when it exists in the collection;
  - if `executionType` is `recurring` – when `runAt` is not set yet or `runAt + executionInterval <= now`.
- `running`: worker has claimed the task and recorded `runBy`/`runAt`.
- `waiting`: recurring task is not due yet: it has already run and `runAt + executionInterval > now`.
- `paused`: recurring task is intentionally disabled but can be resumed.
- `failed`: recurring task execution failed; it remains claimable when the next interval is due.
- `timedOut`: task was detected as hung by the timeout monitor; recurring tasks remain claimable when the next interval is due.

There is no permanent `done` status in `task_active` for the first implementation: completed `single` tasks are deleted, while completed `recurring` tasks go back to `waiting`. Failed or timed-out `single` tasks are moved to `task_archive` for manual analysis. Failed or timed-out recurring tasks stay in `task_active` with their last error and can be claimed again when due.

#### `single` task status workflow
| Event | From status | To status | Comment |
| --- | --- | --- | --- |
| Enqueue | none | `pending` | A process or script creates a one-time task. |
| Claim | `pending` | `running` | Worker atomically sets `runBy` and `runAt`. |
| Complete | `running` | none | Worker updates domain DB state and deletes the task from `task_active`. |
| Fail | `running` | none | Worker records the error, copies the task to `task_archive`, and deletes it from `task_active`. No automatic retries for now. |
| Timeout detected | `running` | none | Monitor records timeout data, copies the task to `task_archive`, deletes it from `task_active`, and handles the hung worker process. |

#### `recurring` task status workflow
| Event | From status | To status | Comment |
| --- | --- | --- | --- |
| Create | none | `pending` | A process or script creates a recurring task with `executionInterval`; because `runAt` is not set yet, the first execution is due immediately unless the task is created as `paused`. |
| Due | `waiting` | `pending` | Worker or queue core treats the task as executable when `runAt + executionInterval <= now`. This can be an explicit update or part of the claim query. |
| Claim | `pending` | `running` | Worker atomically claims the due recurring task and sets `runBy` and `runAt`. |
| Complete | `running` | `waiting` | Worker updates domain DB state and clears lock/error fields. The next due time is calculated from `runAt + executionInterval`. |
| Fail | `running` | `failed` | Worker records the error on the active task and clears the lock. The task can be claimed again when `runAt + executionInterval <= now`. |
| Pause | `waiting`/`pending` | `paused` | Operator disables future runs without deleting the task definition. |
| Resume | `paused` | `waiting`/`pending` | Operator re-enables the task; if `runAt` is missing or `runAt + executionInterval <= now`, it is due immediately, otherwise it waits. |
| Timeout detected | `running` | `timedOut` | Monitor records timeout data on the active task, clears the lock, and handles the hung worker process. The task can be claimed again when `runAt + executionInterval <= now`. |

#### Timeout handling
- Workers should only claim tasks by atomically changing `status` from `pending` to `running` and setting `runBy`/`runAt`; they do not decide when the task expires.
- Task timeout should be configured on task creation in the `timeout` field, so different task types or individual tasks can have different execution limits.
- The queue core owns timeout handling by running monitor code in the same process tree as the workers. The monitor periodically finds `running` tasks where `runAt + timeout <= now`, calls the workflow `timeout()` method, and asks the queue core to terminate/restart the worker whose process ID is stored in `runBy`.
- Because the queue core launches the workers itself, killing/restarting a timed-out worker can be implemented with normal OS process control while keeping ownership local to the queue entry point.

#### Architecture
- Task producers, such as Flask handlers, scripts, or task modules:
  - call `create_task(...)` to create `single` task documents and `recurring` task definitions in `task_active`;
  - rely on `create_task(...)` to validate payloads, set initial fields like `taskType`, `executionType`, `executionInterval`, `payload`, `status`, and `timeout`, and insert the DB record;
- Queue core:
  - runs as the separate queue entry point, for example `scripts/run_task_queue.py`;
  - loads task modules dynamically and passes the registry to worker processes;
  - starts and supervises a configured number of worker subprocesses;
  - starts monitor logic in the same process tree;
  - atomically claims suitable tasks from the DB;
  - completes tasks by deleting `single` tasks or rescheduling `recurring` tasks;
  - fails or times out tasks by applying the execution-type-specific `TaskWorkflow` logic;
  - terminates and restarts worker subprocesses that exceeded task timeout;
- Worker:
  - run as a subprocess managed by the queue core;
  - claim a task through the queue core;
  - execute the task;
  - update the task record in the DB through `TaskWorkflow` methods;
- Monitor:
  - run under the queue core, not as an unrelated standalone service;
  - detect timed-out running tasks;
  - call `TaskWorkflow.timeout()` for timed-out executions;
  - notify the queue core which worker process must be terminated and replaced;

#### Updating classification and detailization
- Current `call_classify_cells(...)` and `call_get_details_for_cells(...)` rely on in-process `Future` callbacks for failure handling.
- Durable workers cannot return values to the original web process, so task handlers must update MongoDB directly.
- `classify_ds` task handler:
  - set `ds_list.classification.status = "in progress"` when execution starts;
  - run the existing `classify_cells(...)` logic;
  - write classification rows to `ds_classification[ds_id]`;
  - set `ds_list.classification.status = "finished"` on success;
  - sets `ds_list.classification.status = "failed"` with an error on failure;
  - enqueue follow-up detailization tasks after successful classification.
- `detailize_cols` task handler:
  - selects applicable detailizers for the dataset;
  - sets `ds_list.detailization.<col>.status = "in progress"` for each column/detailizer when execution starts;
  - runs the existing detailization logic;
  - writes `details` back to `ds_classification[ds_id]` records;
  - sets `ds_list.detailization.<col>.status = "finished"` and stores labels on success;
  - sets `ds_list.detailization.<col>.status = "failed"` with an error on failure.
- `process_in_parallel(...)` can remain as a local optimization inside task handlers, but it should not be used as the durable scheduling layer.

#### New task types
- `cleanup_old_ds` task handler:
  - find all DSes by a given `name` with `status` "old";
  - if there are more than N old DSes, remove all but N last ones:
    - drop the corresponding DS collection;
    - drop the corresponding DS classification collection;
    - delete `ds_list` record.

#### Important implementation details
- Task payloads must be JSON-serializable and backward-compatible across deployments; do not store Python callables or other runtime-dependent objects.
- Handlers should be idempotent because a worker can crash after doing DB writes but before marking the task completed.
- Keep task core generic; concrete workflow chaining belongs in task modules, not in the scheduler.

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
