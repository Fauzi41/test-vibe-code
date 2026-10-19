"""Quota-aware task scheduler.

Modules (kept in one file for easy sharing, each section can be split out):
    1. Task model          - Task, TaskStatus
    2. User management     - User, UserManager (quota control)
    3. Task execution      - ActionStrategy (+ built-ins), TaskExecutor
    4. Scheduling          - Scheduler (sync + async)
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, time as dtime
from enum import Enum
from typing import Any, Callable, Iterable, Iterator, Mapping

logger = logging.getLogger("scheduler")


# ---------------------------------------------------------------------------
# 1. Task data model
# ---------------------------------------------------------------------------
class TaskStatus(str, Enum):
    PENDING = "pending"      # waiting for its time, or for quota
    DONE = "done"            # executed successfully
    FAILED = "failed"        # executor raised; not retried
    REJECTED = "rejected"    # unknown user / unsupported action; not retried


def parse_hhmm(value: str | dtime) -> dtime:
    """Accept 'HH:MM' strings or datetime.time objects."""
    return value if isinstance(value, dtime) else datetime.strptime(value, "%H:%M").time()


@dataclass
class Task:
    user_id: str
    action: str
    run_at: dtime
    target: str
    params: dict[str, Any] = field(default_factory=dict)   # per-task, action-specific options
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    status: TaskStatus = TaskStatus.PENDING
    error: str | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Task":
        """Build a task from the original dict format, plus an optional 'params' dict."""
        return cls(
            user_id=data["user"],
            action=data["action"],
            run_at=parse_hhmm(data["time"]),
            target=data["target"],
            params=dict(data.get("params", {})),
        )

    def is_due(self, now: dtime) -> bool:
        return self.status is TaskStatus.PENDING and self.run_at <= now

    def __str__(self) -> str:
        return f"Task[{self.id}] {self.action}({self.target}) user={self.user_id}"


# ---------------------------------------------------------------------------
# 2. User management & quota control
# ---------------------------------------------------------------------------
@dataclass
class User:
    name: str
    quota: int
    executed: int = 0

    @property
    def remaining(self) -> int:
        return max(self.quota - self.executed, 0)


class UserManager:
    """Owns users and their quotas. Not thread-safe (fine for asyncio / single thread)."""

    def __init__(self, users: Mapping[str, Mapping[str, int]] | None = None):
        self._users: dict[str, User] = {}
        for name, cfg in (users or {}).items():          # accepts the original dict layout
            self.add_user(name, **cfg)

    def add_user(self, name: str, quota: int, executed: int = 0) -> User:
        user = self._users[name] = User(name, quota, executed)
        return user

    def get(self, name: str) -> User | None:
        return self._users.get(name)

    def exists(self, name: str) -> bool:
        return name in self._users

    def reserve(self, name: str) -> bool:
        """Atomically check quota and consume one unit. Returns False if exhausted."""
        user = self._users[name]
        if user.remaining == 0:
            return False
        user.executed += 1
        return True

    def release(self, name: str) -> None:
        """Give back a unit (used when a reserved task did not complete)."""
        user = self._users[name]
        user.executed = max(user.executed - 1, 0)

    def reset_quotas(self) -> None:
        for user in self._users.values():
            user.executed = 0


# ---------------------------------------------------------------------------
# 3. Task executor (extensible via strategies)
# ---------------------------------------------------------------------------
class ActionStrategy(ABC):
    """One strategy per action. Subclass, set `name`, implement `execute`."""

    name: str

    @abstractmethod
    def execute(self, task: Task) -> None: ...

    async def aexecute(self, task: Task) -> None:
        """Async entry point. Default: run blocking `execute` in a worker thread.
        Override with a native coroutine when the action is truly async I/O."""
        await asyncio.to_thread(self.execute, task)


class SyncAction(ActionStrategy):
    name = "sync"

    def execute(self, task: Task) -> None:
        mode = task.params.get("mode", "full")
        logger.info("Syncing %s (mode=%s)", task.target, mode)       # real impl here


class BackupAction(ActionStrategy):
    name = "backup"

    def execute(self, task: Task) -> None:
        dest = task.params.get("destination", "/backups")
        compress = task.params.get("compress", True)
        logger.info("Backing up %s -> %s (compress=%s)", task.target, dest, compress)


class DeleteAction(ActionStrategy):
    name = "delete"

    def execute(self, task: Task) -> None:
        recursive = task.params.get("recursive", False)
        if task.params.get("dry_run", False):
            logger.info("[dry-run] would delete %s (recursive=%s)", task.target, recursive)
            return
        logger.info("Deleting %s (recursive=%s)", task.target, recursive)


class UnknownActionError(Exception):
    pass


class TaskExecutor:
    """Registry of strategies + logging around every execution.
    The set of registered strategies replaces the old ALLOWED_ACTIONS whitelist."""

    def __init__(self, strategies: Iterable[ActionStrategy] = ()):
        self._strategies: dict[str, ActionStrategy] = {}
        for strategy in strategies:
            self.register(strategy)

    def register(self, strategy: ActionStrategy) -> None:
        self._strategies[strategy.name] = strategy

    def supports(self, action: str) -> bool:
        return action in self._strategies

    def _strategy_for(self, action: str) -> ActionStrategy:
        try:
            return self._strategies[action]
        except KeyError:
            raise UnknownActionError(action) from None

    @contextmanager
    def _logged(self, task: Task) -> Iterator[None]:
        logger.info("[%s] START %s on %s for %s params=%s",
                    task.id, task.action, task.target, task.user_id, task.params)
        started = time.perf_counter()
        try:
            yield
        except Exception:
            logger.exception("[%s] FAILED after %.3fs", task.id, time.perf_counter() - started)
            raise
        logger.info("[%s] DONE in %.3fs", task.id, time.perf_counter() - started)

    def execute(self, task: Task) -> None:
        strategy = self._strategy_for(task.action)
        with self._logged(task):
            strategy.execute(task)

    async def aexecute(self, task: Task) -> None:
        strategy = self._strategy_for(task.action)
        with self._logged(task):
            await strategy.aexecute(task)


# ---------------------------------------------------------------------------
# 4. Scheduler
# ---------------------------------------------------------------------------
class Scheduler:
    """Holds tasks and runs the ones that are due.

    Per-task outcome (same rules as the original script):
      * unknown user / unsupported action -> REJECTED (never retried)
      * quota exhausted                   -> stays PENDING (retried after reset_quotas)
      * executor raises                   -> FAILED, quota NOT consumed
      * success                           -> DONE, one quota unit consumed
    """

    def __init__(
        self,
        users: UserManager,
        executor: TaskExecutor,
        clock: Callable[[], dtime] | None = None,
    ):
        self.users = users
        self.executor = executor
        self._clock = clock or (lambda: datetime.now().time())
        self._tasks: list[Task] = []

    # -- task intake --------------------------------------------------------
    def add_task(self, task: Task | Mapping[str, Any]) -> Task:
        if not isinstance(task, Task):
            task = Task.from_dict(task)
        self._tasks.append(task)
        return task

    def add_tasks(self, tasks: Iterable[Task | Mapping[str, Any]]) -> list[Task]:
        return [self.add_task(t) for t in tasks]

    @property
    def tasks(self) -> list[Task]:
        return list(self._tasks)

    @property
    def pending(self) -> list[Task]:
        return [t for t in self._tasks if t.status is TaskStatus.PENDING]

    # -- shared helpers -----------------------------------------------------
    def _now(self, now: str | dtime | None) -> dtime:
        return self._clock() if now is None else parse_hhmm(now)

    def _admit(self, task: Task) -> bool:
        """Validate and reserve quota. True means: go ahead and execute."""
        if not self.users.exists(task.user_id):
            logger.warning("[%s] Unknown user: %s", task.id, task.user_id)
            self._reject(task, f"unknown user {task.user_id!r}")
            return False
        if not self.executor.supports(task.action):
            logger.warning("[%s] Action not allowed: %s", task.id, task.action)
            self._reject(task, f"unsupported action {task.action!r}")
            return False
        if not self.users.reserve(task.user_id):
            logger.info("[%s] %s has exceeded quota; task stays pending", task.id, task.user_id)
            return False
        return True

    @staticmethod
    def _reject(task: Task, reason: str) -> None:
        task.status, task.error = TaskStatus.REJECTED, reason

    @staticmethod
    def _complete(task: Task) -> None:
        task.status = TaskStatus.DONE

    def _fail(self, task: Task, exc: Exception) -> None:
        task.status, task.error = TaskStatus.FAILED, repr(exc)
        self.users.release(task.user_id)        # failed runs don't count against quota

    # -- synchronous run ----------------------------------------------------
    def run_due(self, now: str | dtime | None = None) -> list[Task]:
        """Run every due task one after another. Returns the tasks that were attempted."""
        now = self._now(now)
        attempted: list[Task] = []
        for task in [t for t in self._tasks if t.is_due(now)]:
            if not self._admit(task):
                continue
            try:
                self.executor.execute(task)
            except Exception as exc:            # already logged with traceback by executor
                self._fail(task, exc)
            else:
                self._complete(task)
            attempted.append(task)
        return attempted

    # -- asynchronous run ---------------------------------------------------
    async def run_due_async(
        self, now: str | dtime | None = None, max_concurrency: int | None = None
    ) -> list[Task]:
        """Run due tasks concurrently (a user's tasks may run in parallel).

        Quota is reserved *before* anything is awaited, so there is no race between
        'check quota' and 'consume quota', and reservation order follows task order.
        """
        now = self._now(now)
        admitted = [t for t in self._tasks if t.is_due(now) and self._admit(t)]
        semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency else None

        async def run_one(task: Task) -> None:
            try:
                if semaphore:
                    async with semaphore:
                        await self.executor.aexecute(task)
                else:
                    await self.executor.aexecute(task)
            except asyncio.CancelledError:
                self.users.release(task.user_id)    # never ran to completion
                raise
            except Exception as exc:
                self._fail(task, exc)
            else:
                self._complete(task)

        await asyncio.gather(*(run_one(t) for t in admitted))
        return admitted


# ---------------------------------------------------------------------------
# Wiring + demo
# ---------------------------------------------------------------------------
def build_executor() -> TaskExecutor:
    return TaskExecutor([SyncAction(), BackupAction(), DeleteAction()])


def sample_tasks() -> list[dict[str, Any]]:
    return [
        {"user": "alice", "time": "12:00", "action": "sync",   "target": "/data/x", "params": {"mode": "incremental"}},
        {"user": "bob",   "time": "12:00", "action": "backup", "target": "/srv/y",  "params": {"compress": False}},
        {"user": "alice", "time": "12:00", "action": "delete", "target": "/tmp/z",  "params": {"dry_run": True}},
        {"user": "alice", "time": "12:00", "action": "sync",   "target": "/data/a"},
        {"user": "alice", "time": "12:00", "action": "backup", "target": "/data/b"},   # over quota (3)
        {"user": "ghost", "time": "12:00", "action": "sync",   "target": "/data/c"},   # unknown user
        {"user": "bob",   "time": "12:00", "action": "format", "target": "/dev/sda"},  # unsupported action
        {"user": "bob",   "time": "23:59", "action": "sync",   "target": "/data/late"},  # not due yet
    ]


def demo_sync() -> None:
    print("\n=== sync ===")
    users = UserManager({"alice": {"quota": 3, "executed": 0}, "bob": {"quota": 5, "executed": 0}})
    scheduler = Scheduler(users, build_executor())
    scheduler.add_tasks(sample_tasks())

    scheduler.run_due("12:00")
    print("pending after run 1:", [str(t) for t in scheduler.pending])

    users.reset_quotas()                         # e.g. a daily cron job
    scheduler.run_due("12:00")
    print("pending after reset + run 2:", [str(t) for t in scheduler.pending])


def demo_async() -> None:
    print("\n=== async ===")
    users = UserManager({"alice": {"quota": 3}, "bob": {"quota": 5}})
    scheduler = Scheduler(users, build_executor())
    scheduler.add_tasks(sample_tasks())
    asyncio.run(scheduler.run_due_async("12:00", max_concurrency=2))
    print({t.id: t.status.value for t in scheduler.tasks})


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    demo_sync()
    demo_async()