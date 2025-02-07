from __future__ import annotations

import datetime
from collections import OrderedDict
from typing import Callable


class TaskStatus:
    def __init__(self, label: str, is_terminal: bool = False, is_bad: bool = False):
        self.label: str = label
        self.is_terminal: bool = is_terminal
        self.is_bad: bool = is_bad

    def __str__(self) -> str:
        return self.label


Not_Started = TaskStatus("Not Started")
Preparing = TaskStatus("Preparing")
In_Progress = TaskStatus("In Progress")
Finishing = TaskStatus("Finishing")
Success = TaskStatus("Success", is_terminal=True)
Failed = TaskStatus("Failed", is_terminal=True, is_bad=True)
Skipped = TaskStatus("Skipped", is_terminal=True)
Children_Failed = TaskStatus("Failed Sub-Tasks", is_terminal=True, is_bad=True)


class Loggable:
    def __init__(self, status_logger: JobLogger):
        self.logger = status_logger

    def log(self, msg: str):
        self.logger.log(msg)

    def set_status(self, status: str):
        self.logger.status_text = status

    def log_status(self, status_msg):
        self.logger.log_status(status_msg)


def default_log_func(context_path: list[str], message: str):
    print(f"{datetime.datetime.now()} [{' : '.join(context_path)}] {message}")


class JobLogger:

    def __init__(self, name: str, parent: JobLogger = None, include_parent=True,
                 log_func: Callable[[list[str], str], None] = default_log_func):
        self._status_type: TaskStatus = Not_Started
        self.name = name
        self.parent = parent
        self.include_parent = include_parent
        self.messages: list[str] = []
        self._children: OrderedDict[str, JobLogger] = OrderedDict()
        self._status_text: str = self._status_type.label
        if log_func is None:
            if parent is None:
                raise ValueError('Either parent or log_func must be specified')
            else:
                self.log_func = parent.log_func
        else:
            self.log_func = log_func

    @property
    def full_path(self) -> list[JobLogger]:
        if self.parent is None or not self.include_parent:
            return [self]
        else:
            return [*self.parent.full_path, self]

    @property
    def full_path_strings(self) -> list[str]:
        return [s.name for s in self.full_path]

    def __str__(self):
        return ' : '.join(self.full_path_strings)

    def log(self, msg):
        self.messages.append(msg)
        self.log_func(self.full_path_strings, msg)

    def log_status(self, status_msg, status_type: TaskStatus = None):
        self.log(status_msg)
        self.status_text = status_msg
        if status_type is not None:
            self.status_type = status_type

    def make_or_replace_child(self, name: str, include_parent: bool = True) -> JobLogger:
        new_child = JobLogger(name, parent=self, include_parent=include_parent, log_func=self.log_func)
        self._children[name] = new_child
        return new_child

    @property
    def children(self) -> OrderedDict[str, JobLogger]:
        return OrderedDict(self._children)

    @property
    def status_text(self) -> str:
        return self._status_text

    @status_text.setter
    def status_text(self, status_text: str):
        assert isinstance(status_text, str)
        self._status_text = status_text

    @property
    def status_type(self) -> TaskStatus:
        return self._status_type

    @status_type.setter
    def status_type(self, status_type: TaskStatus):
        assert isinstance(status_type, TaskStatus)
        if status_type.is_terminal:
            for child in self.children.values():
                if child.status_type == Not_Started:
                    child.log_status('Skipped', Skipped)
        if status_type == Success:
            for child in self.children.values():
                if child.status_type.is_bad:
                    self._status_type = Children_Failed
        self._status_type = status_type


class TopLevelLogger(JobLogger):
    def __init__(self, name: str):
        super().__init__(name=name)
