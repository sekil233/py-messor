# coding: utf-8

from abc import ABCMeta, abstractmethod


class BaseTask(metaclass=ABCMeta):

    @abstractmethod
    def running(self) -> bool:
        """Return True if the task is currently executing."""

        pass

    @abstractmethod
    def done(self) -> bool:
        """Return True if the task was cancelled or finished executing."""

        pass

    @abstractmethod
    def cancel(self) -> bool:
        """Cancel the task if possible.

        Returns True if the task was cancelled, False otherwise. A task
        cannot be cancelled if it is running or has already completed.
        """

        pass
