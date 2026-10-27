# coding: utf-8

import abc


class BaseRateLimiter(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def acquire(self) -> None:
        """获取一个处理名额，不满足限流条件时阻塞等待直到放行"""

        pass
