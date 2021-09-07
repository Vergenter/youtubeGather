import asyncio
from datetime import datetime, timedelta
from typing import Optional


class QuotaManager:
    def __init__(self, quota: int, quota_update_value: int, quota_reset_period_h: float, start_cycle: Optional[datetime] = None):
        start_cycle_or_now = start_cycle or datetime.now()
        self._quota: int = quota
        self.quota_update_value: int = quota_update_value
        self.update_attempt_period_s = timedelta(hours=quota_reset_period_h)
        self._next_update = start_cycle_or_now+self.update_attempt_period_s

    def get_next_update(self):
        now = datetime.now()
        while now > self._next_update:
            self._next_update += self.update_attempt_period_s
            self._quota = self.quota_update_value
        return self._next_update

    async def get_update(self):
        await asyncio.sleep((datetime.now()-self.get_next_update()).total_seconds())

    def read_quota(self):
        self.get_next_update()
        return self._quota

    async def get_quota(self, quota_usage: int):
        while True:
            if self.read_quota() >= quota_usage:
                self._quota -= quota_usage
                break
            else:
                await self.get_update()

    def quota_exceeded(self):
        self.get_next_update()
        self._quota = 0
