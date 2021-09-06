import asyncio
from datetime import timedelta


class QuotaManager:
    # dostawanie quoty -> zużywanie quoty i czekanie jak jej nie ma
    # reagowanie na update'y by ją znormalizować
    # ? przesuwanie quoty w ciągu dnia pomiędzy updateami

    def __init__(self, quota: int, quota_update_value: int, quota_reset_period_h: float):
        self._quota: int = quota
        self._quota_update_value: int = quota_update_value
        self.update_attempt_period_s = timedelta(
            hours=quota_reset_period_h).total_seconds()
        self.updater = asyncio.create_task(self.update_loop())

    def __del__(self):
        self.updater.cancel()

    async def update_loop(self):
        while True:
            self.update_taks = asyncio.create_task(self.update_quota())
            await self.update_taks

    async def update_quota(self):
        self._quota = self._quota_update_value
        await asyncio.sleep(self.update_attempt_period_s)

    async def get_quota(self, quota_usage: int):
        while True:
            if self._quota >= quota_usage:
                self._quota -= quota_usage
                break
            else:
                await self.update_taks

    def quota_exceeded(self):
        self._quota = 0

    def read_quota(self):
        return self._quota
