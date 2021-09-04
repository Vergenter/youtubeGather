from datetime import datetime, time
import logging
import asyncio
import os
import pytz
from datetime import datetime, time, timedelta
import aiokafka
LOGGER_NAME = "QUOTA"
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=os.environ.get("LOGLEVEL", "INFO"), datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(LOGGER_NAME)


TIMEZONE = pytz.timezone('US/Pacific')
ERROR_MARGIN = timedelta(minutes=1)


async def main():

    producer = aiokafka.AIOKafkaProducer(bootstrap_servers='kafka:9092')
    await producer.start()
    try:
        while True:
            today = datetime.now(TIMEZONE)
            sleep_time = datetime.combine(
                today+timedelta(days=1), time.min, TIMEZONE)-today+ERROR_MARGIN
            await asyncio.sleep(sleep_time.total_seconds())
            await producer.send_and_wait("updates", key=b'update', value=b'quotaRefresh')
            log.info("quotaRefresh emited")

    finally:
        await producer.stop()
        log.info("Quota update stopped")


if __name__ == "__main__":
    log.info("Quota update started")
    asyncio.run(main())
