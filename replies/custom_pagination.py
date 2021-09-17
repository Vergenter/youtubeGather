import asyncio
import logging
import os
from aiogoogle.excs import HTTPError
from quota_manager import QuotaManager
from aiogoogle.models import Response
LOGGER_NAME = "REPLIES"
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=os.environ.get("LOGLEVEL", "INFO"), datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(LOGGER_NAME)


async def get_pages(
    arg_response: Response,
    quota: QuotaManager,
    request_cost: int
):
    prev_res = arg_response
    session_factory = arg_response.session_factory
    req_token_name = None
    res_token_name = None
    json_req = False
    prev_url = None
    while prev_res is not None:

        # Avoid infinite looping if google sent the same token twice
        if prev_url == prev_res.req.url:
            break
        prev_url = prev_res.req.url

        # yield
        yield prev_res.content

        # get request for next page
        next_req = prev_res.next_page(
            req_token_name=req_token_name,
            res_token_name=res_token_name,
            json_req=json_req,
        )
        error_count = 3
        if next_req is not None:
            while True:
                try:
                    # await quota reset if quota exceeded
                    await quota.get_quota(request_cost)
                    async with session_factory() as sess:
                        prev_res = await sess.send(next_req, full_res=True)
                    error_count = 3
                    break
                except HTTPError as err:
                    if err.res.status_code == 403 and err.res.content['error']['errors'][0]['reason'] == "quotaExceeded":
                        log.error("QuotaExceeded error in pagination")
                        # mark quota as exceeded
                        quota.quota_exceeded()
                        continue
                    elif err.res.status_code == 400 and err.res.content['error']['errors'][0]['reason'] == "processingFailure":
                        if error_count > 0:
                            error_count -= 1
                            await asyncio.sleep(0.1)
                            continue
                    raise
        else:
            prev_res = None
