from quota_manager import QuotaManager
import logging
import os
from aiogoogle.excs import HTTPError

from aiogoogle.models import Response
LOGGER_NAME = "COMMENTS"
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
        if next_req is not None:
            while True:
                try:
                    await quota.get_quota(request_cost)
                    async with session_factory() as sess:
                        prev_res = await sess.send(next_req, full_res=True)
                    break
                except HTTPError as err:
                    if err.res.status_code == 403 and err.res.content['error']['errors'][0]['reason'] == "quotaExceeded":
                        log.error("QuotaExceeded error in pagination")
                        quota.quota_exceeded()
                        continue
                    raise
        else:
            prev_res = None
