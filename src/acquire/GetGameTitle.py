import json
import re
from typing import Tuple
import aiohttp
import asyncio
from typing import Union
from mcache import filecache, YEAR
head = {
    "User-Agent": "Mozilla/5.0 (X11; U; Linux amd64; rv:5.0) Gecko/20100101 Firefox/5.0 (Debian)",
    "X-Requested-With": "XMLHttpRequest"
}
url_start = "https://www.youtube.com/watch?v="

cookies = {'CONSENT': 'YES+cb.20210523-18-p0.pl+FX+213'}


async def getVideoPage(videoId: str):
    try:
        async with aiohttp.ClientSession(headers=head, cookies=cookies) as client:
            async with client.get(url_start+videoId) as resp:
                return await resp.text()
    except aiohttp.ClientError:
        # sleep a little and try again
        await asyncio.sleep(1)
        async with aiohttp.ClientSession(headers=head, cookies=cookies) as client:
            async with client.get(url_start+videoId) as resp:
                return await resp.text()


def getVideoTitleAndSubtitle(pageText: str) -> Tuple[Union[None, str], Union[None, str]]:
    patternFound = re.search(
        '"title":{"simpleText":"(?:[^"]|"")*"},"subtitle":{"simpleText":"[0-9]+"}', pageText)
    title = None
    subtitle = None
    if patternFound:
        datajson = json.loads('{'+patternFound.group()+'}')
        title = datajson["title"]["simpleText"]
        subtitle = datajson["subtitle"]["simpleText"]
    return title, subtitle


@filecache(lifetime=YEAR)
async def getVideoGameTitle(videoId: str):
    return getVideoTitleAndSubtitle((await getVideoPage(videoId)))
