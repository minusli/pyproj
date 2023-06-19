from http import HTTPStatus

import requests

from libs.crawlite._core import Downloader
from libs.crawlite._core import Request, Response
from libs.crawlite._core import Crawler
from libs.crawlite._utils import retry


class HttpDownloader(Downloader):
    def __init__(self, crawler: Crawler):
        super().__init__(crawler)

    def match(self, req: Request) -> bool:
        return req.url.startswith("http:") or req.url.startswith("https:")

    @retry()
    def download(self, req: Request) -> Response:
        resp_ = requests.request(
            url=req.url,
            method=req.ctx.get("method", "GET"),
            params=req.ctx.get("params"),
            data=req.ctx.get("data"),
            json=req.ctx.get("json"),
            cookies=req.ctx.get("cookies"),
            headers=req.ctx.get("headers"),
            proxies=req.ctx.get("proxies"),
            timeout=req.ctx.get("timeout", 3)
        )

        if resp_.status_code != HTTPStatus.OK:
            raise Exception(f"HTTP {resp_.status_code}: url={req.url}")

        return Response(content=resp_.content, headers=resp_.headers, cookies=resp_.cookies)
