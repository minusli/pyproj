import typing
from http import HTTPStatus

import requests

from libs.nicespider import utils
from libs.nicespider.downloader import Downloader
from libs.nicespider.reqresp import Request, Response

if typing.TYPE_CHECKING:
    from libs.nicespider.spider import Spider


class HttpDownloader(Downloader):
    def __init__(self, spider: "Spider"):
        super().__init__(spider)

    def match(self, req: Request) -> bool:
        return req.url.startswith("http:") or req.url.startswith("https:")

    @utils.retry()
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

        return Response(content=resp_.content, **req.ctx)
