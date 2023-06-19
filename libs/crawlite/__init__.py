from libs.crawlite._core import Queue, Interceptor, Downloader, Handler
from libs.crawlite._core import Request, Response
from libs.crawlite._core import Crawler
from libs.crawlite._downloaders import HttpDownloader
from libs.crawlite._queues import MemQueue


def new_default_crawlite(worker_size=10) -> Crawler:
    crawler = Crawler(worker_size=worker_size)
    crawler.queue = MemQueue()
    crawler.add_downloader(HttpDownloader)
    return crawler
