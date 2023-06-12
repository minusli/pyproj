from libs.nicespider.downloader import HttpDownloader
from libs.nicespider.spider._spider import Spider


def new_web_spider() -> Spider:
    spider = Spider()
    spider.add_downloader(HttpDownloader)
    return spider
