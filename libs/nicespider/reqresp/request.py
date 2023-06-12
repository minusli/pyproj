import json
from typing import Any, Dict


class Request:
    def __init__(self, url: str, **kwargs):
        self.url: str = url
        self.ctx: Dict[str, Any] = kwargs

    def dump(self) -> str:
        return json.dumps({'url': self.url, 'ctx': self.ctx})

    def load(self, content: str) -> "Request":
        data = json.loads(content)
        self.url = data['url']
        self.ctx = data.get('ctx', {})
        return self
