import json
from typing import Any, Dict, Union


class Response:
    def __init__(self, content: Union[str | bytes], **kwargs):
        self.content = content
        self.ctx: Dict[str, Any] = kwargs

    def json(self):
        return json.loads(self.content)

    def text(self, encoding='utf-8'):
        if isinstance(self.content, bytes):
            return self.content.decode(encoding)
        return self.content
