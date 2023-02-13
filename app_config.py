import orjson
import typing

from fastapi.responses import JSONResponse
from fastapi import FastAPI


class ORJSONResponse(JSONResponse):
    """Custom JSONResponse class for returning NaN float values in JSON."""
    media_type = "application/json"

    def render(self, content: typing.Any) -> bytes:
        return orjson.dumps(content)


app = FastAPI(default_response_class=ORJSONResponse)
