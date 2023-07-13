from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat,
    StrictBool,
    conlist
)
from typing import (
    Union, 
    List, 
    Literal,
    Optional
)


class CorsHeaders(BaseModel):
    access_control_allow_origin: conlist(
        StrictStr,
        min_items=1
    )
    access_control_expose_headers: Optional[List[StrictStr]]
    access_control_max_age: Optional[Union[StrictInt, StrictFloat]]
    access_control_allow_credentials: Optional[StrictBool]
    access_control_allow_methods: conlist(
        Literal[
            "GET",
            "HEAD",
            "OPTIONS",
            "POST",
            "PUT",
            "PATCH",
            "DELETE",
            "TRACE"
        ],
        min_items=1
    )
    access_control_allow_headers: Optional[List[StrictStr]]

    def to_headers(self):

        cors_headers = {}

        headers = self.dict(exclude_none=True)

        for key, value in headers.items():
            
            header_key = '-'.join([
                segment.capitalize() for segment in key.split('_')
            ])

            if key == 'access_control_allow_origin':
                header_value = ' | '.join(value)

            elif key == 'access_control_max_age':
                header_value = "true" if value else "false"

            else:
                header_value = ', '.join(value)

            cors_headers[header_key] = header_value

        return cors_headers