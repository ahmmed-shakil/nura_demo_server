from datetime import datetime, timezone
from typing import Annotated
from fastapi import Query


class CommonParams:
    def __init__(
        self,
        imo: Annotated[int, Query(gt=0)],
        start_date: Annotated[
            int,
            Query(
                description="Start date timstamp of the requested period. Default is 1 week ago.",
                default_factory=(lambda: int(datetime.now(timezone.utc).timestamp() - 604800)),
                ge=0,
            ),
        ],
        end_date: Annotated[
            int,
            Query(
                description="End date timstamp of the requested period. Default is now.",
                default_factory=(lambda: int(datetime.now(timezone.utc).timestamp())),
                gt=0,
            ),
        ],
    ):
        self.imo = imo
        self.start_date = start_date
        self.end_date = end_date
