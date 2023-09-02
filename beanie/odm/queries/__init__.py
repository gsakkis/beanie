from typing import Any, Optional

from pymongo.client_session import ClientSession
from typing_extensions import Self


class BaseQuery:
    """Base class of all queries"""

    def __init__(
        self, session: Optional[ClientSession] = None, **pymongo_kwargs: Any
    ):
        self.session = session
        self.pymongo_kwargs = pymongo_kwargs

    def set_session(self, session: Optional[ClientSession] = None) -> Self:
        """
        Set pymongo session
        :param session: Optional[ClientSession] - pymongo session
        :return: self
        """
        if session is not None:
            self.session = session
        return self
