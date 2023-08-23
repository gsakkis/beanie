from typing import Optional

from pymongo.client_session import ClientSession
from typing_extensions import Self


class BaseQuery:
    """Base class of all queries"""

    def __init__(self):
        self.session: Optional[ClientSession] = None

    def set_session(self, session: Optional[ClientSession] = None) -> Self:
        """
        Set pymongo session
        :param session: Optional[ClientSession] - pymongo session
        :return: self
        """
        if session is not None:
            self.session = session
        return self
