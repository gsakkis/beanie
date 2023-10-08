from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional

from pymongo.client_session import ClientSession

from beanie.odm.bulk import BulkWriter
from beanie.odm.operators.update import CurrentDate, Inc, Set
from beanie.odm.queries import FieldNameMapping

if TYPE_CHECKING:
    from beanie.odm.queries.update import UpdateQuery


class UpdateMethods:
    """Update methods"""

    @abstractmethod
    def update(
        self,
        *expressions: FieldNameMapping,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> "UpdateQuery":
        ...

    def set(
        self,
        expression: FieldNameMapping,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> "UpdateQuery":
        """
        Set values

        Example:

        ```python

        class Sample(Document):
            one: int

        await Document.find(Sample.one == 1).set({Sample.one: 100})

        ```

        Uses [Set operator](operators/update.md#set)

        :param expression: Dict[Union[ExpressionField, str], Any] - keys and
        values to set
        :param session: Optional[ClientSession] - pymongo session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return self.update(
            Set(expression),
            session=session,
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        )

    def current_date(
        self,
        expression: FieldNameMapping,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> "UpdateQuery":
        """
        Set current date

        Uses [CurrentDate operator](operators/update.md#currentdate)

        :param expression: Dict[Union[ExpressionField, str], Any]
        :param session: Optional[ClientSession] - pymongo session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return self.update(
            CurrentDate(expression),
            session=session,
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        )

    def inc(
        self,
        expression: FieldNameMapping,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> "UpdateQuery":
        """
        Increment

        Example:

        ```python

        class Sample(Document):
            one: int

        await Document.find(Sample.one == 1).inc({Sample.one: 100})

        ```

        Uses [Inc operator](operators/update.md#inc)

        :param expression: Dict[Union[ExpressionField, str], Any]
        :param session: Optional[ClientSession] - pymongo session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return self.update(
            Inc(expression),
            session=session,
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        )
