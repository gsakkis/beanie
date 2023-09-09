from typing import Optional, Union

import bson

from beanie import Document, Link, PydanticObjectId
from beanie.odm.utils.typing import extract_id_class


class Lock(Document):
    k: int


class TestTyping:
    def test_extract_id_class(self):
        # Union
        assert extract_id_class(Union[str, int]) == str
        assert extract_id_class(Union[str, None]) == str
        assert extract_id_class(Union[str, None, int]) == str
        # Optional
        assert extract_id_class(Optional[str]) == str
        # Link
        assert extract_id_class(Link[Lock]) == Lock
        # PydanticObjectId
        assert extract_id_class(PydanticObjectId) == bson.ObjectId
        assert extract_id_class(Optional[PydanticObjectId]) == bson.ObjectId
