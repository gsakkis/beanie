from typing import Any, Dict, List

import beanie
from beanie.exceptions import NotSupported
from beanie.odm.links import LinkInfo, LinkTypes

# TODO: check if this is the most efficient way for
#  appending subqueries to the queries var


def construct_lookup_queries(cls: type) -> List[Dict[str, Any]]:
    if not issubclass(cls, beanie.Document):
        raise NotSupported(f"{cls} doesn't support link fetching")
    queries: List = []
    link_fields = cls.get_link_fields()
    if link_fields is not None:
        for link_info in link_fields.values():
            construct_query(link_info, queries)
    return queries


def construct_query(link_info: LinkInfo, queries: List):
    database_major_version = beanie.DATABASE_MAJOR_VERSION
    if link_info.link_type in [
        LinkTypes.DIRECT,
        LinkTypes.OPTIONAL_DIRECT,
    ]:
        if database_major_version >= 5 or link_info.nested_links is None:
            _steps_direct1(link_info, queries)
        else:
            _steps_direct2(link_info, queries)

    elif link_info.link_type in [
        LinkTypes.BACK_DIRECT,
        LinkTypes.OPTIONAL_BACK_DIRECT,
    ]:
        if database_major_version >= 5 or link_info.nested_links is None:
            _steps_back_direct1(link_info, queries)
        else:
            _steps_back_direct2(link_info, queries)

    elif link_info.link_type in [
        LinkTypes.LIST,
        LinkTypes.OPTIONAL_LIST,
    ]:
        if database_major_version >= 5 or link_info.nested_links is None:
            _steps_list1(link_info, queries)
        else:
            _steps_list2(link_info, queries)

    elif link_info.link_type in [
        LinkTypes.BACK_LIST,
        LinkTypes.OPTIONAL_BACK_LIST,
    ]:
        if database_major_version >= 5 or link_info.nested_links is None:
            _steps_back_list1(link_info, queries)
        else:
            _steps_back_list2(link_info, queries)

    return queries


def _common_steps(link_info: LinkInfo):
    return [
        {
            "$unwind": {
                "path": f"$_link_{link_info.field_name}",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$set": {
                link_info.field_name: {
                    "$cond": {
                        "if": {
                            "$ifNull": [
                                f"$_link_{link_info.field_name}",
                                False,
                            ]
                        },
                        "then": f"$_link_{link_info.field_name}",
                        "else": f"${link_info.field_name}",
                    }
                }
            }
        },
        {"$unset": f"_link_{link_info.field_name}"},
    ]


def _steps_direct1(link_info: LinkInfo, queries: List):
    lookup_steps = [
        {
            "$lookup": {
                "from": link_info.document_class.get_collection_name(),
                "localField": f"{link_info.lookup_field_name}.$id",
                "foreignField": "_id",
                "as": f"_link_{link_info.field_name}",
            }
        },
        *_common_steps(link_info),
    ]
    if link_info.nested_links is not None:
        lookup_steps[0]["$lookup"]["pipeline"] = []  # type: ignore
        for nested_link in link_info.nested_links:
            construct_query(
                link_info.nested_links[nested_link],
                lookup_steps[0]["$lookup"]["pipeline"],  # type: ignore
            )
    queries += lookup_steps


def _steps_direct2(link_info: LinkInfo, queries: List):
    lookup_steps = [
        {
            "$lookup": {
                "from": link_info.document_class.get_collection_name(),
                "let": {"link_id": f"${link_info.lookup_field_name}.$id"},
                "as": f"_link_{link_info.field_name}",
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$_id", "$$link_id"]}}},
                ],
            }
        },
        *_common_steps(link_info),
    ]
    assert link_info.nested_links is not None
    for nested_link in link_info.nested_links:
        construct_query(
            link_info.nested_links[nested_link],
            lookup_steps[0]["$lookup"]["pipeline"],  # type: ignore
        )
    queries += lookup_steps


def _steps_back_direct1(link_info: LinkInfo, queries: List):
    lookup_steps = [
        {
            "$lookup": {
                "from": link_info.document_class.get_collection_name(),
                "localField": "_id",
                "foreignField": f"{link_info.lookup_field_name}.$id",
                "as": f"_link_{link_info.field_name}",
            }
        },
        *_common_steps(link_info),
    ]
    if link_info.nested_links is not None:
        lookup_steps[0]["$lookup"]["pipeline"] = []  # type: ignore
        for nested_link in link_info.nested_links:
            construct_query(
                link_info.nested_links[nested_link],
                lookup_steps[0]["$lookup"]["pipeline"],  # type: ignore
            )
    queries += lookup_steps


def _steps_back_direct2(link_info: LinkInfo, queries: List):
    lookup_steps = [
        {
            "$lookup": {
                "from": link_info.document_class.get_collection_name(),
                "let": {"link_id": "$_id"},
                "as": f"_link_{link_info.field_name}",
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": [
                                    f"${link_info.lookup_field_name}.$id",
                                    "$$link_id",
                                ]
                            }
                        }
                    },
                ],
            }
        },
        *_common_steps(link_info),
    ]
    assert link_info.nested_links is not None
    for nested_link in link_info.nested_links:
        construct_query(
            link_info.nested_links[nested_link],
            lookup_steps[0]["$lookup"]["pipeline"],  # type: ignore
        )
    queries += lookup_steps


def _steps_list1(link_info: LinkInfo, queries: List):
    queries.append(
        {
            "$lookup": {
                "from": link_info.document_class.get_collection_name(),
                "localField": f"{link_info.lookup_field_name}.$id",
                "foreignField": "_id",
                "as": link_info.field_name,
            }
        }
    )

    if link_info.nested_links is not None:
        queries[-1]["$lookup"]["pipeline"] = []
        for nested_link in link_info.nested_links:
            construct_query(
                link_info.nested_links[nested_link],
                queries[-1]["$lookup"]["pipeline"],
            )


def _steps_list2(link_info: LinkInfo, queries: List):
    lookup_step = {
        "$lookup": {
            "from": link_info.document_class.get_collection_name(),
            "let": {"link_id": f"${link_info.lookup_field_name}.$id"},
            "as": link_info.field_name,
            "pipeline": [
                {"$match": {"$expr": {"$in": ["$_id", "$$link_id"]}}},
            ],
        }
    }
    assert link_info.nested_links is not None
    for nested_link in link_info.nested_links:
        construct_query(
            link_info.nested_links[nested_link],
            lookup_step["$lookup"]["pipeline"],  # type: ignore
        )
    queries.append(lookup_step)


def _steps_back_list1(link_info: LinkInfo, queries: List):
    queries.append(
        {
            "$lookup": {
                "from": link_info.document_class.get_collection_name(),
                "localField": "_id",
                "foreignField": f"{link_info.lookup_field_name}.$id",
                "as": link_info.field_name,
            }
        }
    )

    if link_info.nested_links is not None:
        queries[-1]["$lookup"]["pipeline"] = []
        for nested_link in link_info.nested_links:
            construct_query(
                link_info.nested_links[nested_link],
                queries[-1]["$lookup"]["pipeline"],
            )


def _steps_back_list2(link_info: LinkInfo, queries: List):
    lookup_step = {
        "$lookup": {
            "from": link_info.document_class.get_collection_name(),
            "let": {"link_id": "$_id"},
            "as": link_info.field_name,
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$in": [
                                "$$link_id",
                                f"${link_info.lookup_field_name}.$id",
                            ]
                        }
                    }
                }
            ],
        }
    }
    assert link_info.nested_links is not None
    for nested_link in link_info.nested_links:
        construct_query(
            link_info.nested_links[nested_link],
            lookup_step["$lookup"]["pipeline"],  # type: ignore
        )
    queries.append(lookup_step)
