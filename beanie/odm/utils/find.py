from typing import Any, Dict, List

import beanie
from beanie.exceptions import NotSupported
from beanie.odm.links import LinkInfo

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
    if "DIRECT" in link_info.link_type:
        if database_major_version >= 5 or link_info.nested_links is None:
            _steps_direct1(link_info, queries)
        else:
            _steps_direct2(link_info, queries)
    else:
        if database_major_version >= 5 or link_info.nested_links is None:
            _steps_list1(link_info, queries)
        else:
            _steps_list2(link_info, queries)

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
    lookup_field = f"{link_info.lookup_field_name}.$id"
    id_field = "_id"
    if "BACK" in link_info.link_type:
        local_field, foreign_field = id_field, lookup_field
    else:
        local_field, foreign_field = lookup_field, id_field

    lookup_steps = [
        {
            "$lookup": {
                "from": link_info.document_class.get_collection_name(),
                "localField": local_field,
                "foreignField": foreign_field,
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
    lookup_field = f"${link_info.lookup_field_name}.$id"
    id_field = "$_id"
    if "BACK" in link_info.link_type:
        link_id = id_field
        expr = ["$$link_id", lookup_field]
    else:
        link_id = lookup_field
        expr = [id_field, "$$link_id"]

    lookup_steps = [
        {
            "$lookup": {
                "from": link_info.document_class.get_collection_name(),
                "let": {"link_id": link_id},
                "as": f"_link_{link_info.field_name}",
                "pipeline": [{"$match": {"$expr": {"$eq": expr}}}],
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
    lookup_field = f"{link_info.lookup_field_name}.$id"
    id_field = "_id"
    if "BACK" in link_info.link_type:
        local_field, foreign_field = id_field, lookup_field
    else:
        local_field, foreign_field = lookup_field, id_field

    queries.append(
        {
            "$lookup": {
                "from": link_info.document_class.get_collection_name(),
                "localField": local_field,
                "foreignField": foreign_field,
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
    lookup_field = f"${link_info.lookup_field_name}.$id"
    id_field = "$_id"
    if "BACK" in link_info.link_type:
        link_id = id_field
        expr = ["$$link_id", lookup_field]
    else:
        link_id = lookup_field
        expr = [id_field, "$$link_id"]

    lookup_step = {
        "$lookup": {
            "from": link_info.document_class.get_collection_name(),
            "let": {"link_id": link_id},
            "as": link_info.field_name,
            "pipeline": [{"$match": {"$expr": {"$in": expr}}}],
        }
    }
    assert link_info.nested_links is not None
    for nested_link in link_info.nested_links:
        construct_query(
            link_info.nested_links[nested_link],
            lookup_step["$lookup"]["pipeline"],  # type: ignore
        )
    queries.append(lookup_step)
