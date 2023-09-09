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
    if beanie.DATABASE_MAJOR_VERSION >= 5 or link_info.nested_links is None:
        _steps_concise(link_info, queries)
    else:
        _steps_verbose(link_info, queries)

    if "DIRECT" in link_info.link_type:
        queries.extend(
            [
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
        )

    return queries


def _steps_concise(link_info: LinkInfo, queries: List):
    lookup_field = f"{link_info.lookup_field_name}.$id"
    id_field = "_id"
    if "BACK" in link_info.link_type:
        local_field, foreign_field = id_field, lookup_field
    else:
        local_field, foreign_field = lookup_field, id_field

    if "DIRECT" in link_info.link_type:
        as_field = f"_link_{link_info.field_name}"
    else:
        as_field = link_info.field_name

    lookup = {
        "from": link_info.document_class.get_collection_name(),
        "localField": local_field,
        "foreignField": foreign_field,
        "as": as_field,
    }
    if link_info.nested_links is not None:
        lookup["pipeline"] = []  # type: ignore
        for nested_link in link_info.nested_links:
            construct_query(
                link_info.nested_links[nested_link],
                lookup["pipeline"],  # type: ignore
            )
    queries.append({"$lookup": lookup})


def _steps_verbose(link_info: LinkInfo, queries: List):
    lookup_field = f"${link_info.lookup_field_name}.$id"
    id_field = "$_id"
    if "BACK" in link_info.link_type:
        link_id = id_field
        expr = ["$$link_id", lookup_field]
    else:
        link_id = lookup_field
        expr = [id_field, "$$link_id"]

    if "DIRECT" in link_info.link_type:
        match_expr = "$eq"
        as_field = f"_link_{link_info.field_name}"
    else:
        match_expr = "$in"
        as_field = link_info.field_name

    lookup = {
        "from": link_info.document_class.get_collection_name(),
        "let": {"link_id": link_id},
        "pipeline": [{"$match": {"$expr": {match_expr: expr}}}],
        "as": as_field,
    }
    if link_info.nested_links is not None:
        for nested_link in link_info.nested_links:
            construct_query(
                link_info.nested_links[nested_link],
                lookup["pipeline"],  # type: ignore
            )
    queries.append({"$lookup": lookup})
