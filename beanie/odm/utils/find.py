from typing import Any, Dict, Iterator

import beanie
from beanie.odm.links import LinkInfo


def iter_stages(link_info: LinkInfo) -> Iterator[Dict[str, Any]]:
    if beanie.DATABASE_MAJOR_VERSION >= 5 or link_info.nested_links is None:
        lookup = _concise_lookup(link_info)
    else:
        lookup = _verbose_lookup(link_info)
    if link_info.nested_links is not None:
        pipeline = lookup.setdefault("pipeline", [])
        for nested_link_info in link_info.nested_links.values():
            pipeline.extend(iter_stages(nested_link_info))
    yield {"$lookup": lookup}

    if "DIRECT" in link_info.link_type:
        yield {
            "$unwind": {
                "path": f"$_link_{link_info.field_name}",
                "preserveNullAndEmptyArrays": True,
            }
        }
        yield {
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
        }
        yield {"$unset": f"_link_{link_info.field_name}"}


def _concise_lookup(link_info: LinkInfo) -> Dict[str, Any]:
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

    return {
        "from": link_info.document_class.get_collection_name(),
        "localField": local_field,
        "foreignField": foreign_field,
        "as": as_field,
    }


def _verbose_lookup(link_info: LinkInfo) -> Dict[str, Any]:
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

    return {
        "from": link_info.document_class.get_collection_name(),
        "let": {"link_id": link_id},
        "pipeline": [{"$match": {"$expr": {match_expr: expr}}}],
        "as": as_field,
    }
