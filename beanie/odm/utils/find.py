from typing import Any, Dict, Iterator

import beanie
from beanie.odm.links import LinkInfo


def iter_stages(link_info: LinkInfo) -> Iterator[Dict[str, Any]]:
    as_field = link_info.field_name
    is_direct = "DIRECT" in link_info.link_type
    if is_direct:
        as_field = "_link_" + as_field

    lookup: Dict[str, Any] = {
        "from": link_info.document_class.get_collection_name(),
        "as": as_field,
    }

    lookup_field = f"{link_info.lookup_field_name}.$id"
    is_backlink = "BACK" in link_info.link_type
    if beanie.DATABASE_MAJOR_VERSION >= 5 or link_info.nested_links is None:
        local_field, foreign_field = lookup_field, "_id"
        if is_backlink:
            local_field, foreign_field = foreign_field, local_field
        lookup["localField"] = local_field
        lookup["foreignField"] = foreign_field
    else:
        match_op = "$eq" if is_direct else "$in"
        if is_backlink:
            let_expr = {"link_id": "$_id"}
            match_expr = ["$$link_id", "$" + lookup_field]
        else:
            let_expr = {"link_id": "$" + lookup_field}
            match_expr = ["$_id", "$$link_id"]
        lookup["let"] = let_expr
        lookup["pipeline"] = [{"$match": {"$expr": {match_op: match_expr}}}]

    if link_info.nested_links is not None:
        pipeline = lookup.setdefault("pipeline", [])
        for nested_link_info in link_info.nested_links.values():
            pipeline.extend(iter_stages(nested_link_info))

    yield {"$lookup": lookup}

    if is_direct:
        yield {
            "$unwind": {
                "path": "$" + as_field,
                "preserveNullAndEmptyArrays": True,
            }
        }
        yield {
            "$set": {
                link_info.field_name: {
                    "$cond": {
                        "if": {"$ifNull": ["$" + as_field, False]},
                        "then": "$" + as_field,
                        "else": "$" + link_info.field_name,
                    }
                }
            }
        }
        yield {"$unset": as_field}
