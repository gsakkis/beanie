from enum import Enum
from typing import Any, Dict, List, Optional

from beanie.odm.operators import BaseFieldOperator, FieldName


class BaseGeoOperator(BaseFieldOperator):
    def __init__(
        self, field: FieldName, geo_type: str, coordinates: List[List[float]]
    ):
        super().__init__(
            field,
            {"$geometry": {"type": geo_type, "coordinates": coordinates}},
        )


class GeoIntersects(BaseGeoOperator):
    """
    `$geoIntersects` query operator

    Example:

    ```python
    class GeoObject(BaseModel):
        type: str = "Point"
        coordinates: Tuple[float, float]

    class Place(Document):
        geo: GeoObject

        class Collection:
            name = "places"
            indexes = [
                [("geo", pymongo.GEOSPHERE)],  # GEO index
            ]

    GeoIntersects(Place.geo, "Polygon", [[0,0], [1,1], [3,3]])
    ```

    Will return query object like

    ```python
    {
        "geo": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Polygon",
                    "coordinates": [[0,0], [1,1], [3,3]],
                }
            }
        }
    }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/geoIntersects/>
    """

    operator = "$geoIntersects"


class GeoWithinTypes(str, Enum):
    Polygon = "Polygon"
    MultiPolygon = "MultiPolygon"


class GeoWithin(BaseGeoOperator):
    """
    `$geoWithin` query operator

    Example:

    ```python
    class GeoObject(BaseModel):
        type: str = "Point"
        coordinates: Tuple[float, float]

    class Place(Document):
        geo: GeoObject

        class Collection:
            name = "places"
            indexes = [
                [("geo", pymongo.GEOSPHERE)],  # GEO index
            ]

    GeoWithin(Place.geo, "Polygon", [[0,0], [1,1], [3,3]])
    ```

    Will return query object like

    ```python
    {
        "geo": {
            "$geoWithin": {
                "$geometry": {
                    "type": "Polygon",
                    "coordinates": [[0,0], [1,1], [3,3]],
                }
            }
        }
    }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/geoWithin/>
    """

    operator = "$geoWithin"


class Box(BaseFieldOperator):
    """
    `$box` query operator

    Example:

    ```python
    class GeoObject(BaseModel):
        type: str = "Point"
        coordinates: Tuple[float, float]

    class Place(Document):
        geo: GeoObject

        class Collection:
            name = "places"
            indexes = [
                [("geo", pymongo.GEOSPHERE)],  # GEO index
            ]

    Box(Place.geo, lower_left=[10,12], upper_right=[15,20])
    ```

    Will return query object like

    ```python
    {
        "geo": {
            "$geoWithin": {
                "$box": [[10, 12], [15, 20]]
            }
        }
    }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/box/>
    """

    operator = "$geoWithin"

    def __init__(
        self,
        field: FieldName,
        lower_left: List[float],
        upper_right: List[float],
    ):
        super().__init__(field, {"$box": [lower_left, upper_right]})


class Near(BaseFieldOperator):
    """
    `$near` query operator

    Example:

    ```python
    class GeoObject(BaseModel):
        type: str = "Point"
        coordinates: Tuple[float, float]

    class Place(Document):
        geo: GeoObject

        class Collection:
            name = "places"
            indexes = [
                [("geo", pymongo.GEOSPHERE)],  # GEO index
            ]

    Near(Place.geo, 1.2345, 2.3456, max_distance=500)
    ```

    Will return query object like

    ```python
    {
        "geo": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [1.2345, 2.3456],
                },
                "$maxDistance": 500,
            }
        }
    }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/near/>
    """

    operator = "$near"

    def __init__(
        self,
        field: FieldName,
        longitude: float,
        latitude: float,
        max_distance: Optional[float] = None,
        min_distance: Optional[float] = None,
    ):
        expression: Dict[str, Any] = {
            "$geometry": {
                "type": "Point",
                "coordinates": [longitude, latitude],
            }
        }
        if max_distance:
            expression["$maxDistance"] = max_distance
        if min_distance:
            expression["$minDistance"] = min_distance
        super().__init__(field, expression)


class NearSphere(Near):
    """
    `$nearSphere` query operator

    Example:

    ```python
    class GeoObject(BaseModel):
        type: str = "Point"
        coordinates: Tuple[float, float]

    class Place(Document):
        geo: GeoObject

        class Collection:
            name = "places"
            indexes = [
                [("geo", pymongo.GEOSPHERE)],  # GEO index
            ]

    NearSphere(Place.geo, 1.2345, 2.3456, min_distance=500)
    ```

    Will return query object like

    ```python
    {
        "geo": {
            "$nearSphere": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [1.2345, 2.3456],
                },
                "$maxDistance": 500,
            }
        }
    }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/nearSphere/>
    """

    operator = "$nearSphere"
