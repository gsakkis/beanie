from enum import Enum


class ModelType(str, Enum):
    Document = "Document"
    View = "View"
    UnionDoc = "UnionDoc"
