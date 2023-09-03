class DocumentNotFound(Exception):
    pass


class DocumentWasNotSaved(Exception):
    pass


class ReplaceError(Exception):
    pass


class StateManagementIsTurnedOff(Exception):
    pass


class StateNotSaved(Exception):
    pass


class RevisionIdWasChanged(Exception):
    pass


class NotSupported(Exception):
    pass


class MongoDBVersionError(Exception):
    pass


class SettingsNotInitialized(Exception):
    pass


class UnionHasNoRegisteredDocs(Exception):
    pass


class DocWasNotRegisteredInUnionClass(Exception):
    pass
