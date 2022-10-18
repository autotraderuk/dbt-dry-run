class NotCompiledException(Exception):
    pass


class UpstreamFailedException(Exception):
    pass


class NodeExecutionException(Exception):
    pass


class SchemaChangeException(Exception):
    pass


class SnapshotConfigException(Exception):
    pass


class ManifestValidationError(Exception):
    pass
