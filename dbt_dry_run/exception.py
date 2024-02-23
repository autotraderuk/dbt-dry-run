from google.cloud.bigquery import SchemaField
from pydantic import ValidationError


class NotCompiledException(Exception):
    pass


class UpstreamFailedException(Exception):
    pass


class SourceMissingException(Exception):
    pass


class InvalidColumnSpecification(Exception):
    pass


class UnknownDataTypeException(Exception):
    pass


class NodeExecutionException(Exception):
    pass


class SchemaChangeException(Exception):
    pass


class SnapshotConfigException(Exception):
    pass


class ManifestValidationError(Exception):
    pass


class UnknownSchemaException(Exception):
    pass

    @classmethod
    def from_validation_error(
        cls, schema_field: SchemaField, e: ValidationError
    ) -> "UnknownSchemaException":
        errors = e.errors()
        column_type_exception = list(filter(lambda err: "type" in err["loc"], errors))
        if column_type_exception:
            return cls(
                f"BigQuery dry run field '{schema_field.name}' returned unknown column types: {column_type_exception[0]['msg']}."
                f"If you think this column type is valid then raise an issue on GitHub"
            )
        else:
            return cls(
                f"Couldn't understand schema returned from BigQuery for field '{schema_field.name}' error:\n{str(e)}"
            )
