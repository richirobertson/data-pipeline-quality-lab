"""Domain exceptions that make pipeline failure modes explicit."""


class PipelineError(RuntimeError):
    """Base error for an expected pipeline failure."""


class OnsApiError(PipelineError):
    """The ONS API returned an unusable response."""


class FilterOutputTimeout(PipelineError):
    """A submitted filter did not produce a downloadable output in time."""


class ArtifactIntegrityError(PipelineError):
    """A downloaded artifact did not match its expected integrity metadata."""


class ObjectConflictError(PipelineError):
    """An immutable object key already contains different content."""


class SchemaContractError(PipelineError):
    """A source artifact does not match the declared technical contract."""
