class HoldingsError(Exception):
    """Base exception for holdings producer failures."""


class AlignmentError(HoldingsError):
    """Raised when an entity alignment table is malformed."""


class PayloadValidationError(HoldingsError):
    """Raised when a produced payload fails the shared Ex-3 contract."""


class AdapterError(HoldingsError):
    """Raised when a configured holdings mart adapter cannot fail closed."""


class AdapterSchemaError(AdapterError):
    """Raised when a configured mart table is missing required fields."""
