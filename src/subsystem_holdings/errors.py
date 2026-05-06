class HoldingsError(Exception):
    """Base exception for holdings producer failures."""


class AlignmentError(HoldingsError):
    """Raised when an entity alignment table is malformed."""


class PayloadValidationError(HoldingsError):
    """Raised when a produced payload fails the shared Ex-3 contract."""
