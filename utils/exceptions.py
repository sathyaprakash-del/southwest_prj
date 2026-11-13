class MDMError(Exception):
    pass


class ConfigError(MDMError):
    pass


class ValidationError(MDMError):
    pass


class ThresholdExceededError(MDMError):
    pass


class PipelineError(MDMError):
    pass
