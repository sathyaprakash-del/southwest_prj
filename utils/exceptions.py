class MDMError(Exception):
    pass


class ThresholdExceededError(MDMError):
    pass


class PipelineError(MDMError):
    pass
