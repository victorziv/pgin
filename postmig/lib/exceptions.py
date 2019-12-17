class CustomException(Exception):
    def __init__(self, msg):
        self.err = msg

    def __str__(self):
        return self.err

# =================================================


class CaseSkippedException(CustomException):
    pass
# =================================================


class JiraShipmentTicketNotFound(CustomException):
    pass

# =================================================


class JiraSOTicketNotFound(CustomException):
    pass
# =================================================


class JiraticketUnsupportedFieldException(CustomException):
    pass
# =================================================


class JiraIVTTicketNotFound(CustomException):
    pass

# =================================================


class MachineAlreadyExists(CustomException):
    pass

# =================================================


class SlotNotAssignedException(CustomException):
    pass
# =================================================


class SlotOccupiedException(CustomException):
    pass
# =================================================


class SlotNumberInvalidException(CustomException):
    pass
# =================================================


class TaskStateChangeTimeoutException(CustomException):
    pass

# =================================================


class SinkNotFoundException(CustomException):
    pass

# =================================================


class SrvNotFoundException(CustomException):
    pass

# =================================================


class TaskSkippedException(CustomException):
    pass

# =================================================


class TaskStoppedException(CustomException):
    pass

# =================================================


class TaskPausedException(CustomException):
    pass

# =================================================


class TaskProcessIsDeadException(CustomException):
    pass

# =================================================


class TaskTerminatedException(CustomException):
    pass

# =================================================


class TaskWorkerNotFoundException(CustomException):
    pass

# =================================================


class UnsupportedModelException(CustomException):
    pass
# =================================================


class UnsupportedVendorException(CustomException):
    pass
# =================================================


class UnsupportedCaseSelectionMode(CustomException):
    pass
# =================================================


class UnsupportedATSModelException(CustomException):
    pass
# =================================================
