class DataIssueError(Exception):
    pass


class InsufficientMonthsError(DataIssueError):
    pass


class MissingDataError(DataIssueError):
    pass