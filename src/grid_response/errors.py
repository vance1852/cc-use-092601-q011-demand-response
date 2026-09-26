"""需求响应服务向 API 和 CLI 暴露的稳定错误。"""


class GridError(RuntimeError):
    code = "grid_error"
    status = 400


class NotFound(GridError):
    code = "not_found"
    status = 404


class Conflict(GridError):
    code = "conflict"
    status = 409


class Forbidden(GridError):
    code = "forbidden"
    status = 403


class InvalidState(GridError):
    code = "invalid_state"
    status = 409


class ValidationFailed(GridError):
    code = "validation_failed"
    status = 422
