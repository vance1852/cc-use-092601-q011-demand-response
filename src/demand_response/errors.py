"""需求响应服务向 API 和 CLI 暴露的稳定错误。"""


class DemandResponseError(RuntimeError):
    code = "demand_response_error"
    status = 400


class NotFound(DemandResponseError):
    code = "not_found"
    status = 404


class Conflict(DemandResponseError):
    code = "conflict"
    status = 409


class Forbidden(DemandResponseError):
    code = "forbidden"
    status = 403


class InvalidState(DemandResponseError):
    code = "invalid_state"
    status = 409


class ValidationFailed(DemandResponseError):
    code = "validation_failed"
    status = 422
