import schemathesis

schema = schemathesis.openapi.from_url(
    "http://127.0.0.1:8080/openapi.json",
)
# To show the token in the cURL snippet
schema.config.output.sanitization.update(enabled=False)

@schema.parametrize()
def test_api(case):
    case.call_and_validate(headers={"Authorization": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhZG1pbkBpZnJzOXByby5jb20iLCJpZCI6MjUzLCJyb2xlIjoiYWRtaW4iLCJpc19hY3RpdmUiOnRydWUsImV4cCI6MTc2NTUwMDMwNn0.VPX169FNYKqeARw6OmjAljrO2SF_9DGvmkUwIhrod3g"})