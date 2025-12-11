
'''
import pytest
from starlette.websockets import WebSocketDisconnect


def test_websocket_requires_token(client):
    """
    The websocket should reject connections without a token.
    """
    with pytest.raises(WebSocketDisconnect):
        client.websocket_connect("/ws/tasks/abc")
'''
