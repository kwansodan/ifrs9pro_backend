from app.models import Feedback, Help


def test_create_feedback(client):
    resp = client.post("/user/feedback", json={"description": "Great app"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["description"] == "Great app"
    assert body["status"]


def test_like_feedback_toggle(client, db_session):
    feedback = Feedback(description="Like me", status="submitted", user_id=1)
    db_session.add(feedback)
    db_session.commit()

    like_resp = client.post(f"/user/feedback/{feedback.id}/like")
    assert like_resp.status_code == 200
    assert like_resp.json()["is_liked_by_user"] is True


def test_help_crud(client):
    create_resp = client.post("/user/help", json={"description": "I need an assist for this test. Let us attempt a fix on description"})
    assert create_resp.status_code == 200
    help_id = create_resp.json()["id"]

    update_resp = client.put(f"/user/help/{help_id}", json={"description": "updated to fix at least 10 len"})
    assert update_resp.status_code == 200
    assert update_resp.json()["description"] == "updated to fix at least 10 len"

    list_resp = client.get("/user/help")
    assert list_resp.status_code == 200
    assert any(item["id"] == help_id for item in list_resp.json())

    delete_resp = client.delete(f"/user/help/{help_id}")
    assert delete_resp.status_code == 204

