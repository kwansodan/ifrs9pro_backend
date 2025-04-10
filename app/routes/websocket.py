from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, HTTPException, Query
from typing import Dict, List, Any, Optional
from sqlalchemy.orm import Session
from app.database import get_db
from app.auth.utils import verify_token, get_current_active_user_ws, get_token_from_query_param
from app.utils.background_tasks import task_manager
from app.models import User
from jose import jwt, JWTError
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

router = APIRouter(prefix="/ws", tags=["websocket"])

# Store active websocket connections
active_connections: Dict[str, List[WebSocket]] = {}

async def notify_client(websocket: WebSocket, data: Dict[str, Any]) -> None:
    await websocket.send_json(data)

@router.websocket("/tasks/{task_id}")
async def websocket_task_progress(
    websocket: WebSocket, 
    task_id: str,
    token: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    WebSocket endpoint for task progress updates
    """
    # Authenticate the connection
    try:
        if not token:
            # Try to extract token from query parameters
            token = get_token_from_query_param(websocket)
        
        if not token:
            await websocket.close(code=1008, reason="Missing authentication token")
            return
        
        # Verify the token
        try:
            payload = verify_token(token)
            user_id = payload.get("id")
            if not user_id:
                await websocket.close(code=1008, reason="Invalid token payload")
                return
                
            # Get user from database
            user = db.query(User).filter(User.id == user_id).first()
            if not user or not user.is_active:
                await websocket.close(code=1008, reason="User not found or inactive")
                return
        except JWTError as e:
            logger.error(f"Invalid token: {str(e)}")
            await websocket.close(code=1008, reason="Invalid authentication token")
            return
        
        # Accept the connection
        await websocket.accept()
        
        # Check if the task exists
        task = task_manager.get_task(task_id)
        if not task:
            await websocket.send_json({"error": "Task not found"})
            await websocket.close()
            return
        
        # Add connection to active connections
        if task_id not in active_connections:
            active_connections[task_id] = []
        active_connections[task_id].append(websocket)
        
        # Define callback for task updates
        async def on_task_update(task_data: Dict[str, Any]) -> None:
            await notify_client(websocket, task_data)
        
        # Subscribe to task updates
        task_manager.subscribe(task_id, on_task_update)
        
        # Send initial task state
        await notify_client(websocket, task)
        
        try:
            # Keep the connection open and handle incoming messages
            while True:
                # Wait for any message from the client (like a ping)
                await websocket.receive_text()
        except WebSocketDisconnect:
            # Client disconnected
            logger.info(f"Client disconnected from task {task_id}")
            # Remove connection when client disconnects
            if task_id in active_connections and websocket in active_connections[task_id]:
                active_connections[task_id].remove(websocket)
                if not active_connections[task_id]:
                    del active_connections[task_id]
        finally:
            # Unsubscribe from task updates
            task_manager.unsubscribe(task_id, on_task_update)
    
    except Exception as e:
        logger.exception(f"Error in WebSocket connection: {str(e)}")
        try:
            await websocket.close(code=1011, reason="Server error")
        except:
            pass
