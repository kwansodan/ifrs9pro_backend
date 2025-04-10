import asyncio
import logging
from typing import Dict, Any, Callable, Awaitable, Optional, List
import uuid
import time
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects by converting them to ISO format strings."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def serialize_task_info(task_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Serialize task information to ensure it's JSON-compatible.
    Converts datetime objects to ISO format strings.
    """
    serialized = {}
    for key, value in task_info.items():
        if key == "subscribers":
            # Skip the subscribers set as it's not serializable and not needed in responses
            continue
        elif isinstance(value, datetime):
            serialized[key] = value.isoformat()
        elif isinstance(value, set):
            # Convert sets to lists for serialization
            serialized[key] = list(value)
        else:
            serialized[key] = value
    return serialized

class BackgroundTaskManager:
    """
    Manages background tasks and their progress tracking.
    """
    def __init__(self):
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self._loop = None
        
    def _ensure_loop(self):
        """Ensure we have an event loop for the current thread"""
        try:
            self._loop = asyncio.get_event_loop()
        except RuntimeError:
            # If there's no event loop in this thread, create one
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        return self._loop
    
    def _run_coroutine(self, coro):
        """Run a coroutine in the appropriate way depending on context"""
        loop = self._ensure_loop()
        
        # Check if we're in an async context
        try:
            # If we're in an async context, we can just create a task
            if asyncio.get_running_loop() == loop:
                return asyncio.create_task(coro)
        except RuntimeError:
            # We're not in an async context, run the coroutine to completion
            if threading.current_thread() is threading.main_thread():
                # In the main thread, we can use run_until_complete
                return loop.run_until_complete(coro)
            else:
                # In a background thread, we need to run the coroutine in a way
                # that doesn't block the thread
                try:
                    # Get the current event loop in this thread
                    thread_loop = asyncio.get_event_loop()
                    # If we have a valid loop, run the coroutine in it
                    if thread_loop.is_running():
                        # Create a task in the current loop
                        return thread_loop.create_task(coro)
                    else:
                        # Run the coroutine to completion
                        return thread_loop.run_until_complete(coro)
                except RuntimeError:
                    # No event loop in this thread, use run_coroutine_threadsafe
                    future = asyncio.run_coroutine_threadsafe(coro, loop)
                    # We don't wait for the result, this is fire-and-forget
                    return None
        
    def create_task(self, task_type: str, description: str) -> str:
        """
        Create a new background task and return its ID.
        """
        task_id = str(uuid.uuid4())
        self.tasks[task_id] = {
            "id": task_id,
            "type": task_type,
            "description": description,
            "status": "pending",
            "progress": 0,
            "created_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None,
            "error": None,
            "result": None,
            "total_items": 0,
            "processed_items": 0,
            "subscribers": set()
        }
        return task_id
    
    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get task information by ID.
        """
        if task_id in self.tasks:
            task_info = self.tasks[task_id].copy()
            # Remove the subscribers set from the returned info
            task_info.pop("subscribers", None)
            return serialize_task_info(task_info)
        return None
    
    def update_task(self, task_id: str, **kwargs) -> None:
        """
        Update task information.
        """
        if task_id in self.tasks:
            self.tasks[task_id].update(kwargs)
            # Notify subscribers about the update
            self._run_coroutine(self._notify_subscribers(task_id))
    
    def update_progress(self, task_id: str, progress: float, 
                        processed_items: Optional[int] = None,
                        total_items: Optional[int] = None,
                        status_message: Optional[str] = None) -> None:
        """
        Update the progress of a task.
        """
        if task_id in self.tasks:
            updates = {"progress": progress}
            
            if processed_items is not None:
                updates["processed_items"] = processed_items
                
            if total_items is not None:
                updates["total_items"] = total_items
                
            if status_message is not None:
                updates["status_message"] = status_message
                
            self.tasks[task_id].update(updates)
            
            # Notify subscribers about the progress update
            self._run_coroutine(self._notify_subscribers(task_id))
    
    def mark_as_started(self, task_id: str) -> None:
        """
        Mark a task as started.
        """
        if task_id in self.tasks:
            self.tasks[task_id].update({
                "status": "running",
                "started_at": datetime.utcnow()
            })
            self._run_coroutine(self._notify_subscribers(task_id))
    
    def mark_as_completed(self, task_id: str, result: Any = None) -> None:
        """
        Mark a task as completed.
        """
        if task_id in self.tasks:
            self.tasks[task_id].update({
                "status": "completed",
                "progress": 100,
                "completed_at": datetime.utcnow(),
                "result": result
            })
            self._run_coroutine(self._notify_subscribers(task_id))
    
    def mark_as_failed(self, task_id: str, error: str) -> None:
        """
        Mark a task as failed.
        """
        if task_id in self.tasks:
            self.tasks[task_id].update({
                "status": "failed",
                "error": error,
                "completed_at": datetime.utcnow()
            })
            self._run_coroutine(self._notify_subscribers(task_id))
    
    def subscribe(self, task_id: str, callback: Callable[[Dict[str, Any]], Awaitable[None]]) -> bool:
        """
        Subscribe to task updates.
        """
        if task_id in self.tasks:
            self.tasks[task_id]["subscribers"].add(callback)
            return True
        return False
    
    def unsubscribe(self, task_id: str, callback: Callable[[Dict[str, Any]], Awaitable[None]]) -> bool:
        """
        Unsubscribe from task updates.
        """
        if task_id in self.tasks and callback in self.tasks[task_id]["subscribers"]:
            self.tasks[task_id]["subscribers"].remove(callback)
            return True
        return False
    
    async def _notify_subscribers(self, task_id: str) -> None:
        """
        Notify all subscribers about a task update.
        """
        if task_id in self.tasks:
            task_info = self.tasks[task_id].copy()
            # Remove the subscribers set from the info sent to subscribers
            subscribers = task_info.pop("subscribers", set())
            
            # Serialize task info to ensure it's JSON-compatible
            serialized_info = serialize_task_info(task_info)
            
            for callback in subscribers:
                try:
                    await callback(serialized_info)
                except Exception as e:
                    logger.error(f"Error notifying subscriber for task {task_id}: {e}")
    
    def clean_old_tasks(self, max_age_hours: int = 24) -> None:
        """
        Remove old completed or failed tasks.
        """
        now = datetime.utcnow()
        to_remove = []
        
        for task_id, task in self.tasks.items():
            if task["status"] in ["completed", "failed"]:
                completed_at = task.get("completed_at")
                if completed_at and (now - completed_at).total_seconds() > max_age_hours * 3600:
                    to_remove.append(task_id)
        
        for task_id in to_remove:
            del self.tasks[task_id]

# Use a lazy-loaded singleton pattern instead
_task_manager_instance = None

def get_task_manager():
    """
    Get or create the task manager instance.
    Uses lazy initialization to avoid startup overhead.
    """
    global _task_manager_instance
    if _task_manager_instance is None:
        _task_manager_instance = BackgroundTaskManager()
    return _task_manager_instance

# Function to run a task in the background
async def run_background_task(task_id: str, func, *args, **kwargs):
    """
    Run a function as a background task with progress tracking.
    """
    task_manager = get_task_manager()
    try:
        task_manager.mark_as_started(task_id)
        result = await func(task_id=task_id, *args, **kwargs)
        task_manager.mark_as_completed(task_id, result)
        return result
    except Exception as e:
        logger.exception(f"Background task {task_id} failed: {e}")
        task_manager.mark_as_failed(task_id, str(e))
        raise
