import json
import time
import uuid
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import threading
import os
import tempfile

class InMemorySessionManager:
    """Simple in-memory session manager for stateless operations."""
    
    def __init__(self, max_sessions: int = 1000, session_timeout: int = 3600):
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.max_sessions = max_sessions
        self.session_timeout = session_timeout
        self._lock = threading.RLock()
        
    def create_session(self, initial_data: Optional[Dict] = None) -> str:
        """Create a new session and return session ID."""
        with self._lock:
            session_id = str(uuid.uuid4())
            
            # Clean up old sessions if we're at the limit
            if len(self.sessions) >= self.max_sessions:
                self._cleanup_expired_sessions()
                
                # If still at limit, remove oldest session
                if len(self.sessions) >= self.max_sessions:
                    oldest_session = min(
                        self.sessions.keys(), 
                        key=lambda k: self.sessions[k].get('created_at', 0)
                    )
                    del self.sessions[oldest_session]
            
            self.sessions[session_id] = {
                'id': session_id,
                'created_at': time.time(),
                'updated_at': time.time(),
                'data': initial_data or {}
            }
            
            return session_id
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data by ID."""
        with self._lock:
            if session_id not in self.sessions:
                return None
                
            session = self.sessions[session_id]
            
            # Check if session has expired
            if time.time() - session['updated_at'] > self.session_timeout:
                del self.sessions[session_id]
                return None
                
            return session['data']
    
    def update_session(self, session_id: str, data: Dict[str, Any]) -> bool:
        """Update session data."""
        with self._lock:
            if session_id not in self.sessions:
                return False
                
            # Check if session has expired
            if time.time() - self.sessions[session_id]['updated_at'] > self.session_timeout:
                del self.sessions[session_id]
                return False
                
            self.sessions[session_id]['data'].update(data)
            self.sessions[session_id]['updated_at'] = time.time()
            return True
    
    def delete_session(self, session_id: str) -> bool:
        """Delete a session."""
        with self._lock:
            if session_id in self.sessions:
                del self.sessions[session_id]
                return True
            return False
    
    def _cleanup_expired_sessions(self):
        """Remove expired sessions."""
        current_time = time.time()
        expired_sessions = [
            session_id for session_id, session in self.sessions.items()
            if current_time - session['updated_at'] > self.session_timeout
        ]
        
        for session_id in expired_sessions:
            del self.sessions[session_id]
    
    def get_session_count(self) -> int:
        """Get the number of active sessions."""
        with self._lock:
            self._cleanup_expired_sessions()
            return len(self.sessions)

class FileSessionManager:
    """File-based session manager for persistence across restarts."""
    
    def __init__(self, session_dir: Optional[str] = None, session_timeout: int = 3600):
        self.session_dir = session_dir or os.path.join(tempfile.gettempdir(), 'ipaffs_sessions')
        self.session_timeout = session_timeout
        self._lock = threading.RLock()
        
        # Ensure session directory exists
        os.makedirs(self.session_dir, exist_ok=True)
    
    def create_session(self, initial_data: Optional[Dict] = None) -> str:
        """Create a new session and return session ID."""
        with self._lock:
            session_id = str(uuid.uuid4())
            session_data = {
                'id': session_id,
                'created_at': time.time(),
                'updated_at': time.time(),
                'data': initial_data or {}
            }
            
            session_file = os.path.join(self.session_dir, f"{session_id}.json")
            with open(session_file, 'w') as f:
                json.dump(session_data, f)
            
            # Clean up old sessions periodically
            self._cleanup_expired_sessions()
            
            return session_id
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data by ID."""
        with self._lock:
            session_file = os.path.join(self.session_dir, f"{session_id}.json")
            
            if not os.path.exists(session_file):
                return None
            
            try:
                with open(session_file, 'r') as f:
                    session = json.load(f)
                
                # Check if session has expired
                if time.time() - session['updated_at'] > self.session_timeout:
                    os.remove(session_file)
                    return None
                
                return session['data']
                
            except (json.JSONDecodeError, KeyError, OSError):
                # Remove corrupted session file
                try:
                    os.remove(session_file)
                except OSError:
                    pass
                return None
    
    def update_session(self, session_id: str, data: Dict[str, Any]) -> bool:
        """Update session data."""
        with self._lock:
            session_file = os.path.join(self.session_dir, f"{session_id}.json")
            
            if not os.path.exists(session_file):
                return False
            
            try:
                with open(session_file, 'r') as f:
                    session = json.load(f)
                
                # Check if session has expired
                if time.time() - session['updated_at'] > self.session_timeout:
                    os.remove(session_file)
                    return False
                
                session['data'].update(data)
                session['updated_at'] = time.time()
                
                with open(session_file, 'w') as f:
                    json.dump(session, f)
                
                return True
                
            except (json.JSONDecodeError, KeyError, OSError):
                return False
    
    def delete_session(self, session_id: str) -> bool:
        """Delete a session."""
        with self._lock:
            session_file = os.path.join(self.session_dir, f"{session_id}.json")
            
            if os.path.exists(session_file):
                try:
                    os.remove(session_file)
                    return True
                except OSError:
                    pass
            
            return False
    
    def _cleanup_expired_sessions(self):
        """Remove expired session files."""
        current_time = time.time()
        
        try:
            for filename in os.listdir(self.session_dir):
                if filename.endswith('.json'):
                    session_file = os.path.join(self.session_dir, filename)
                    try:
                        with open(session_file, 'r') as f:
                            session = json.load(f)
                        
                        if current_time - session.get('updated_at', 0) > self.session_timeout:
                            os.remove(session_file)
                            
                    except (json.JSONDecodeError, KeyError, OSError):
                        # Remove corrupted or unreadable files
                        try:
                            os.remove(session_file)
                        except OSError:
                            pass
                            
        except OSError:
            pass
    
    def get_session_count(self) -> int:
        """Get the number of active sessions."""
        with self._lock:
            self._cleanup_expired_sessions()
            try:
                return len([f for f in os.listdir(self.session_dir) if f.endswith('.json')])
            except OSError:
                return 0
