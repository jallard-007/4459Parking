from typing import Optional, List, Dict
import raft_pb2
import msgpack
import pathlib


def encode_persistent_state(obj):
    if isinstance(obj, raft_pb2.LogEntry):
        return {'__raft_pb2__LogEntry__': True, 'as_str': obj.SerializeToString()}
    return obj


def decode_persistent_state(obj):
    if "__raft_pb2__LogEntry__" in obj:
        return raft_pb2.LogEntry.FromString(obj["as_str"])
    return obj


class PersistentState:
    def __init__(self, log_file_path: str = "raft_state", reuse_state: bool = True):
        self._log_file_path = pathlib.Path(log_file_path)

        self._persistent_state = _PersistentState()

        if self._log_file_path.is_file() and reuse_state:
            self._load_persistent_state()
        else:
            # the first entry is blank since first index is 1
            self._persistent_state.log[0] = None  # type: ignore
            self._save_persistent_state()

    @property
    def current_term(self) -> int:
        return self._persistent_state.current_term

    @current_term.setter
    def current_term(self, current_term: int) -> None:
        if self._persistent_state.current_term != current_term:
            self._persistent_state.current_term = current_term
            # self._save_persistent_state()

    @property
    def voted_for(self) -> Optional[int]:
        return self._persistent_state.voted_for

    @voted_for.setter
    def voted_for(self, voted_for: Optional[int]) -> None:
        if self._persistent_state.voted_for != voted_for:
            self._persistent_state.voted_for = voted_for
            # self._save_persistent_state()

    def _load_persistent_state(self) -> None:
        with open(self._log_file_path, "r") as log_file:
            try:
                state = msgpack.unpack(log_file, object_hook=decode_persistent_state)
            except Exception as ex:
                print(f"Failed to load data from {self._log_file_path}: {ex}")
                return

        if not isinstance(state, PersistentState):
            print(f"{self._log_file_path} does not contain a valid state")
            return
        
        self._persistent_state = state

    def _save_persistent_state(self) -> None:
        with open(self._log_file_path, "w") as log_file:
            try:
                msgpack.pack(self._persistent_state, log_file, default=encode_persistent_state)
            except Exception as ex:
                print(f"Failed to save data to {self._log_file_path}: {ex}")
                return
    
    def save(self) -> None:
        self._save_persistent_state()

    def append_log(self, log: raft_pb2.LogEntry) -> None:
        assert log.index == self.get_log_len() + 1
        self._persistent_state.log.append(log)
        # self._save_persistent_state()

    def get_log_at(self, index: int) -> Optional[raft_pb2.LogEntry]:
        try:
            return self._persistent_state.log[index]
        except IndexError:
            return None

    def get_last_log(self) -> Optional[raft_pb2.LogEntry]:
        return self.get_log_at(-1)

    def get_log_term_at(self, index: int) -> int:
        if prev_log := self.get_log_at(index):
            return prev_log.term
        return 0
    
    def get_logs_starting_from(self, index: int) -> List[raft_pb2.LogEntry]:
        return self._persistent_state.log[index:]
    
    def clear_log_from(self, index: int) -> None:
        """clears the log from index on"""
        if index < 1:
            index = 1
        if self.get_log_len() >= index:
            self._persistent_state.log = self._persistent_state.log[:index]
            # self._save_persistent_state()

    def get_log_len(self) -> int:
        return len(self._persistent_state.log) - 1


class _PersistentState:
    """Persistent state on all servers (Updated on stable storage before responding to RPCs)"""

    def __init__(self):
        self.current_term: int = 0
        """latest term server has seen (initialized to 0 on first boot, increases monotonically)"""
        self.voted_for: Optional[int] = None
        """candidate_id that received vote in current term (or None if none)"""
        self.log: List[raft_pb2.LogEntry] = []
        """log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)"""


class LeaderState:
    """Volatile state on leaders (Reinitialized after election)"""

    def __init__(self):
        self.next_index: Dict[int, int] = {}
        """for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)"""
        self.match_index: Dict[int, int] = {}
        """for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)"""