import grpc
import raft_pb2
import raft_pb2_grpc
from typing import Optional, List, Dict, Callable, Tuple
import random
import threading
from definitions import MEMBER_ID_TO_ADDRESS
from state import PersistentState, LeaderState
import multiprocessing
import time
import queue

MIN_TIMEOUT_MS_RANGE = 100

"""

If commitIndex > lastApplied: increment lastApplied, apply
log[lastApplied] to state machine (§5.3)
• If RPC request or response contains term T > currentTerm:
set currentTerm = T, convert to follower (§5.1)


"""

class Server(raft_pb2_grpc.RaftServicer):
    def AppendEntries(
        self, request: raft_pb2.AppendEntriesRequest, context: grpc.ServicerContext
    ) -> raft_pb2.AppendEntriesResponse:
        pass

    def RequestVote(
        self, request: raft_pb2.RequestVoteRequest, context: grpc.ServicerContext
    ) -> raft_pb2.RequestVoteResponse:
        pass

class Member:
    def __init__(
        self,
        id: int,
        timeout_min_ms: int = 200,
        timeout_max_ms: int = 400,
    ):

        # TODO: identify critical zones
        self.thread_lock = threading.RLock()

        self.timeout_min_ms = timeout_min_ms
        self.timeout_max_ms = timeout_max_ms

        self.id = id
        self.leader_id: Optional[int] = None

        self._is_leader = threading.Event()

        self.timeout_handler_thread: Optional[threading.Timer] = None
        self.reset_timeout()

        self.state = PersistentState(log_file_path=f"raft_state.{self.id}", reuse_state=False)
        """Persistent state"""

        """Volatile state on all servers (Reinitialized after election)"""

        self.commit_index: int = 0
        """index of highest log entry known to be committed (initialized to 0, increases monotonically)"""
        self.last_applied: int = 0
        """index of highest log entry applied to state machine (initialized to 0, increases monotonically)"""

        self.leader_state: LeaderState = LeaderState()
        """State when this member is the leader"""

    def handle_append_entries_request(
        self, request: raft_pb2.AppendEntriesRequest, context: grpc.ServicerContext
    ) -> raft_pb2.AppendEntriesResponse:

        # reply false if term < current_term
        if request.term < self.state.current_term:
            return raft_pb2.AppendEntriesResponse(
                term=self.state.current_term, success=False
            )
        
        # if request.term == self.term and we are leader; this should not be possible.
        # only the leader should be sending AppendEntries, and there can be only be one leader per term
        if request.term == self.state.current_term:
            assert self.leader_id != self.id
        else:
            # only other option is request.term > self.term
            # in that case the leader has changed, if we are not already a follower, we need to become one
            self.set_new_term(request.term)
            self.leader_id = request.leader_id

        self.reset_timeout()

        # reply false if log doesn't contain an entry at prev_log_index
        # whose term matches prev_log_term
        prev_log_index_log = self.state.get_log_at(request.prev_log_index)
        if prev_log_index_log is None or prev_log_index_log.term != request.prev_log_index:
            return raft_pb2.AppendEntriesResponse(
                term=self.state.current_term, success=False
            )

        # If an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it
        # Append any new entries not already in the log
        made_state_changes = False
        for entry in request.entries:
            existing_log = self.state.get_log_at(entry.index)

            if existing_log is not None and existing_log.term != entry.term:
                self.state.clear_log_from(entry.index)
                made_state_changes = True

            elif existing_log is None and self.state.get_log_len() == entry.index:
                self.state.append_log(entry)
                made_state_changes = True

        if made_state_changes:
            self.state.save()

        # If leader_commit > commit_index, set commit_index =
        # min(leader_commit, index of last new entry)
        if request.leader_commit > self.commit_index:
            self.commit_index = min(request.leader_commit, self.state.get_log_len())

        # TODO: async apply/commit anything that can be

        return raft_pb2.AppendEntriesResponse(
            term=self.state.current_term, success=True
        )

    def handle_request_vote_request(
        self, request: raft_pb2.RequestVoteRequest, context: grpc.ServicerContext
    ) -> raft_pb2.RequestVoteResponse:

        # Reply false if term < currentTerm
        if request.term < self.state.current_term:
            return raft_pb2.RequestVoteResponse(
                term=self.state.current_term, vote_granted=False
            )
        
        if request.term > self.state.current_term:
            self.state.voted_for = None
            self.state.current_term = request.term
            self.state.save()
            self.leader_state = None
            self.leader_id = None
        
        # If votedFor is null or candidateId, and candidate's log is at
        # least as up-to-date as receiver's log, grant vote (§5.2, §5.4)

        # Raft determines which of two logs is more up-to-date
        # by comparing the index and term of the last entries in the
        # logs. If the logs have last entries with different terms, then
        # the log with the later term is more up-to-date. If the logs
        # end with the same term, then whichever log is longer is
        # more up-to-date.
        
        vote_granted = self.state.voted_for in [None, request.candidate_id] and (
            (last_log := self.state.get_last_log()) is None or
            request.last_log_term > last_log.term or 
            request.last_log_term == last_log.term and request.last_log_index >= last_log.index
        )
        
        if vote_granted:
            self.state.voted_for = request.candidate_id
            self.state.save()

        return raft_pb2.RequestVoteResponse(
            term=self.state.current_term, vote_granted=vote_granted
        )

    def temp_all_server_rules(self) -> None:
        # When should we run this?:
        #  If commitIndex > lastApplied: increment lastApplied, apply
        #  log[lastApplied] to state machine 
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            # TODO: apply self.state.log[self.last_applied]
            # commit log / run in state machine
            # need some sort of call back provided by state machine to run the command
            # use queue with worker thread?

        # Run for every RPC recieved:
        #  If RPC request or response contains term T > currentTerm:
        # #  set currentTerm = T, convert to follower
        # if request.term > self.term:
        #     self.term = request.term

    def leader_action() -> None:
        """
        When a leader first comes to power,
        it initializes all nextIndex values to the index just after the
        last one in its log (11 in Figure 7). If a follower's log is
        inconsistent with the leader's, the AppendEntries consistency check will fail in the next AppendEntries RPC. After a rejection, the leader decrements nextIndex and retries
        the AppendEntries RPC. Eventually nextIndex will reach
        a point where the leader and follower logs match. When
        this happens, AppendEntries will succeed, which removes
        any conflicting entries in the follower's log and appends
        entries from the leader's log (if any). Once AppendEntries
        succeeds, the follower's log is consistent with the leader's,
        and it will remain that way for the rest of the term
        """

    def reset_timeout(self) -> None:
        if self.timeout_handler_thread:
            self.timeout_handler_thread.cancel()

        election_timeout_ms: int = random.randint(self.timeout_min_ms, self.timeout_max_ms)
        self.timeout_handler_thread = threading.Timer(
            interval=(election_timeout_ms / 1000), function=self.timeout_handler
        )
        self.timeout_handler_thread.start()

    def timeout_handler(self) -> None:
        print("TIMEOUT HIT!!!")
        self.increment_term()

        # TODO: start election

        # if we win the elction, cancel the timeout thread. how do we know if it went off before we won?

        self.reset_timeout()

    def send_append_entries_to_followers(self, current_term: int) -> None:
        while self._is_leader.is_set():
            curr_time = time.perf_counter()
            heartbeat_interval_sec = 0.1
            next_time = curr_time + heartbeat_interval_sec

            response_queue: queue.Queue[Tuple[int, int, raft_pb2.AppendEntriesResponse]] = queue.Queue()
            processes: List[multiprocessing.Process] = []
            for member_id in self.leader_state.match_index:
                p = multiprocessing.Process(target=self.send_append_entries, args=(member_id, current_term, response_queue))
                processes.append(p)
                p.start()

            for process in processes:
                process.join()

            responses = [response_queue.get() for _ in processes]
            for member_id, entries_len, response in responses:
                self.handle_append_entries_response(member_id, entries_len, response)

            # TODO: calculate commit index

            time.sleep(
                max((next_time - time.perf_counter()), 0)
            )

    def send_append_entries(self, member_id: int, current_term: int, response_queue: queue.Queue[Tuple[int, int, raft_pb2.AppendEntriesResponse]]) -> None:
        next_index = self.leader_state.next_index[member_id]
        prev_log_index = next_index - 1
        prev_log_term = self.state.get_log_term_at(prev_log_index)
        entries = self.state.get_logs_starting_from(next_index)
        try:
            response = member_stub.AppendEntries(raft_pb2.AppendEntriesRequest(
                term=current_term,
                leader_id=self.id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.commit_index
            ))
        except grpc.RpcError as ex:
            print(f"Failed to send heartbeat to member {member_id}: {ex}")
        else:
            response_queue.put((member_id, len(entries), response))
            # self.handle_append_entries_response(member_id, len(missing_entries), response)
    
    def set_new_term(self, new_term: int) -> None:
        with self.thread_lock:
            self.state.current_term = new_term
            self.state.voted_for = None
            self.state.save()
            self._is_leader.clear()
    
    def increment_term(self) -> None:
        with self.thread_lock:
            self.set_new_term(self.state.current_term + 1)

    def handle_append_entries_response(self, member_id: int, sent_entries_len: int, response: raft_pb2.AppendEntriesResponse) -> None:
        if response.term > self.state.current_term:
            self.set_new_term(response.term)
            return

        if response.success:
            # If successful: update nextIndex and matchIndex for
            # follower (§5.3)
            self.leader_state.next_index[member_id] += sent_entries_len
            self.leader_state.match_index[member_id] = self.leader_state.next_index[member_id] - 1
        else:
            # decrement next_index so we can try to get them up to speed
            self.leader_state.next_index[member_id] = max(self.leader_state.next_index[member_id] - 1, 1)
