import math
import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Literal, Optional, Protocol, Set, Tuple

import grpc

from raft import raft_pb2, raft_pb2_grpc
from raft.state import LeaderState, PersistentState


class ElectionData:
    def __init__(self, election_term: int):
        self.election_term = election_term
        self.votes_received = 0
        self.voters: Set[int] = set()


class FSM(Protocol):
    def apply(self, command: str) -> None:
        ...


class Member:
    def __init__(
        self,
        id: int,
        state: PersistentState,
        members: Dict[int, str],
        timeout_min_ms: int = 300,
        timeout_max_ms: int = 450,
    ):
        self.id = id

        # self.state = PersistentState(
        #     log_file_path=f"raft_state.{self.id}", reuse_state=False
        # )
        self.state = state
        """Persistent state"""

        self.timeout_min_ms = timeout_min_ms
        self.timeout_max_ms = timeout_max_ms
        self.timeout_task: Optional[threading.Timer] = None

        self._lock = threading.RLock()

        self.members: Dict[int, str] = members
        self.member_channels: Dict[int, grpc.Channel] = {}
        self.member_stubs: Dict[int, raft_pb2_grpc.RaftStub] = {}

        self.leader_id: Optional[int] = None
        """ID of the current known leader"""

        """Volatile state on all servers (Reinitialized after election)"""

        self.commit_index: int = 0
        """index of highest log entry known to be committed (initialized to 0, increases monotonically)"""
        self.last_applied: int = 0
        """index of highest log entry applied to state machine (initialized to 0, increases monotonically)"""

        self.leader_state = LeaderState()
        """State when this member is the leader"""

        self._role: Literal["F", "C", "L"] = "F"
        """The role of this member. F = follower, C = candidate, L = leader"""

        self._req_queue: queue.Queue[
            Tuple[Optional[threading.Event], Any]
        ] = queue.Queue()

        self._event_to_append_response: Dict[
            threading.Event, Optional[raft_pb2.AppendEntriesResponse]
        ] = {}
        self._event_to_vote_response: Dict[
            threading.Event, Optional[raft_pb2.RequestVoteResponse]
        ] = {}
        self._event_to_append_log_status: Dict[threading.Event, Optional[bool]] = {}

    # region api for user server

    def append_log(self, command: str) -> bool:
        """Returns True if the command was applied to the FSM"""
        event = threading.Event()
        self._req_queue.put((event, command))
        self._event_to_append_log_status[event] = None
        event.wait()
        res = self._event_to_append_log_status.pop(event)
        assert res is not None
        return res

    # endregion

    # region api for raft servicer

    def process_append_entries_request(
        self, request: raft_pb2.AppendEntriesRequest
    ) -> raft_pb2.AppendEntriesResponse:
        event = threading.Event()
        self._req_queue.put((event, request))
        self._event_to_append_response[event] = None
        event.wait()
        res = self._event_to_append_response.pop(event)
        assert res is not None
        return res

    def process_request_vote_request(
        self, request: raft_pb2.AppendEntriesRequest
    ) -> raft_pb2.RequestVoteResponse:
        event = threading.Event()
        self._req_queue.put((event, request))
        self._event_to_vote_response[event] = None
        event.wait()
        res = self._event_to_vote_response.pop(event)
        assert res is not None
        return res

    # endregion

    # region Base

    def _handle_append_entries_request(
        self, request: raft_pb2.AppendEntriesRequest
    ) -> raft_pb2.AppendEntriesResponse:
        with self._lock:
            # reply false if term < current_term
            if request.term < self.state.current_term:
                return raft_pb2.AppendEntriesResponse(
                    term=self.state.current_term, success=False
                )

            self._reset_timeout()

            # if request.term == self.term and we are leader; this should not be possible.
            # only the leader should be sending AppendEntries, and there can be only be one leader per term
            if request.term == self.state.current_term:
                assert self.leader_id != self.id
            else:
                # only other option is request.term > self.term
                # in that case the leader has changed, if we are not already a follower, we need to become one
                self._set_new_term(request.term, None, request.leader_id)

            # reply false if log doesn't contain an entry at prev_log_index
            # whose term matches prev_log_term
            prev_log_index_log = self.state.get_log_at(request.prev_log_index)
            if (
                prev_log_index_log is None
                or prev_log_index_log.term != request.prev_log_index
            ):
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

            self._reset_timeout()

            return raft_pb2.AppendEntriesResponse(
                term=self.state.current_term, success=True
            )

    def _handle_request_vote_request(
        self, request: raft_pb2.RequestVoteRequest
    ) -> raft_pb2.RequestVoteResponse:
        with self._lock:
            # Reply false if term < currentTerm
            if request.term < self.state.current_term:
                return raft_pb2.RequestVoteResponse(
                    term=self.state.current_term, vote_granted=False
                )

            if request.term > self.state.current_term:
                self._set_new_term(request.term, None)

            # If votedFor is null or candidateId, and candidate's log is at
            # least as up-to-date as receiver's log, grant vote (§5.2, §5.4)

            vote_granted = False
            if self.state.voted_for is None:
                candidate_log_up_to_date = self._check_if_candidate_log_is_up_to_date(
                    request.last_log_index, request.last_log_term
                )

                if candidate_log_up_to_date:
                    vote_granted = self._set_voted_for(request.candidate_id)

            if vote_granted:
                self._reset_timeout()

            return raft_pb2.RequestVoteResponse(
                term=self.state.current_term, vote_granted=vote_granted
            )

    def _check_if_candidate_log_is_up_to_date(
        self, candidate_last_log_index: int, candidate_last_log_term: int
    ) -> bool:
        """Raft determines which of two logs is more up-to-date
        by comparing the index and term of the last entries in the
        logs. If the logs have last entries with different terms, then
        the log with the later term is more up-to-date. If the logs
        end with the same term, then whichever log is longer is
        more up-to-date."""
        return (
            (last_log := self.state.get_last_log()) is None
            or candidate_last_log_term > last_log.term
            or candidate_last_log_term == last_log.term
            and candidate_last_log_index >= last_log.index
        )

    # endregion

    # region Candidate

    def _run_election(self) -> None:
        election_data = ElectionData(self.state.current_term)
        self._set_voted_for(self.id)
        election_data.votes_received = 1  # we voted for ourself

        self._send_request_vote_to_members_and_handle_responses(election_data)
        if election_data.votes_received >= self._get_quorum_val():
            self._role = "L"

    def _send_request_vote_to_members_and_handle_responses(self, election_data) -> None:
        """This takes care of sending out the vote requests and counting them up"""
        request = self._create_request_vote_request()
        with ThreadPoolExecutor(len(self.members)) as t_pool:
            futures = []
            for member_id in self.members.keys():
                if member_id == self.id:
                    continue

                futures.append(
                    t_pool.submit(
                        self._send_request_vote_request,
                        member_id,
                        request,
                    )
                )

            for future in as_completed(futures):
                if result := future.result():
                    member_id, response = result
                    self._handle_request_vote_response(
                        election_data, member_id, response
                    )

    def _create_request_vote_request(self) -> raft_pb2.RequestVoteRequest:
        last_log_index = 0
        last_log_term = 0

        if last_log := self.state.get_last_log():
            last_log_index = last_log.index
            last_log_term = last_log.term

        return raft_pb2.RequestVoteRequest(
            term=self.state.current_term,
            candidate_id=self.id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )

    def _send_request_vote_request(
        self,
        member_id: int,
        append_entries_request: raft_pb2.RequestVoteRequest,
    ) -> Optional[Tuple[int, raft_pb2.RequestVoteResponse]]:
        """Sends append entries request and handles response
        :param member_id: id of the member to send the request to
        :param sent_index: index of highest sent log
        :param append_entries_request: the request to send
        """
        member_stub = self.member_stubs[member_id]
        try:
            return (
                member_id,
                member_stub.RequestVote(append_entries_request),
            )
        except grpc.RpcError as ex:
            print(f"Failed to send RequestVote to member {member_id}: {ex}")
            return None

    def _handle_request_vote_response(
        self,
        election_data: ElectionData,
        member_id: int,
        response: raft_pb2.RequestVoteResponse,
    ) -> None:
        """Returns true if the vote was provided"""
        if response.term > election_data.election_term:
            with self._lock:
                self._set_new_term(response.term, None)
            return

        if response.vote_granted and member_id not in election_data.voters:
            election_data.voters.add(member_id)
            election_data.votes_received += 1

    # endregion Candidate

    # region Leader

    def _initialize_leader(self) -> None:
        # TODO: initialize indexes in leader_state
        pass

    def _send_append_entries_to_followers(self, current_term: int) -> None:
        with ThreadPoolExecutor(len(self.leader_state.next_index)) as t_pool:
            while self._role == "L":
                curr_time = time.perf_counter()
                heartbeat_interval_sec = 0.1
                next_time = curr_time + heartbeat_interval_sec

                futures = []
                requests = self._create_append_entries_requests(current_term)
                for member_id, sent_index, request in requests:
                    futures.append(
                        t_pool.submit(
                            self._send_append_entries_request,
                            member_id,
                            sent_index,
                            request,
                        )
                    )

                for future in as_completed(futures):
                    if result := future.result():
                        member_id, sent_index, response = result
                        self._handle_append_entries_response(
                            member_id, sent_index, response
                        )

                with self._lock:
                    self._calculate_commit_index()

                time.sleep(max((next_time - time.perf_counter()), 0))

    def _create_append_entries_requests(
        self, current_term: int
    ) -> List[Tuple[int, int, raft_pb2.AppendEntriesRequest]]:
        with self._lock:
            requests: List[Tuple[int, int, raft_pb2.AppendEntriesRequest]] = []
            for member_id, next_index in self.leader_state.next_index.items():
                if member_id == self.id:
                    continue

                append_entries_request = self._create_append_entries_request(
                    current_term, next_index
                )
                sent_index = next_index + len(append_entries_request.entries) - 1
                requests.append((member_id, sent_index, append_entries_request))
            return requests

    def _create_append_entries_request(
        self, term: int, next_index: int
    ) -> raft_pb2.AppendEntriesRequest:
        prev_log_index = next_index - 1
        prev_log_term = self.state.get_log_term_at(prev_log_index)
        entries = self.state.get_logs_starting_from(next_index, max=10)

        return raft_pb2.AppendEntriesRequest(
            term=term,
            leader_id=self.id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.commit_index,
        )

    def _send_append_entries_request(
        self,
        member_id: int,
        sent_index: int,
        append_entries_request: raft_pb2.AppendEntriesRequest,
    ) -> Optional[Tuple[int, int, raft_pb2.AppendEntriesResponse]]:
        """Sends append entries request and handles response
        :param member_id: id of the member to send the request to
        :param sent_index: index of highest sent log
        :param append_entries_request: the request to send
        """
        member_stub = self.member_stubs[member_id]
        try:
            return (
                member_id,
                sent_index,
                member_stub.AppendEntries(append_entries_request),
            )
        except grpc.RpcError as ex:
            print(f"Failed to send AppendEntries to member {member_id}: {ex}")
            return None

    def _handle_append_entries_response(
        self,
        member_id: int,
        sent_index: int,
        response: raft_pb2.AppendEntriesResponse,
    ) -> None:
        """Used by leader"""
        with self._lock:
            if response.term > self.state.current_term:
                self._set_new_term(response.term, None)
                return

            if self._role != "L":
                return

            # handle old responses
            if sent_index < self.leader_state.match_index[member_id]:
                return

            if response.success:
                # If successful: update nextIndex and matchIndex for
                # follower (§5.3)
                self.leader_state.next_index[member_id] = sent_index
                self.leader_state.match_index[member_id] = (
                    self.leader_state.next_index[member_id] - 1
                )
            else:
                # decrement next_index so we can try to get them up to speed
                self.leader_state.next_index[member_id] = max(
                    self.leader_state.next_index[member_id] - 1, 1
                )

    def _calculate_commit_index(self) -> None:
        with self._lock:
            # Find the highest log index replicated on a majority of servers
            sorted_match_indexes = sorted(
                self.leader_state.match_index.values(), reverse=True
            )
            new_commit_index = sorted_match_indexes[self._get_quorum_val() - 1]
            if new_commit_index > self.commit_index:
                self.commit_index = new_commit_index

    # endregion Leader

    # region state loops

    def start_raft_processing(self) -> None:
        # TODO Run self._base_loop it's own thread; 'RAFT processor'
        pass

    def stop_raft_processing(self) -> None:
        # TODO Stop the 'RAFT processor' thread
        pass

    def _base_loop(self) -> None:
        while True:  # might want to add some condition here
            if self._role == "F":
                self._follower_loop()
            elif self._role == "C":
                self._candidate_loop()
            elif self._role == "F":
                self._leader_loop()
            else:
                raise ValueError(f"Invalid role: {self._role}")

    def _handle_default_actions(self, event: threading.Event, action: Any) -> None:
        if isinstance(action, raft_pb2.AppendEntriesRequest):
            append_res = self._handle_append_entries_request(action)
            self._event_to_append_response[event] = append_res
            event.set()

        elif isinstance(action, raft_pb2.RequestVoteRequest):
            vote_res = self._handle_request_vote_request(action)
            self._event_to_vote_response[event] = vote_res
            event.set()

        else:
            raise TypeError(f"Invalid action provided: '{action}'")

    def _follower_loop(self) -> None:
        while self._role == "F":
            try:
                # adding timeout here just in case
                event, action = self._req_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if event is None:
                if isinstance(action, TimeoutError):
                    # start election process
                    self._role = "C"
                    return

                raise TypeError(f"Invalid action provided: {action}")

            if isinstance(action, str):
                # TODO: redirect to leader
                # self._event_to_append_log_status[event]
                # event.set()
                raise NotImplementedError()

            self._handle_default_actions(event, action)

    def _candidate_loop(self) -> None:
        self._reset_timeout()

        self._run_election()
        # if we got enough votes and became leader, exit, otherwise do loop below
        if self._role == "L":
            return

        while self._role == "C":
            try:
                # adding timeout here just in case
                event, action = self._req_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if event is None:
                if isinstance(action, TimeoutError):
                    # restart election process
                    return

                raise TypeError(f"Invalid action provided: {action}")

            if isinstance(action, str):
                # TODO: ? probably just respond as 'busy'
                # event.set()
                raise NotImplementedError()

            self._handle_default_actions(event, action)

    def _leader_loop(self) -> None:
        self._cancel_timeout()

        while self._role == "L":
            try:
                # adding timeout here just in case
                event, action = self._req_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if event is None:
                raise TypeError(f"Invalid action provided: {action}")

            if isinstance(action, str):
                # TODO: process action -- append to log;
                # need to wait till the action gets committed before setting the event,
                # but we don't want to sit here and block everything while that happens.
                # will need to keep the event somewhere until the action is committed, and then set it
                raise NotImplementedError()

            self._handle_default_actions(event, action)

    # endregion

    # region Misc

    def _create_member_stubs(self) -> None:
        for member_id, member_address in self.members.items():
            if self.member_stubs.get(member_id) is not None:
                continue

            channel = self.member_channels.get(member_id)
            if channel is None:
                channel = grpc.insecure_channel(member_address)
                self.member_channels[member_id] = channel

            self.member_stubs[member_id] = raft_pb2_grpc.RaftStub(channel)

    def _close_member_stubs(self) -> None:
        for member_channel in self.member_channels.values():
            member_channel.close()

        self.member_channels.clear()
        self.member_stubs.clear()

    def _reset_timeout(self) -> None:
        self._cancel_timeout()

        election_timeout_ms: int = random.randint(
            self.timeout_min_ms, self.timeout_max_ms
        )
        self.timeout_task = threading.Timer(
            election_timeout_ms * 1000, self._timeout_handler
        )
        self.timeout_task.start()

    def _cancel_timeout(self) -> None:
        if self.timeout_task:
            self.timeout_task.cancel()

    def _timeout_handler(self) -> None:
        self._req_queue.put((None, TimeoutError()))

    def _set_new_term(
        self, new_term: int, voted_for: Optional[int], leader_id: Optional[int] = None
    ) -> None:
        if new_term > self.state.current_term:
            self.state.set_new_term(new_term, voted_for)
            self._role = "C" if voted_for == self.id else "F"
            self.leader_id = leader_id

    def _get_quorum_val(self) -> int:
        """The number of votes required to reach majority"""
        # TODO
        return math.floor(len(self.members) / 2) + 1

    def temp_all_server_rules(self) -> None:
        # TODO: When should we run this?:
        #  If commitIndex > lastApplied: increment lastApplied, apply
        #  log[lastApplied] to state machine
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            # TODO: apply self.state.log[self.last_applied]
            # commit log / run in state machine
            # need some sort of call back provided by state machine to run the command

    def _set_voted_for(self, candidate_id: int):
        self.state.set_voted_for(candidate_id)

    # endregion Misc


MEMBER_ID_TO_ADDRESS: Dict[int, str] = {
    0: "[::]:50050",
    1: "[::]:50051",
    2: "[::]:50052",
    3: "[::]:50053",
    4: "[::]:50054",
}
