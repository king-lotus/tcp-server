# Connection state machine

Generated from `tcp-server-mt.h` by `tcp_server_fsm_test --table`; the table below is produced by
running the machine, so it cannot disagree with the code.

A connection's behavior is decided by one machine (`conn_machine`, pure core in `conn_sm::`). It
replaced eleven fields on two structs. The state is three regions plus the data the close protocol
decides with.

## Regions

| region | values | meaning |
|---|---|---|
| phase | `open`, `draining`, `closing`, `fin_sent`, `closed` | lifecycle, forward only |
| read | `active`, `paused`, `eof` | read-side backpressure; the peer's FIN |
| write | `write_armed` | EPOLLOUT wanted |
| extended | `in_round`, `round`, `close_input_bytes`, `close_started_at_ms`, `paused_at_ms` | what the close protocol decides with |

Phases: `open` normal; `draining` a close was requested and buffered input is being served
(submissions still accepted, because the replies are what the close delivers); `closing` parked
(submissions refused, wbuf draining, FIN pending); `fin_sent` our FIN is out; `closed` terminal.
`open` and `draining` may go straight to `closing`; any phase may go to `closed`.

## Invariant (`conn_sm::violation`, asserted on every transition)

1. paused <=> pause timestamp
2. paused only in open/draining, outside a serving round
3. closed is terminal and normalised
4. a serving round needs a requested close
5. open has no measured input budget
6. draining has a measured input budget
7. the close clock runs while parked
8. the close clock is off before parking
9. a close-owned round needs a requested close
10. a peer-owned round needs the peer's FIN

Everything not excluded by these clauses is a reachable combination, deliberately: for example
`fin_sent` with `write_armed` set (a park that drains completely sends its FIN without disarming; the
stale EPOLLOUT disarms it), and `draining` with `read=paused` between serving rounds.

## Events and preconditions

| event | precondition | notes |
|---|---|---|
| `on_pause` | none | refused unless `read=active`, phase `open`/`draining`, outside a round |
| `on_resume` | none | returns the paused interval for the frame-assembly deadline |
| `on_eof` | none | recv() == 0 |
| `on_set_write` | none | false when nothing changed, or closed |
| `on_graceful` | none | entering `closing` resumes a paused connection |
| `on_request` | none | measures the input budget once; resumes a paused connection |
| `on_round_begin` / `on_round_end` | reads not paused | brackets a serving round |
| `on_input_drained`, `on_input_defer_failed` | a close was requested | the park decision |
| `on_peer_fin` | `read=eof` | FIN, defer one round, or tear down |
| `on_fin_sent` | phase `closing` | shutdown(SHUT_WR) succeeded |
| `on_closed` | none | true once; normalises write/round/pause |

Entering `closing` or `draining` resumes reads *inside the machine* and returns a `read_resume` for
the reactor to apply (EPOLL_CTL_MOD, deadline shift, continuation), because "closing and paused" is
not a state.

## Transition table (representative facts)

`peer FIN ... replies owed` means wbuf is empty but `pending_bytes > 0`.

| state (phase, read) | event | next (phase, read) | action | reactor must also |
|---|---|---|---|---|
| open, active | pause (backpressure) | open, paused | (paused) | drop EPOLLIN |
| open, active | resume | open, active | none |  |
| open, active | graceful close / idle | closing, active | drain_wbuf | drain wbuf |
| open, active | close_connection (request) | draining, active | drain_input | apply the implied resume; run a serving round |
| open, active | recv() == 0 | open, eof | (eof) | finish_peer_closed |
| open, active | teardown | closed, active | (closed) | close fd, account, deferred callback |
| open, paused | pause (backpressure) | open, paused | refused |  |
| open, paused | resume | open, active | (resumed) | restore EPOLLIN; shift frame deadline; schedule if rbuf non-empty |
| open, paused | graceful close / idle | closing, active | drain_wbuf | apply the implied resume; drain wbuf |
| open, paused | close_connection (request) | draining, active | drain_input | apply the implied resume; run a serving round |
| open, paused | recv() == 0 | open, eof | (eof) | finish_peer_closed |
| open, paused | teardown | closed, active | (closed) | close fd, account, deferred callback |
| open, eof | pause (backpressure) | open, eof | refused |  |
| open, eof | resume | open, eof | none |  |
| open, eof | graceful close / idle | closing, eof | drain_wbuf | drain wbuf |
| open, eof | close_connection (request) | draining, eof | drain_input | apply the implied resume; run a serving round |
| open, eof | recv() == 0 | open, eof | (eof) | finish_peer_closed |
| open, eof | peer FIN, wbuf empty, nothing owed | open, eof | close_graceful |  |
| open, eof | peer FIN, wbuf empty, replies owed | open, eof | defer_peer_closed | queue one round |
| open, eof | teardown | closed, eof | (closed) | close fd, account, deferred callback |
| draining, active | pause (backpressure) | draining, paused | (paused) | drop EPOLLIN |
| draining, active | resume | draining, active | none |  |
| draining, active | graceful close / idle | closing, active | drain_wbuf | drain wbuf |
| draining, active | close_connection (request) | draining, active | drain_input | apply the implied resume; run a serving round |
| draining, active | recv() == 0 | draining, eof | (eof) | finish_peer_closed |
| draining, active | teardown | closed, active | (closed) | close fd, account, deferred callback |
| closing, active | pause (backpressure) | closing, active | refused |  |
| closing, active | resume | closing, active | none |  |
| closing, active | graceful close / idle | closing, active | drain_wbuf | drain wbuf |
| closing, active | close_connection (request) | closing, active | drain_input | apply the implied resume; run a serving round |
| closing, active | recv() == 0 | closing, eof | (eof) | finish_peer_closed |
| closing, active | shutdown(SHUT_WR) ok | fin_sent, active | (fin sent) |  |
| closing, active | teardown | closed, active | (closed) | close fd, account, deferred callback |
| fin_sent, active | pause (backpressure) | fin_sent, active | refused |  |
| fin_sent, active | resume | fin_sent, active | none |  |
| fin_sent, active | graceful close / idle | fin_sent, active | none |  |
| fin_sent, active | close_connection (request) | fin_sent, active | none |  |
| fin_sent, active | recv() == 0 | fin_sent, eof | (eof) | finish_peer_closed |
| fin_sent, active | teardown | closed, active | (closed) | close fd, account, deferred callback |
| closed, active | pause (backpressure) | closed, active | refused |  |
| closed, active | resume | closed, active | none |  |
| closed, active | graceful close / idle | closed, active | none |  |
| closed, active | close_connection (request) | closed, active | none |  |
| closed, active | recv() == 0 | closed, eof | (eof) | finish_peer_closed |
| closed, active | teardown | closed, active | none |  |
