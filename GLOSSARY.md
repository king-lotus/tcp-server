# Domain glossary

The names this codebase uses for the concepts that matter. Use these in reviews,
ADRs, and commit messages instead of drifting to "component", "service", or
"handler".

- **frame** — the 8-byte big-endian header plus payload (≤ 64 KiB) on the wire.
- **connection** — one accepted socket and all of its per-peer state.
- **reactor** — one `sub_reactor`: a thread that owns a set of connections and drives
  their epoll I/O, parsing, and lifecycle.
- **close protocol** — the staged shutdown (serve buffered input → drain output →
  send FIN → wait peer FIN → teardown) behind `close_connection` / `stop_gracefully`.
- **drain** — move buffered bytes into (or out of) the kernel until the buffer is
  empty.
- **FIN / RST** — the two ways a TCP close is visible on the wire: orderly vs. reset.
- **pending bytes** — admission credit: bytes reserved for a connection and not yet
  handed to the kernel.
- **admission** — the `max_connections` ceiling plus a connection's lifetime ticket.
- **seam / adapter / depth / leverage / locality** — the architecture vocabulary;
  see the `codebase-design` skill for definitions.

## Deepening decisions (2026-11-09)

- **close_fsm** *(working name)* — the graceful-close state machine being extracted
  from the close protocol. A pure FSM: it owns the close state (`closing`, `fin_sent`,
  `deferred_round`, `close_input_bytes`, …) and *decides* the next `close_action`; the
  reactor performs the effect. It holds a read-only view into `wbuf`/`rbuf`/
  `pending_bytes`.
