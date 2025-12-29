# Architecture

## Components
- Raft-based replicated state machine for Distributed Lock Manager.
- Deadlock detection via wait-for graph cycle detection, resolved by aborting newest request.

## Demo endpoints
- GET /raft/status
- POST /locks/acquire
- POST /locks/release
- GET /locks/state
- GET /locks/wfg
