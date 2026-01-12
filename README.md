# Brook

Brook is a lightweight message broker with Kafka-like semantics (append-only logs, consumer groups, offset tracking) built primarily for learning and understanding distributed systems.

This project maintains production-ready principles while serving as a lightweight alternative for scenarios where consistency matters more than extreme performance — think microservices choreography, event sourcing in smaller systems, or teams wanting Kafka's mental model without the operational complexity.

# Project Structure

I have kept it super simple for now. No abstractions until needed. There are three major components of a message broker:
1. **storage** - Append-only log implementation. Think dealing with the disk. 
2. **network** - TCP server handling the wire protocol, request routing and connection lifecycle.
3. **brain** - Coordination layer managing topics, partitions, consumer groups, offset tracking, and rebalancing.

```
├── cmd/
├── internal/
├────brain/
├────network/
├────storage/
├── go.mod
├── go.sum 
└── README.md
```
