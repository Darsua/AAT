package main

// Notifier consumer implementation breakdown:
//  1. Load REDPANDA_BROKERS, TOPIC, and CONSUMER_GROUP from the environment.
//  2. Read canonical HazardEvent JSON messages from Redpanda.
//  3. Deduplicate messages by hazard_id.
//  4. Select urgent earthquake, tsunami, and volcanic events.
//  5. Emit one simulated notification for each new urgent event.
//  6. Commit each Kafka message only after successful processing.
//  7. Shut down gracefully when the process receives an interrupt signal.
func main() {
}
