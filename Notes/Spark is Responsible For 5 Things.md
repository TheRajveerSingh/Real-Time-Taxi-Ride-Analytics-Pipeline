Spark tracks the state in its internal memory (Stateful Processing).
Spark groups the events into time buckets (Event-Time Windowing).
Spark enforces the cutoff for late data (Watermarking).
Spark writes progress files to storage so it can recover from crashes (Checkpointing).
Spark sorts out the mess when data arrives delayed or backwards (Late & Out-of-Order Events).

## 1. Stateful Stream Processing

* What it means: The system remembers past data to calculate continuous analytics.
* Example: To calculate a running average of ride fares over the last 10 minutes, Spark cannot just look at the current incoming second of data; it must maintain a "state" (memory) of the previous 9 minutes and 59 seconds.

## 2. Event-Time Windowing

* What it means: Grouping data based on when the event actually happened in the real world, rather than when the server received it.
* Example: A rider requests a trip at 5:02 PM, but due to poor cell service, the data doesn't reach your system until 5:15 PM. Event-time windowing ensures this data is correctly counted in the "5:00 PM–5:05 PM" bracket, not the 5:15 PM bracket.

## 3. Watermarking

* What it means: Setting a strict "cutoff time" for how long the system will wait for delayed data before closing the window.
* Example: You set a 10-minute watermark. If a ride event from 5:00 PM arrives at 5:09 PM, Spark accepts it. If it arrives at 5:12 PM, Spark considers it too late and drops it so the system doesn't waste memory waiting forever.

## 4. Checkpointing

* What it means: Periodically saving the exact progress and internal "state" of the pipeline to secure, permanent storage (like HDFS or S3).
* Example: If your server suddenly crashes or loses power at 5:30 PM, checkpointing allows the pipeline to restart, read the saved state, and pick up exactly where it left off without duplicating data or recalculating everything from scratch (fault-tolerant and resumable).

## 5. Late and Out-of-Order Events

* What it means: Data that arrives delayed or scrambled due to network lag.
* Example: Event A happens at 5:01 PM and Event B happens at 5:02 PM. Because of network congestion, Event B arrives at your Kafka cluster before Event A. Your system is designed to automatically re-order and process them accurately.

