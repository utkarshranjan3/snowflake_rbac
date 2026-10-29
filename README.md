Here is a complete, end-to-end Uber Data Engineering project in Snowflake, structured step-by-step with production-grade code.

This project covers RBAC, Snowpipe (Incremental Loading), Streams & CDC, SCD Type 2, Task Scheduling, Time Travel / Table Properties, and Visualization.

Architecture Overview
[JSON Files in Stage] 
         │ (Snowpipe - Automated Incremental Load)
         ▼
[Raw Table] ──► [Stream (CDC)] ──► [SCD Type 2 Task] ──► [Silver/Gold Dimension] 
                                                                  │
                                                        [Time Travel / Analytics]
