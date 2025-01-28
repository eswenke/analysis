# Week 3 Results

## Size of pre-processed results
2.02 GB

**Timeframe:** 2022-04-01 20 to 2022-04-01 23

### Ranking of Colors by Distinct Users
- **Top **
  1. Black                  968748
  2. White                  675742
  3. Blood Orange           598784
  4. Dusk Blue              338155
  5. Sun Yellow             251045
  6. Pinky                  209369
  7. Bluish Green           206115
  8. Robin'S Egg            153155
  9. Orange Yellow          128480
  10. Purple                109180
  11. Lighter Green         107890
  12. Dark Sky Blue          88397
  13. Grey                   63239
  14. Light Grey             60834
  15. Sepia                  55968
  16. Pinky Purple           35221

### Average Session Length
- **Output:** 1461.34 seconds

### Percentiles of Pixels Placed
- **Output:**
  - 50th Percentile: 2 pixels
  - 75th Percentile: 5 pixels
  - 90th Percentile: 10 pixels
  - 99th Percentile: 24 pixels

### Count of First-Time Users
- **Output:** 1009955 users
  - NOTE: could not get the proper query to work, computer would explode and
  - was not able to figure out how to chunk or optimize this query to get it to run
  - and return what it needed to. it doesn't get the user count of first EVER pixel
  - placements, just first pixel in given time range. the desired query is
  - commented out above the runnable (incorrect) query, but it just would not run.
  - also, my preprocessing compression for the user_ids capped the unique integer at
  - around 74302.

### Runtime
2289 ms
