# Glacier Baseline Snapshot — Before Shutdown

Captured: 2026-03-23T14:11:25Z

## DAG Structure
```
sensor_readings (ROOT, no deps)
  → anomaly_detector (depends on sensor_readings)
    → ops_dashboard (depends on anomaly_detector)
```

## Baseline Values

### sensor_readings
- Transform: rust (compiled binary)
- Total executions: 2
- Last exec ID: `fecb6f3e-0c27-472d-afb3-103b487a53d9`
- Last rows: 5
- Last refresh: 2026-03-23T14:11:19Z
- Last duration: 742ms

### anomaly_detector
- Transform: rust (compiled binary)
- Total executions: 1
- Last exec ID: `1f2d8b87-8100-48f3-9a72-840e96d84357`
- Last rows: 3
- Last refresh: 2026-03-23T14:11:22Z
- Last duration: 955ms

### ops_dashboard
- Transform: rust (compiled binary)
- Total executions: 1
- Last exec ID: `a5fdbfe1-b56b-47c0-9a97-3c6e3729228a`
- Last rows: 1
- Last refresh: 2026-03-23T14:11:25Z
- Last duration: 755ms

## What to Check After Dispatcher Runs

1. Execution IDs must be DIFFERENT (new UUIDs)
2. Last refresh timestamps must be LATER
3. Total executions must be HIGHER
4. Row counts should match (5, 3, 1) — same structure, different data values
5. The actual data (sensor readings, alert IDs, snapshot IDs) will contain different nanosecond timestamps
