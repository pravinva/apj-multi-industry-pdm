# Real Zerobus Ingestion (No Hardcoded Data)

This path uses:

1. `ot_simulator` from unified-ot-zerobus-connector
2. real `opcua2uc` connector ingestion
3. Bronze DLT reading `pravin_zerobus` (single canonical landing table)

## 1) Start OT simulator (all protocol outputs)

```bash
python -m ot_simulator --protocol all
```

By default this exposes:

- OPC-UA: `opc.tcp://localhost:4840`
- MQTT: `localhost:1883`
- Modbus TCP: `localhost:5020`
- Sparkplug B: MQTT topic family `spBv1.0/#`
- CANBUS: `canbus://vcan0` (default virtual interface)

## 2) Run connector with defaults

Use generated defaults from `core/zerobus_ingest/defaults.yaml`:

```bash
python tools/zerobus_easy_start.py --no-simulator
```

or run connector directly:

```bash
python -m opcua2uc --config config.yaml
```

## 3) Run Databricks connector job

```bash
databricks bundle run -t dev -p DEFAULT --var "industry=mining" ot_pdm_zerobus_connector_job
```

Repeat with `industry=energy|water|automotive|semiconductor`.

## 4) Run Bronze DLT in real mode

```bash
databricks bundle run -t dev -p DEFAULT --var "industry=mining" ot_pdm_dlt_pipeline --refresh-all
```

Ensure pipeline config uses the canonical ingest source:

- `zerobus_source_table = pravin_zerobus`

## 5) Verify Bronze

```sql
SELECT source_protocol, COUNT(*) 
FROM pdm_mining.bronze.sensor_readings
GROUP BY source_protocol
ORDER BY 1;
```

Expected protocol labels include:

- `OPC-UA`
- `MQTT`
- `SPARKPLUG_B` (or connector-equivalent MQTT Sparkplug label)
- `MODBUS`
- `CANBUS`
