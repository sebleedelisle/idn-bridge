# IDN Bridge

`IDN Bridge` discovers DACs with [`libera-core`](https://github.com/sebleedelisle/libera-core) and exposes each discovered DAC as an OpenIDN service endpoint.

All discovered DACs are exposed as separate OpenIDN services on a single UDP port (default `7255`), matching standard OpenIDN behavior.

## Build

```bash
cmake -S . -B build
cmake --build build -j
```

`IDN_BRIDGE_OPENIDN_BRIDGE_MODE` controls bridge-specific OpenIDN behavior:

- `ON` (default): enables macOS/bridge behavior patches.
- `OFF`: keeps original OpenIDN behavior paths.

## Run

```bash
./build/idn_bridge
```

Useful options:

```bash
./build/idn_bridge --idn-port 7255 --discovery-timeout-ms 8000 --max-dacs 4
```

## Notes

- Streaming to hardware uses a Libera callback-backed queue.
- Point data and effective point-rate changes coming from IDN chunks are translated to Libera point streams.
- Already-IDN Helios network DACs are skipped automatically (only non-IDN DACs are bridged).
