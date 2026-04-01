from __future__ import annotations

import struct
from bisect import bisect_right
from dataclasses import dataclass
from pathlib import Path
from typing import Any

@dataclass(frozen=True)
class LogRecord:
    entry: int
    timestamp_us: int
    data: bytes


@dataclass(frozen=True)
class EntryInfo:
    name: str
    type_name: str

def iter_records(raw: bytes) -> list[LogRecord]:
    if len(raw) < 12 or raw[:6] != b"WPILOG":
        raise ValueError("not a WPILOG file")

    header_size = struct.unpack_from("<I", raw, 8)[0]
    pos = 12 + header_size
    records: list[LogRecord] = []

    while pos < len(raw):
        length_byte = raw[pos]
        entry_len = (length_byte & 0x3) + 1
        size_len = ((length_byte >> 2) & 0x3) + 1
        timestamp_len = ((length_byte >> 4) & 0x7) + 1
        header_len = 1 + entry_len + size_len + timestamp_len

        entry = int.from_bytes(raw[pos + 1 : pos + 1 + entry_len], "little")
        size = int.from_bytes(raw[pos + 1 + entry_len : pos + 1 + entry_len + size_len], "little")
        timestamp_us = int.from_bytes(raw[pos + 1 + entry_len + size_len : pos + header_len], "little")
        data = raw[pos + header_len : pos + header_len + size]
        records.append(LogRecord(entry=entry, timestamp_us=timestamp_us, data=data))
        pos += header_len + size

    return records


def decode_control_start(data: bytes) -> tuple[int, EntryInfo] | None:
    if not data or data[0] != 0:
        return None

    offset = 1
    entry = int.from_bytes(data[offset : offset + 4], "little")
    offset += 4

    name_len = int.from_bytes(data[offset : offset + 4], "little")
    offset += 4
    name = data[offset : offset + name_len].decode("utf-8", "replace")
    offset += name_len

    type_len = int.from_bytes(data[offset : offset + 4], "little")
    offset += 4
    type_name = data[offset : offset + type_len].decode("utf-8", "replace")
    offset += type_len

    metadata_len = int.from_bytes(data[offset : offset + 4], "little")
    offset += 4 + metadata_len

    if offset > len(data):
        return None

    return entry, EntryInfo(name=name, type_name=type_name)


def decode_value(type_name: str, data: bytes) -> Any:
    if type_name == "double":
        if len(data) != 8:
            raise ValueError(f"invalid double record length: {len(data)}")
        return struct.unpack("<d", data)[0]
    if type_name == "float":
        if len(data) != 4:
            raise ValueError(f"invalid float record length: {len(data)}")
        return struct.unpack("<f", data)[0]
    if type_name == "boolean":
        if len(data) != 1:
            raise ValueError(f"invalid boolean record length: {len(data)}")
        return bool(data[0])
    if type_name == "int64":
        if len(data) != 8:
            raise ValueError(f"invalid int64 record length: {len(data)}")
        return struct.unpack("<q", data)[0]
    if type_name == "string":
        return data.decode("utf-8", "replace")
    if type_name == "double[]":
        if len(data) % 8 != 0:
            raise ValueError("invalid double[] record length")
        count = len(data) // 8
        return list(struct.unpack(f"<{count}d", data))
    raise ValueError(f"unsupported type {type_name}")


def load_series(log_path: Path) -> dict[str, list[tuple[int, Any]]]:
    raw = log_path.read_bytes()
    entry_info: dict[int, EntryInfo] = {}
    series: dict[str, list[tuple[int, Any]]] = {}

    for record in iter_records(raw):
        if record.entry == 0:
            started = decode_control_start(record.data)
            if started is not None:
                entry, info = started
                entry_info[entry] = info
            continue

        info = entry_info.get(record.entry)
        if info is None:
            continue

        try:
            value = decode_value(info.type_name, record.data)
        except ValueError:
            continue

        series.setdefault(info.name, []).append((record.timestamp_us, value))

    return series


def expand_paths(items: list[str], script_dir: Path) -> list[Path]:
    if not items:
        items = [str(script_dir)]

    resolved: list[Path] = []
    for item in items:
        path = Path(item).expanduser()
        if path.is_dir():
            resolved.extend(sorted(path.glob("*.wpilog")))
        elif path.suffix == ".wpilog":
            resolved.append(path)

    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in resolved:
        resolved_path = path.resolve()
        if resolved_path not in seen:
            deduped.append(resolved_path)
            seen.add(resolved_path)

    return deduped


def split_series(series: list[tuple[int, Any]]) -> tuple[list[int], list[Any]]:
    return [timestamp for timestamp, _ in series], [value for _, value in series]


def value_at(timestamps: list[int], values: list[Any], timestamp_us: int, default: Any = None) -> Any:
    if not timestamps:
        return default
    index = bisect_right(timestamps, timestamp_us) - 1
    if index < 0:
        return default
    return values[index]


def state_at(series: list[tuple[int, Any]], timestamp_us: int, default: Any) -> Any:
    timestamps, values = split_series(series)
    return value_at(timestamps, values, timestamp_us, default)
