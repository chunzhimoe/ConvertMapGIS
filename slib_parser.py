"""
slib_parser.py — MapGIS 符号库 (.lib) 文件解析模块

支持三种符号库文件：
  - Subgraph.lib   点符号库（子图）
  - Fillgrph.lib   面填充符号库
  - LINESTY.lib    线型符号库

目录格式（经过逆向工程）：
  Subgraph.lib  : 4-byte per entry  → u32 file_offset
  Fillgrph.lib  : 12-byte per entry → (u32, u32, u32 file_offset)
  LINESTY.lib   : 12-byte per entry → (u32, u32, u32 file_offset)

Fillgrph/Subgraph 记录格式（标准）：
  u16 part_count
  u16 float_count
  part_count * (6 × u16)  = 12 bytes each
  float_count  * float32

LINESTY 记录格式：
  顶层记录起始处：N × (u32 abs_sub_offset, u16 sub_length) 子记录目录
  N = (first_sub_abs_offset - record_start) / 6
  各子记录：
    u16 prim_count
    prim_count × (u16 type, u16 point_count, u16 float_offset) → 6 bytes each
    float payload（坐标对，从记录末尾反推）

用法示例：
    from slib_parser import SlibDirectory
    slib = SlibDirectory('/path/to/slib_dir')
    point_sym  = slib.lookup_point(子图号)
    line_sym   = slib.lookup_line(线型号, 辅助线号, 覆盖方式)
    fill_sym   = slib.lookup_fill(填充符号)
"""

import os
import struct


# ─────────────────────────────────────────────────────────────────────────────
# 内部工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _read_u16(data, offset):
    return struct.unpack_from('<H', data, offset)[0]

def _read_u32(data, offset):
    return struct.unpack_from('<I', data, offset)[0]

def _read_f32_array(data, offset, count):
    if count == 0:
        return []
    if offset + count * 4 > len(data):
        count = max(0, (len(data) - offset) // 4)
    if count == 0:
        return []
    return list(struct.unpack_from(f'<{count}f', data, offset))


# ─────────────────────────────────────────────────────────────────────────────
# 目录读取
# ─────────────────────────────────────────────────────────────────────────────

def _read_directory_4b(data):
    """Subgraph.lib：4字节/条目，直接为文件偏移量（u32 LE）。"""
    if len(data) < 4:
        return []
    n = _read_u32(data, 0)
    offsets = []
    for i in range(n):
        ep = 4 + i * 4
        if ep + 4 > len(data):
            break
        offsets.append(_read_u32(data, ep))
    return offsets


def _read_directory_12b(data):
    """Fillgrph.lib / LINESTY.lib：12字节/条目，偏移量在第3个 u32（+8）。"""
    if len(data) < 4:
        return []
    n = _read_u32(data, 0)
    offsets = []
    for i in range(n):
        ep = 4 + i * 12
        if ep + 12 > len(data):
            break
        offsets.append(_read_u32(data, ep + 8))
    return offsets


# ─────────────────────────────────────────────────────────────────────────────
# Subgraph.lib / Fillgrph.lib 记录解析
# ─────────────────────────────────────────────────────────────────────────────

# Fillgrph.lib 中特殊记录（extra 4 字节前缀）的索引集合
_FILLGRPH_SPECIAL = {0, 2}


def _parse_subgraph_record(data, abs_offset, extra_header_bytes=0):
    """
    解析一条 Subgraph/Fillgrph 记录。

    参数
    ----
    data              : bytes — 整个库文件字节
    abs_offset        : int   — 记录在文件中的起始偏移
    extra_header_bytes: int   — 额外前缀字节数（Fillgrph 特殊记录用，通常为 0 或 4）

    返回 dict
    """
    pos = abs_offset + extra_header_bytes
    if pos + 4 > len(data):
        return {'ok': False, 'error': 'truncated at header', 'record_start': abs_offset}

    part_count  = _read_u16(data, pos);    pos += 2
    float_count = _read_u16(data, pos);    pos += 2

    parts = []
    for _ in range(part_count):
        if pos + 12 > len(data):
            return {'ok': False, 'error': 'truncated in parts',
                    'record_start': abs_offset, 'part_count': part_count, 'float_count': float_count}
        descriptor = list(struct.unpack_from('<6H', data, pos))
        parts.append(descriptor)
        pos += 12

    floats = _read_f32_array(data, pos, float_count)

    return {
        'ok': True,
        'record_start': abs_offset,
        'extra_header_bytes': extra_header_bytes,
        'part_count': part_count,
        'float_count': float_count,
        'parts': parts,
        'floats': floats,
    }


def _load_subgraph_lib(path, special_indices=None):
    """解析 Subgraph.lib（4-byte 目录）。"""
    if special_indices is None:
        special_indices = set()

    with open(path, 'rb') as f:
        data = f.read()

    offsets = _read_directory_4b(data)
    records = []
    for i, off in enumerate(offsets):
        extra = 4 if i in special_indices else 0
        rec = _parse_subgraph_record(data, off, extra_header_bytes=extra)
        rec['index'] = i
        records.append(rec)

    return {
        'ok': True,
        'path': path,
        'record_count': len(offsets),
        'records': records,
    }


def _load_fillgrph_lib(path, special_indices=None):
    """解析 Fillgrph.lib（12-byte 目录，偏移量在 +8）。"""
    if special_indices is None:
        special_indices = set()

    with open(path, 'rb') as f:
        data = f.read()

    offsets = _read_directory_12b(data)
    records = []
    for i, off in enumerate(offsets):
        extra = 4 if i in special_indices else 0
        if off == 0 or off >= len(data):
            records.append({'ok': False, 'error': f'invalid offset {off}', 'index': i})
            continue
        rec = _parse_subgraph_record(data, off, extra_header_bytes=extra)
        rec['index'] = i
        records.append(rec)

    return {
        'ok': True,
        'path': path,
        'record_count': len(offsets),
        'records': records,
    }


# ─────────────────────────────────────────────────────────────────────────────
# LINESTY.lib 记录解析
# ─────────────────────────────────────────────────────────────────────────────

def _parse_linesty_subrecord(data, abs_offset, sub_length):
    """
    解析 LINESTY.lib 中的一条子记录。

    参数
    ----
    data       : bytes — 整个文件
    abs_offset : int   — 子记录在文件中的绝对起始偏移
    sub_length : int   — 子记录总字节数
    """
    pos = abs_offset
    if pos + 2 > len(data):
        return {'ok': False, 'error': 'truncated at prim_count', 'abs_start': abs_offset}

    prim_count = _read_u16(data, pos); pos += 2

    primitives = []
    for _ in range(prim_count):
        if pos + 6 > len(data):
            return {'ok': False, 'error': 'truncated in primitives',
                    'prim_count': prim_count, 'abs_start': abs_offset}
        ptype     = _read_u16(data, pos); pos += 2
        pt_count  = _read_u16(data, pos); pos += 2
        float_off = _read_u16(data, pos); pos += 2
        primitives.append({'type': ptype, 'point_count': pt_count, 'float_offset': float_off})

    # Float payload is at the end of the subrecord
    total_floats = sum(p['point_count'] * 2 for p in primitives)
    float_payload_start = abs_offset + sub_length - total_floats * 4

    # Sanity check: payload must not overlap the primitive header
    header_end = abs_offset + 2 + prim_count * 6
    if float_payload_start < header_end:
        float_payload_start = header_end  # lenient fallback

    floats = _read_f32_array(data, float_payload_start, total_floats)

    # Distribute floats as (x, y) pairs per primitive
    fi = 0
    for p in primitives:
        pc = p['point_count']
        coords = []
        for _ in range(pc):
            if fi + 1 < len(floats):
                coords.append((round(floats[fi], 6), round(floats[fi + 1], 6)))
            fi += 2
        p['coords'] = coords

    return {
        'ok': True,
        'abs_start': abs_offset,
        'sub_length': sub_length,
        'prim_count': prim_count,
        'primitives': primitives,
        'total_floats': total_floats,
    }


def _parse_linesty_record(data, rec_abs_offset, rec_length=None):
    """
    解析 LINESTY.lib 中的一条顶层记录。

    顶层记录起始处存放若干 (u32 abs_sub_offset, u16 sub_length) 条目组成的子记录目录。
    子记录数量 N = (first_sub_abs_offset - rec_abs_offset) / 6。

    参数
    ----
    data            : bytes — 整个文件
    rec_abs_offset  : int   — 记录起始绝对偏移
    rec_length      : int   — 记录字节长度（由目录相邻偏移计算，用于边界检查）
    """
    if rec_abs_offset + 6 > len(data):
        return {'ok': False, 'error': 'truncated at subdir', 'rec_start': rec_abs_offset}

    # 读取第一个子记录的绝对偏移，由此推断子记录数
    first_sub_off = _read_u32(data, rec_abs_offset)

    # Sanity check: first_sub_off must be strictly inside this record's byte range
    rec_end = rec_abs_offset + rec_length if rec_length else len(data)
    if first_sub_off <= rec_abs_offset or first_sub_off >= rec_end:
        # Empty or corrupt record — treat as having no subrecords
        return {'ok': True, 'empty': True, 'subrecords': [], 'prim_count': 0,
                'rec_start': rec_abs_offset}

    n_subdirs = (first_sub_off - rec_abs_offset) // 6
    if n_subdirs <= 0:
        return {'ok': True, 'empty': True, 'subrecords': [], 'prim_count': 0,
                'rec_start': rec_abs_offset}

    # 读取子记录目录
    sub_entries = []
    for j in range(n_subdirs):
        ep = rec_abs_offset + j * 6
        if ep + 6 > len(data):
            break
        sub_off = _read_u32(data, ep)
        sub_len = _read_u16(data, ep + 4)
        sub_entries.append((sub_off, sub_len))

    # 解析各子记录
    subrecords = []
    for sub_off, sub_len in sub_entries:
        if sub_off == 0 or sub_off >= len(data) or sub_len == 0:
            subrecords.append({'ok': False, 'error': 'invalid subrecord entry'})
            continue
        sr = _parse_linesty_subrecord(data, sub_off, sub_len)
        subrecords.append(sr)

    # 使用第一个有效子记录的 prim_count 作为顶层记录摘要
    prim_count = next((s['prim_count'] for s in subrecords if s.get('ok')), 0)

    return {
        'ok': True,
        'rec_start': rec_abs_offset,
        'n_subdirs': n_subdirs,
        'prim_count': prim_count,
        'subrecords': subrecords,
    }


def _load_linesty_lib(path):
    """解析 LINESTY.lib（12-byte 目录，偏移量在 +8）。"""
    with open(path, 'rb') as f:
        data = f.read()

    if len(data) < 4:
        return {'ok': False, 'error': 'file too small', 'records': []}

    offsets = _read_directory_12b(data)
    n = len(offsets)

    # Build a sorted list of valid offsets so we can compute tight record bounds.
    # Many directory entries may share offsets or have off=0 (padding records).
    valid_off_set = sorted({o for o in offsets if 0 < o < len(data)})
    # Map each valid offset → its "next" valid offset (upper bound for this record)
    _next_valid = {}
    for idx, o in enumerate(valid_off_set):
        _next_valid[o] = valid_off_set[idx + 1] if idx + 1 < len(valid_off_set) else len(data)

    records = []
    for i, off in enumerate(offsets):
        if off == 0 or off >= len(data):
            records.append({'ok': False, 'error': f'invalid offset {off}',
                            'index': i, 'empty': True, 'subrecords': [], 'prim_count': 0})
            continue
        # Tight rec_length derived from sorted valid offsets
        rec_length = _next_valid.get(off, len(data)) - off
        rec = _parse_linesty_record(data, off, rec_length=rec_length)
        rec['index'] = i
        records.append(rec)

    return {
        'ok': True,
        'path': path,
        'record_count': len(offsets),
        'records': records,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SlibDirectory — 主入口
# ─────────────────────────────────────────────────────────────────────────────

class SlibDirectory:
    """
    MapGIS slib 符号库目录。

    参数
    ----
    slib_dir : str — 含有 Subgraph.lib / Fillgrph.lib / LINESTY.lib 的目录路径

    属性
    ----
    subgraph  : dict — Subgraph.lib 解析结果（None 若文件不存在）
    fillgrph  : dict — Fillgrph.lib 解析结果（None 若文件不存在）
    linesty   : dict — LINESTY.lib  解析结果（None 若文件不存在）
    ok        : bool — 至少一个库文件成功加载
    """

    def __init__(self, slib_dir: str):
        self.slib_dir = slib_dir
        self.subgraph = None
        self.fillgrph = None
        self.linesty  = None
        self.ok       = False
        self._load()

    def _find_lib(self, name: str):
        """大小写不敏感地查找库文件路径。"""
        target = name.lower()
        try:
            for fname in os.listdir(self.slib_dir):
                if fname.lower() == target:
                    return os.path.join(self.slib_dir, fname)
        except OSError:
            pass
        return None

    def _load(self):
        try:
            p = self._find_lib('subgraph.lib')
            if p:
                self.subgraph = _load_subgraph_lib(p, special_indices=set())
                if self.subgraph.get('ok'):
                    self.ok = True
        except Exception as e:
            self.subgraph = {'ok': False, 'error': str(e), 'records': []}

        try:
            p = self._find_lib('fillgrph.lib')
            if p:
                self.fillgrph = _load_fillgrph_lib(p, special_indices=_FILLGRPH_SPECIAL)
                if self.fillgrph.get('ok'):
                    self.ok = True
        except Exception as e:
            self.fillgrph = {'ok': False, 'error': str(e), 'records': []}

        try:
            p = self._find_lib('linesty.lib')
            if p:
                self.linesty = _load_linesty_lib(p)
                if self.linesty.get('ok'):
                    self.ok = True
        except Exception as e:
            self.linesty = {'ok': False, 'error': str(e), 'records': []}

    # ── 查找接口 ──────────────────────────────────────────────────────────────

    def lookup_point(self, sym_id: int) -> dict:
        """查找点符号（子图）记录。sym_id = WT「子图号」字段值。"""
        if self.subgraph is None or not self.subgraph.get('ok'):
            return {'ok': False, 'reason': 'Subgraph.lib not loaded', 'sl_id': sym_id}
        records = self.subgraph.get('records', [])
        if sym_id < 0 or sym_id >= len(records):
            return {'ok': False, 'reason': f'index {sym_id} out of range [0, {len(records)})',
                    'sl_id': sym_id}
        rec = records[sym_id]
        return {**rec, 'sl_lib': 'subgraph', 'sl_id': sym_id}

    def lookup_line(self, line_type: int, aux_line: int = 0, cover_mode: int = 0) -> dict:
        """查找线型记录。line_type = WL「线型号」字段值。"""
        if self.linesty is None or not self.linesty.get('ok'):
            return {'ok': False, 'reason': 'LINESTY.lib not loaded',
                    'sl_type': line_type, 'sl_aux': aux_line, 'sl_cov': cover_mode}
        records = self.linesty.get('records', [])
        if line_type < 0 or line_type >= len(records):
            return {'ok': False, 'reason': f'index {line_type} out of range [0, {len(records)})',
                    'sl_type': line_type, 'sl_aux': aux_line, 'sl_cov': cover_mode}
        rec = records[line_type]
        return {**rec, 'sl_lib': 'linesty', 'sl_type': line_type,
                'sl_aux': aux_line, 'sl_cov': cover_mode}

    def lookup_fill(self, fill_sym: int) -> dict:
        """查找面填充符号记录。fill_sym = WP「填充符号」字段值。"""
        if self.fillgrph is None or not self.fillgrph.get('ok'):
            return {'ok': False, 'reason': 'Fillgrph.lib not loaded', 'sl_id': fill_sym}
        records = self.fillgrph.get('records', [])
        if fill_sym < 0 or fill_sym >= len(records):
            return {'ok': False, 'reason': f'index {fill_sym} out of range [0, {len(records)})',
                    'sl_id': fill_sym}
        rec = records[fill_sym]
        return {**rec, 'sl_lib': 'fillgrph', 'sl_id': fill_sym}

    # ── 统计 ────────────────────────────────────────────────────────────────

    def stats(self) -> dict:
        """返回各库文件的基本统计信息。"""
        def _stat(lib):
            if lib is None:
                return {'loaded': False}
            if not lib.get('ok'):
                return {'loaded': False, 'error': lib.get('error', 'unknown')}
            recs = lib.get('records', [])
            ok_count = sum(1 for r in recs if r.get('ok'))
            return {
                'loaded': True,
                'record_count': lib.get('record_count', 0),
                'ok_count': ok_count,
            }

        return {
            'subgraph': _stat(self.subgraph),
            'fillgrph': _stat(self.fillgrph),
            'linesty':  _stat(self.linesty),
            'ok': self.ok,
        }
