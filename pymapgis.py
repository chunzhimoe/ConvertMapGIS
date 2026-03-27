import datetime
import glob
import os
import re
import struct
from collections import Counter

import geopandas as gpd
import numpy as np
import pandas as pd
import pypinyin
import shapely
from shapely import affinity as shapely_affinity
from pyproj import CRS


# ──────────────────────────────────────────────────────────────────────────────
# 坐标系自动检测：椭球体 → 地理坐标系 EPSG
# ──────────────────────────────────────────────────────────────────────────────
_GEO_EPSG = {
    1:   4214,   # Krasovsky  → Beijing 1954
    16:  4214,   # Krasovsky (alt)
    2:   4610,   # IAG75      → Xian 1980
    7:   4326,   # WGS84
    9:   4984,   # WGS72
    116: 4555,   # Clarke 1880 → New Beijing
}

# ──────────────────────────────────────────────────────────────────────────────
# 高斯-克吕格 EPSG 查找表
# 结构：{椭球体代码: {中央经线整度数: (6度带EPSG或None, 3度带EPSG或None)}}
# 当两种带宽都有对应 EPSG 时，无法从文件自动区分，置为 None 让用户自判断。
# ──────────────────────────────────────────────────────────────────────────────
_GK_EPSG = {
    # Beijing 1954 (ellipsoid=1 or 16)
    # 无官方6度带 EPSG，只有3度带 CM 系列
    1: {
        75:  (None, 2422), 78:  (None, 2423), 81:  (None, 2424),
        84:  (None, 2425), 87:  (None, 2426), 90:  (None, 2427),
        93:  (None, 2428), 96:  (None, 2429), 99:  (None, 2430),
        102: (None, 2431), 105: (None, 2432), 108: (None, 2433),
        111: (None, 2434), 114: (None, 2435), 117: (None, 2436),
        120: (None, 2437), 123: (None, 2438), 126: (None, 2439),
        129: (None, 2440), 132: (None, 2441), 135: (None, 2442),
    },
    16: {  # 与 1 相同，引用同一份数据
        75:  (None, 2422), 78:  (None, 2423), 81:  (None, 2424),
        84:  (None, 2425), 87:  (None, 2426), 90:  (None, 2427),
        93:  (None, 2428), 96:  (None, 2429), 99:  (None, 2430),
        102: (None, 2431), 105: (None, 2432), 108: (None, 2433),
        111: (None, 2434), 114: (None, 2435), 117: (None, 2436),
        120: (None, 2437), 123: (None, 2438), 126: (None, 2439),
        129: (None, 2440), 132: (None, 2441), 135: (None, 2442),
    },
    # Xian 1980 (ellipsoid=2)
    # 6度带 CM 系列：2338-2348；3度带 CM 系列：2370-2390
    # 当 CM % 6 == 0（即75/81/87/…/135）时两者均有，无法自动区分，返回 None。
    # 当 CM % 3 == 0 但 CM % 6 != 0（即78/84/90/…/132）时只有3度带，返回唯一 EPSG。
    2: {
        75:  (2338, None), 78:  (None, 2371), 81:  (2339, None),
        84:  (None, 2373), 87:  (2340, None), 90:  (None, 2375),
        93:  (2341, None), 96:  (None, 2377), 99:  (2342, None),
        102: (None, 2379), 105: (2343, None), 108: (None, 2381),
        111: (2344, None), 114: (None, 2383), 117: (2345, None),
        120: (None, 2385), 123: (2346, None), 126: (None, 2387),
        129: (2347, None), 132: (None, 2389), 135: (2348, None),
    },
    # New Beijing (ellipsoid=116)
    # 6度带 CM 系列：4579-4589；3度带 CM 系列：4782-4822
    116: {
        75:  (4579, None), 78:  (None, 4783), 81:  (4580, None),
        84:  (None, 4785), 87:  (4581, None), 90:  (None, 4787),
        93:  (4582, None), 96:  (None, 4789), 99:  (4583, None),
        102: (None, 4791), 105: (4584, None), 108: (None, 4793),
        111: (4585, None), 114: (None, 4795), 117: (4586, None),
        120: (None, 4797), 123: (4587, None), 126: (None, 4799),
        129: (4588, None), 132: (None, 4801), 135: (4589, None),
    },
}

# CGCS2000：ellipsoid 字段在文件里实际值尚未确认，暂不加入自动检测
# 若确认字段值后，可按相同结构添加：
# 6度带 CM 系列：4502-4512；3度带 CM 系列：4534-4554


class MapGisReader:
    """
    MapGIS 文件读取器，支持点、线、面要素的解析与转换。

    参数
    ----
    filepath          : str   — 文件路径
    scale_factor      : int   — 比例尺（可选）
    source_wkid       : str/int — 手动指定源坐标系 EPSG（None = 由 auto_detect_source_crs 决定）
    target_wkid       : str/int — 目标坐标系 EPSG，指定后在 to_file() 前执行 to_crs()（None = 不重投影）
    auto_detect_source_crs : bool — True 时从文件元数据自动识别源坐标系；
                                     False 时仅用文件内置 CRS（不覆盖）
    wkid              : 兼容旧接口，等效于同时设置 source_wkid（已弃用，优先使用 source_wkid）
    """
    def __init__(self, filepath, scale_factor=None,
                 source_wkid=None, target_wkid=None,
                 auto_detect_source_crs=True,
                 wkid=None, slib_dir=None):
        self.element_count = 0
        # 向后兼容旧 wkid 参数：若调用方仍传 wkid，视为 source_wkid
        if wkid is not None and source_wkid is None:
            source_wkid = wkid
        self.source_wkid = source_wkid
        self.target_wkid = target_wkid
        self.auto_detect_source_crs = auto_detect_source_crs
        # 保留旧属性名以兼容现有调用方（指向 source_wkid）
        self.wkid = source_wkid
        self._user_provided_scale = scale_factor is not None
        self.coordinate_scale = scale_factor if scale_factor is not None else None
        self.filepath = filepath
        # 原始元数据（由 _parse_crs 填充，供 _detect_wkid_from_metadata 使用）
        self._raw_proj_type = None
        self._raw_ellipsoid = None
        self._raw_central_meridian = None
        self._raw_scale_factor = None
        self._raw_bbox = None
        self._metadata_crs_suspect = False
        self._inferred_source_epsg = None
        self._spatial_context_note = ''
        # 自动检测结果（dict，供调用方查询）
        self.crs_detection = None
        # slib 符号库（可选）
        self._slib = None
        self._slib_ok = False
        self._slib_json_data = None  # 每行的完整符号信息，用于 JSON sidecar
        if slib_dir is not None:
            try:
                import slib_parser
                self._slib = slib_parser.SlibDirectory(slib_dir)
                self._slib_ok = self._slib.ok
            except Exception:
                self._slib = None
                self._slib_ok = False
        self.file = open(filepath, 'rb')
        self.shape_type = self._detect_shape_type()
        self._read_headers()
        self._parse_feature_data()
        self._normalise_spatial_context()
        # 源坐标系：优先手动指定，其次自动检测，最后保留文件内置
        if self.source_wkid is not None:
            # 手动指定源坐标系
            try:
                from pyproj import CRS as _CRS
                self.crs = _CRS.from_epsg(self.source_wkid)
            except Exception:
                pass  # 保留 _parse_crs 已赋的 crs
        elif self.auto_detect_source_crs:
            self._apply_auto_detected_crs()
        # 否则保留 _parse_crs 中解析的文件内置 CRS，不做任何覆盖
        self._build_geodataframe()
        # slib 符号信息附加（在 GeoDataFrame 构建后）
        if self._slib is not None and self._slib_ok:
            self._apply_slib()

    def _detect_shape_type(self):
        """检测文件类型并返回要素类型。"""
        type_dict = {'WMAP`D22': 'POINT', 'WMAP`D23': 'POLYGON', 'WMAP`D21': 'LINE'}
        file_type = self.file.read(8).decode('gbk')
        if file_type not in type_dict:
            raise InvalidFileError()
        self.file.read(4)  # 跳过无用字节
        return type_dict[file_type]

    def _read_headers(self):
        """读取数据区头部信息。"""
        data_start = struct.unpack('1i', self.file.read(4))[0]
        self.file.seek(data_start)
        self.headers = [self.file.read(10) for _ in range(10)]

    def _parse_feature_data(self):
        """根据要素类型解析属性和几何数据。"""
        if self.shape_type == 'POINT':
            start, _ = struct.unpack('2i', self.headers[2][:-2])
            self._parse_attributes(start)
            self._parse_points()
        elif self.shape_type == 'LINE':
            start, _ = struct.unpack('2i', self.headers[2][:-2])
            self._parse_attributes(start)
            self._parse_lines()
        elif self.shape_type == 'POLYGON':
            start, _ = struct.unpack('2i', self.headers[9][:-2])
            self._parse_attributes(start)
            self._parse_polygons()

    def _compute_raw_bbox(self):
        """基于未缩放的原始坐标计算包围盒。"""
        if not hasattr(self, 'coords') or self.coords is None:
            return None

        if self.shape_type == 'POINT':
            arr = np.asarray(self.coords, dtype=float)
            if arr.size == 0:
                return None
            return (
                float(arr[:, 0].min()),
                float(arr[:, 1].min()),
                float(arr[:, 0].max()),
                float(arr[:, 1].max()),
            )

        xs = []
        ys = []
        for part in self.coords:
            arr = np.asarray(part, dtype=float)
            if arr.size == 0:
                continue
            arr = arr.reshape(-1, 2)
            xs.extend(arr[:, 0].tolist())
            ys.extend(arr[:, 1].tolist())

        if not xs:
            return None
        return (min(xs), min(ys), max(xs), max(ys))

    def _bbox_looks_geographic(self, bbox):
        """判断原始坐标包围盒是否像经纬度。"""
        if bbox is None:
            return False
        xmin, ymin, xmax, ymax = bbox
        return (-180 <= xmin <= 180 and -180 <= xmax <= 180 and
                -90 <= ymin <= 90 and -90 <= ymax <= 90)

    def _infer_spatial_context_from_siblings(self):
        """从同目录其他图层推断缩放和源坐标系。"""
        folder = os.path.dirname(os.path.abspath(self.filepath))
        sibling_paths = []
        for pattern in ('*.WP', '*.WL', '*.WT', '*.wp', '*.wl', '*.wt'):
            sibling_paths.extend(glob.glob(os.path.join(folder, pattern)))

        scale_counts = Counter()
        epsg_counts = Counter()
        current_path = os.path.abspath(self.filepath)

        for sibling in sibling_paths:
            if os.path.abspath(sibling) == current_path:
                continue

            meta = _read_mapgis_spatial_header(sibling)
            if meta.get('error'):
                continue

            proj_type = meta.get('proj_type')
            raw_scale = meta.get('raw_scale')
            if proj_type in {2, 3, 5} and isinstance(raw_scale, (int, float)) and raw_scale > 0:
                scale_counts[round(raw_scale / 1000.0, 6)] += 1

            det = meta.get('detection') or {}
            epsg = det.get('detected_epsg')
            if epsg and det.get('confidence') == 'high':
                epsg_counts[int(epsg)] += 1

        scale_hint = None
        if scale_counts:
            scale_value, scale_count = scale_counts.most_common(1)[0]
            if scale_count >= 2 or len(scale_counts) == 1:
                scale_hint = float(scale_value)

        epsg_hint = None
        if epsg_counts:
            epsg_value, epsg_count = epsg_counts.most_common(1)[0]
            if epsg_count >= 2 or len(epsg_counts) == 1:
                epsg_hint = int(epsg_value)

        return scale_hint, epsg_hint

    def _rescale_geometries(self, new_scale):
        """按新的缩放倍数重建当前几何的坐标尺度。"""
        if new_scale is None or new_scale <= 0:
            new_scale = 1.0

        old_scale = self.coordinate_scale if self.coordinate_scale not in (None, 0) else 1.0
        if abs(float(new_scale) - float(old_scale)) < 1e-12:
            self.coordinate_scale = float(new_scale)
            return

        ratio = float(new_scale) / float(old_scale)
        self.geom = [
            shapely_affinity.scale(geom, xfact=ratio, yfact=ratio, origin=(0, 0))
            for geom in self.geom
        ]
        self.coordinate_scale = float(new_scale)

    def _normalise_spatial_context(self):
        """修正明显异常的比例尺/坐标系元数据。"""
        self._raw_bbox = self._compute_raw_bbox()

        # proj_type==0 但坐标明显不是经纬度：不要再按地理坐标处理
        if self._raw_proj_type != 0 or self._raw_bbox is None:
            return

        if self._bbox_looks_geographic(self._raw_bbox):
            # 真正经纬度坐标不应再乘比例尺
            self._rescale_geometries(1.0)
            return

        self._metadata_crs_suspect = True
        scale_hint, epsg_hint = self._infer_spatial_context_from_siblings()
        self._inferred_source_epsg = epsg_hint

        if (not self._user_provided_scale and
                (self._raw_scale_factor is None or self._raw_scale_factor <= 0) and
                scale_hint is not None):
            self._rescale_geometries(scale_hint)
            self._spatial_context_note = (
                '文件头标注为地理坐标，但原始坐标超出经纬度范围；'
                f'已按同目录图层推断比例尺 {scale_hint:g}'
            )
        else:
            self._rescale_geometries(self.coordinate_scale if self.coordinate_scale else 1.0)
            self._spatial_context_note = (
                '文件头标注为地理坐标，但原始坐标超出经纬度范围；'
                '已保留原始图幅坐标输出'
            )

        self.crs = None
        if epsg_hint is not None:
            try:
                self.crs = CRS.from_epsg(epsg_hint)
                self._spatial_context_note += f'，并按同目录多数图层推断 EPSG:{epsg_hint}'
            except Exception:
                self.crs = None

    def _parse_attributes(self, start):
        """解析属性表。"""
        self.file.seek(start)
        self.file.read(2)
        self.file.read(4)  # 创建日期
        self.file.read(6)
        attr_offset = struct.unpack('1i', self.file.read(4))[0]
        self.file.read(4)
        self.file.read(4)
        self.file.read(128)  # 工作目录路径
        self.file.read(128)
        self.file.read(40)
        self.file.read(2)
        field_count = struct.unpack('1h', self.file.read(2))[0]
        record_count = struct.unpack('1i', self.file.read(4))[0]
        record_length = struct.unpack('1h', self.file.read(2))[0]
        self.file.read(18)
        field_names, field_types, field_offsets, field_lengths = [], [], [], []
        for _ in range(field_count):
            raw = self.file.read(20)
            try:
                name = raw.decode('gbk').strip('\x00')
            except UnicodeDecodeError as err:
                name = raw[:int(re.search(r'in position (\d+)', str(err)).group(1))].decode('gbk')
            field_names.append(name)
            field_types.append(ord(self.file.read(1)))
            field_offsets.append(struct.unpack('1i', self.file.read(4))[0])
            self.file.read(2)
            field_lengths.append(struct.unpack('1h', self.file.read(2))[0])
            self.file.read(1)
            self.file.read(1)
            self.file.read(2)
            self.file.read(2)  # 跳过记录数
            self.file.read(4)
        # 过滤有效字段
        valid_types = {0, 1, 2, 3, 4, 5, 6, 7}
        mask = [t in valid_types for t in field_types]
        field_names = np.array(field_names)[mask]
        type_dict = {0: 'string', 1: 'byte', 2: 'short', 3: 'int', 4: 'float', 5: 'double', 6: 'date', 7: 'time'}
        field_types = np.array(field_types)[mask]
        field_offsets = np.array(field_offsets)[mask]
        field_lengths = np.array(field_lengths)[mask]
        # 计算每个字段的实际长度
        offsets = list(field_offsets) + [record_length]
        field_actual_lengths = [offsets[i+1] - offsets[i] for i in range(len(field_offsets))]
        self.fields = list(zip(field_names, [type_dict[t] for t in field_types], field_actual_lengths))
        self.file.read(record_length)
        # 优化：一次性读取所有属性数据，减少 file.read 调用次数
        all_attr_bytes = self.file.read(record_length * (record_count - 1))
        data = []
        for i in range(record_count - 1):
            row = all_attr_bytes[i * record_length: (i + 1) * record_length]
            attr = []
            for j in range(len(field_offsets)):
                start_idx = field_offsets[j]
                end_idx = field_offsets[j+1] if j < len(field_offsets) - 1 else None
                value = row[start_idx:end_idx]
                t = field_types[j]
                if t == 4:
                    attr.append(struct.unpack('1f', value)[0])
                elif t == 3:
                    attr.append(struct.unpack('1i', value)[0])
                elif t == 2:
                    attr.append(struct.unpack('1h', value)[0])
                elif t == 1:
                    attr.append(ord(value))
                elif t == 5:
                    attr.append(struct.unpack('1d', value)[0])
                elif t == 6:
                    temp = value
                    attr.append(datetime.date(struct.unpack('1h', temp[:2])[0], temp[2], temp[3]))
                elif t == 7:
                    temp = value
                    attr.append(datetime.time(temp[0], temp[1], *self._parse_time_fraction(struct.unpack('1d', temp[2:])[0])))
                elif t == 0:
                    try:
                        attr.append(value.decode('gbk').strip('\x00'))
                    except UnicodeDecodeError as err:
                        m = re.search(r'in position (\\d+)', str(err))
                        if m:
                            attr.append(value[:int(m.group(1))].decode('gbk'))
                        else:
                            attr.append(value.decode('gbk', errors='replace').strip('\x00'))
            data.append(attr)
        self.data = pd.DataFrame(data, columns=field_names)
        # 合并更多信息
        more_info = self._parse_more_info()
        self.data = pd.concat([self.data, more_info], axis=1)
        # 字段名去重
        self.data.columns = self._deduplicate_columns(self.data.columns)
        self.res_head = self.data.columns.tolist()

    def _parse_time_fraction(self, value):
        """解析小数部分为微秒。"""
        int_part = int(np.floor(value))
        micro = int(1000000 * (value - np.floor(value)))
        return int_part, micro

    def _deduplicate_columns(self, columns):
        """字段名去重。"""
        seen = set()
        result = []
        for col in columns:
            if col not in seen:
                result.append(col)
                seen.add(col)
            else:
                idx = 1
                new_col = f"{col}-{idx}"
                while new_col in seen:
                    idx += 1
                    new_col = f"{col}-{idx}"
                result.append(new_col)
                seen.add(new_col)
        return result

    def _parse_more_info(self):
        """根据类型解析更多要素信息。"""
        self.file.seek(0)
        type_dict = {'WMAP`D22': 'POINT', 'WMAP`D23': 'POLYGON', 'WMAP`D21': 'LINE'}
        file_type = self.file.read(8).decode('gbk')
        if file_type not in type_dict:
            raise InvalidFileError()
        self.file.read(4)
        data_start = struct.unpack('1i', self.file.read(4))[0]
        self.file.seek(data_start)
        headers = [self.file.read(10) for _ in range(10)]
        if file_type == 'WMAP`D22':
            return self._parse_point_info(headers)
        elif file_type == 'WMAP`D21':
            return self._parse_line_info(headers)
        elif file_type == 'WMAP`D23':
            return self._parse_polygon_info(headers)

    def _parse_point_info(self, headers):
        """解析点要素详细信息。"""
        start, vol = struct.unpack('2i', headers[0][:-2])
        columns = ["ID", '坐标X', "坐标Y", "点类型", "透明输出", "颜色", "字符串", "字符高度", "字符宽度", "字符间隔", "字符串角度", "中文字体", "西文字体", "字形", "排列", "子图号", "子图高", "子图宽", "子图角度", "子图线宽", "子图辅色", "圆半径", "圆轮廓颜色", "圆笔宽", "圆填充", "弧半径", "弧起始角度", "弧终止角度", "弧笔宽"]
        df = pd.DataFrame(columns=columns)
        for i in range(int(vol / 93) - 1):
            df.loc[i, 'ID'] = i
            self.file.seek(start + 93 * (i + 1))
            self.file.read(1)
            str_count = struct.unpack('1h', self.file.read(2))[0]
            char_offset = struct.unpack('1i', self.file.read(4))[0]
            x, y = struct.unpack('2d', self.file.read(16))
            df.loc[i, '坐标X'] = x
            df.loc[i, '坐标Y'] = y
            self.file.read(8)
            point_type = ord(self.file.read(1))
            transparent = ord(self.file.read(1))
            df.loc[i, '透明输出'] = "透明" if transparent else "不透明"
            if point_type == 0:
                df.loc[i, '点类型'] = "字符串"
                df.loc[i, '字符高度'] = round(struct.unpack('1f', self.file.read(4))[0], 8)
                df.loc[i, '字符宽度'] = round(struct.unpack('1f', self.file.read(4))[0], 8)
                df.loc[i, '字符间隔'] = round(struct.unpack('1f', self.file.read(4))[0], 8)
                df.loc[i, '字符串角度'] = round(struct.unpack('1f', self.file.read(4))[0], 8)
                df.loc[i, '中文字体'] = struct.unpack('1h', self.file.read(2))[0]
                df.loc[i, '西文字体'] = struct.unpack('1h', self.file.read(2))[0]
                df.loc[i, '字形'] = ord(self.file.read(1))
                df.loc[i, '排列'] = ord(self.file.read(1))
                char_start, _ = struct.unpack('2i', headers[1][:-2])
                self.file.seek(char_start + char_offset)
                char_text = self.file.read(str_count)
                df.loc[i, '字符串'] = char_text.decode('gb18030')
            elif point_type == 1:
                df.loc[i, '点类型'] = "子图"
                df.loc[i, '子图号'] = struct.unpack('1i', self.file.read(4))[0]
                df.loc[i, '子图高'] = struct.unpack('1f', self.file.read(4))[0]
                df.loc[i, '子图宽'] = struct.unpack('1f', self.file.read(4))[0]
                df.loc[i, '子图角度'] = round(struct.unpack('1f', self.file.read(4))[0], 4)
                df.loc[i, '子图线宽'] = round(struct.unpack('1f', self.file.read(4))[0], 8)
                df.loc[i, '子图辅色'] = struct.unpack('1f', self.file.read(4))[0]
            elif point_type == 2:
                df.loc[i, '点类型'] = "圆"
                df.loc[i, '圆半径'] = round(struct.unpack('1d', self.file.read(8))[0], 8)
                df.loc[i, '圆轮廓颜色'] = struct.unpack('1i', self.file.read(4))[0]
                df.loc[i, '圆笔宽'] = struct.unpack('1f', self.file.read(4))[0]
                fill = ord(self.file.read(1))
                df.loc[i, '圆填充'] = "填充圆" if fill else "空心圆"
            elif point_type == 3:
                df.loc[i, '点类型'] = "弧"
                df.loc[i, '弧半径'] = round(struct.unpack('1d', self.file.read(8))[0], 8)
                df.loc[i, '弧起始角度'] = round(struct.unpack('1f', self.file.read(4))[0], 8)
                df.loc[i, '弧终止角度'] = round(struct.unpack('1f', self.file.read(4))[0], 8)
                df.loc[i, '弧笔宽'] = round(struct.unpack('1f', self.file.read(4))[0], 8)
            self.file.seek(start + 93 * (i + 1) + 73)
            self.file.read(2)
            color = struct.unpack('1i', self.file.read(4))[0]
            df.loc[i, '颜色'] = color
        df.dropna(how='all', axis=1, inplace=True)
        return df

    def _parse_line_info(self, headers):
        """解析线要素详细信息（优化版）。"""
        start, vol = struct.unpack('2i', headers[0][:-2])
        n = int(vol / 57) - 1
        columns = ["ID", "线型号", "辅助线号", "覆盖方式", "线颜色", "线宽", "线种类", "X系数", "Y系数", "辅助色", "图层", "锚点数目", "锚点坐标存储位置"]
        # 一次性读取所有要素的二进制数据
        self.file.seek(start + 57)  # 跳过第一个
        all_bytes = self.file.read(57 * n)
        rows = []
        for i in range(n):
            offset = i * 57
            chunk = all_bytes[offset:offset+57]
            row = {
                "ID": i,
                "锚点数目": struct.unpack('1i', chunk[10:14])[0],
                "锚点坐标存储位置": struct.unpack('1i', chunk[14:18])[0],
                # bytes 20-21: 线型号 (short); byte 22: 辅助线号; byte 23: 覆盖方式
                "线型号": struct.unpack('<h', chunk[20:22])[0],
                "辅助线号": chunk[22],
                "覆盖方式": chunk[23],
                "线颜色": struct.unpack('<i', chunk[24:28])[0],
                # bytes 28-29: 2 unknown bytes (padding/flags), skip
                "线宽": struct.unpack('<f', chunk[30:34])[0],
                "线种类": chunk[34],
                "X系数": struct.unpack('<f', chunk[35:39])[0],
                "Y系数": struct.unpack('<f', chunk[39:43])[0],
                "辅助色": struct.unpack('<i', chunk[43:47])[0],
                "图层": struct.unpack('<i', chunk[47:51])[0],
            }
            rows.append(row)
            self.element_count += 1
        df = pd.DataFrame(rows, columns=columns)
        return df

    def _parse_polygon_info(self, headers):
        """解析面要素详细信息。"""
        columns = ["ID", "填充颜色", "填充符号", "图案高度", "图案宽度", "图案颜色"]
        start, vol = struct.unpack('2i', headers[8][:-2])
        df = pd.DataFrame(columns=columns)
        for i in range(int(vol / 40) - 1):
            self.file.seek(start + 40 * (i + 1))
            df.loc[i, 'ID'] = i
            self.file.read(1)
            self.file.read(4)
            self.file.read(4)
            df.loc[i, '填充颜色'] = struct.unpack('1i', self.file.read(4))[0]
            df.loc[i, '填充符号'] = struct.unpack('1h', self.file.read(2))[0]
            df.loc[i, '图案高度'] = struct.unpack('1f', self.file.read(4))[0]
            df.loc[i, '图案宽度'] = struct.unpack('1f', self.file.read(4))[0]
            self.file.read(2)
            df.loc[i, '图案颜色'] = struct.unpack('1i', self.file.read(4))[0]
            self.element_count += 1
        return df

    def _parse_points(self):
        """解析点要素几何。"""
        self._parse_crs()
        start, vol = struct.unpack('2i', self.headers[0][:-2])
        self.file.seek(start)
        self.file.read(93)
        coords = []
        for _ in range(int(vol / 93) - 1):
            self.file.read(1)
            self.file.read(2)
            self.file.read(4)
            x, y = struct.unpack('2d', self.file.read(16))
            coords.append((x, y))
            self.file.read(70)
        self.coords = np.array(coords)
        scale = self.coordinate_scale if self.coordinate_scale is not None else 1
        self.geom = [shapely.geometry.Point(np.array(xy) * scale) for xy in self.coords]

    def _parse_lines(self):
        """解析线要素几何。"""
        self._parse_crs()
        start, vol = struct.unpack('2i', self.headers[0][:-2])
        self.file.seek(start)
        k = vol // 57
        self.file.read(57)
        points, points_offset = [], []
        for _ in range(k - 1):
            self.file.read(10)
            points.append(struct.unpack('1i', self.file.read(4))[0])
            points_offset.append(struct.unpack('1i', self.file.read(4))[0])
            self.file.read(39)
        start, _ = struct.unpack('2i', self.headers[1][:-2])
        self.coords = []
        for i in range(k - 1):
            self.file.seek(start + points_offset[i])
            self.coords.append(struct.unpack(f'{points[i]*2}d', self.file.read(points[i]*16)))
        scale = self.coordinate_scale if self.coordinate_scale is not None else 1
        self.geom = [shapely.geometry.LineString(np.array(i).reshape(-1, 2) * scale) for i in self.coords]

    def _parse_polygons(self):
        """解析面要素几何。"""
        try:
            self._parse_crs()
            start, vol = struct.unpack('2i', self.headers[0][:-2])
            self.file.seek(start)
            k = vol // 57
            self.file.read(57)
            points, points_offset = [], []
            for _ in range(k - 1):
                self.file.read(10)
                points.append(struct.unpack('1i', self.file.read(4))[0])
                points_offset.append(struct.unpack('1i', self.file.read(4))[0])
                self.file.read(39)
            start, _ = struct.unpack('2i', self.headers[1][:-2])
            self.coords = []
            for i in range(k - 1):
                self.file.seek(start + points_offset[i])
                self.coords.append(struct.unpack(f'{points[i]*2}d', self.file.read(points[i]*16)))
            scale = self.coordinate_scale if self.coordinate_scale is not None else 1
            geom_lines = [shapely.geometry.LineString(np.array(i).reshape(-1, 2) * scale) for i in self.coords]
            start, vol = struct.unpack('2i', self.headers[3][:-2])
            self.file.seek(start)
            self.file.read(24)
            temp = []
            for _ in range(int(vol / 24 - 1)):
                temp.append(struct.unpack('4i', self.file.read(16)))
                self.file.read(8)
            temp = np.array(temp)
            temp = np.hstack((temp, np.arange(temp.shape[0]).reshape((-1, 1))))
            self.geom = []
            for i in set(temp[:, 2:4].flatten()) - {0}:
                mask = (temp[:, 2] == i) | (temp[:, 3] == i)
                x = temp[mask]
                mask_ = x[:, 2] == i
                kk = x[mask_]
                t = kk[:, 0].copy()
                kk[:, 0] = kk[:, 1]
                kk[:, 1] = t
                x[mask_] = kk
                if x.shape[0] == 1:
                    poly = list(geom_lines[x[0][-1]].coords)
                    self.geom.append(shapely.geometry.Polygon(poly))
                else:
                    m = [list(geom_lines[ii[-1]].coords) for ii in x]
                    lines = []
                    while m:
                        ring = m.pop(0)
                        changed = True
                        while changed and m:
                            changed = False
                            for idx, seg in enumerate(m):
                                if np.allclose(ring[-1], seg[0]):
                                    ring.extend(seg[1:])
                                    m.pop(idx)
                                    changed = True
                                    break
                                elif np.allclose(ring[-1], seg[-1]):
                                    ring.extend(seg[-2::-1])
                                    m.pop(idx)
                                    changed = True
                                    break
                                elif np.allclose(ring[0], seg[-1]):
                                    ring = seg[:-1] + ring
                                    m.pop(idx)
                                    changed = True
                                    break
                                elif np.allclose(ring[0], seg[0]):
                                    ring = seg[::-1][:-1] + ring
                                    m.pop(idx)
                                    changed = True
                                    break
                        lines.append(ring)
                    lines = [i for i in lines if len(i) > 2]
                    self.geom.append(shapely.geometry.MultiPolygon(get_multipolygons(lines)))
        except struct.error as e:
            if 'unpack requires a buffer of' in str(e):
                raise Exception("原mapgis文件异常，无法转换，请检查该文件在mapgis中是否能正常保存") from e
            else:
                raise

    def _parse_crs(self):
        """解析坐标系信息。

        MapGIS 内部坐标以毫米存储，投影坐标系（proj_type 2/3/5）需除以1000换算为米；
        地理坐标系（proj_type 0）以度存储，无需换算。
        换算统一在读取 proj_type 之后进行，与 wkid 是否指定无关，
        避免原来将换算逻辑散落在各分支中导致 proj_type==5 漏除、
        指定 wkid 时绕过换算等问题。
        """
        self.file.seek(109)
        proj_type = ord(self.file.read(1))
        ellipsoid = ord(self.file.read(1))
        # 保存原始元数据供自动检测使用
        self._raw_proj_type = proj_type
        self._raw_ellipsoid = ellipsoid
        self.file.seek(143)

        # 读取比例尺：优先使用调用方传入的 scale_factor，否则从文件读取
        user_provided_scale = self.coordinate_scale is not None
        if user_provided_scale:
            self.file.read(8)  # 跳过文件中的比例尺字段
            raw_scale = float(self.coordinate_scale)
        else:
            raw_scale = struct.unpack('1d', self.file.read(8))[0]
        self._raw_scale_factor = raw_scale
        raw_scale_invalid = raw_scale is None or raw_scale <= 0
        self.coordinate_scale = 1 if raw_scale_invalid else raw_scale

        ellip_dict = {
            1: '+ellps=krass +towgs84=15.8,-154.4,-82.3,0,0,0,0 +units=m +no_d',
            2: '+a=6378140 +b=6356755.288157528',
            7: '+datum=WGS84',
            9: '+ellps=WGS72',
            10: '+ellps=aust_SA +towgs84=-117.808,-51.536,137.784,0.303,0.446,0.234,-0.29',
            11: '+ellps=aust_SA +towgs84=-134,-48,149,0,0,0,0',
            16: '+ellps=krass',
            116: '+ellps=clrk80 +towgs84=-166,-15,204,0,0,0,0',
            'cgcs2000': '+ellps=GRS80',
        }

        # 投影坐标系（高斯-克吕格等），坐标单位为毫米，需除以1000换算为米
        # 地理坐标系（proj_type==0）以度存储，无需换算
        # 此换算与 wkid 是否指定无关，只由源文件的 proj_type 决定
        PROJECTED_TYPES = {2, 3, 5}
        if proj_type in PROJECTED_TYPES and not raw_scale_invalid:
            self.coordinate_scale = self.coordinate_scale / 1000

        # 椭球体未知或比例尺原本为0（已被上面兜底为1）的异常情况
        ellipsoid_unknown = ellipsoid not in ellip_dict
        if ellipsoid_unknown:
            if ellipsoid == 0 and (self.source_wkid is None or str(self.source_wkid) == '0'):
                # 椭球体类型为0且未手动指定源坐标系，crs置空，主程序日志会有详细提示
                self.crs = ''
                return
            # 椭球体未知但手动指定了源坐标系，__init__ 会后续覆盖，此处只清空 crs
            self.crs = ''

        # 仅在未手动指定源坐标系时，依据文件中的 proj_type 解析文件内置 CRS
        if self.source_wkid is None and not ellipsoid_unknown:
            if proj_type == 5:
                # 高斯-克吕格投影
                self.file.seek(151)
                cl = struct.unpack('1d', self.file.read(8))[0]
                cl = int(str(cl).split('.')[0][:-4]) + int(str(cl).split('.')[0][-4:-2]) / 60.0 + int(str(cl).split('.')[0][-2:]) / 60.0 / 60
                self._raw_central_meridian = cl  # 保存供自动检测使用
                self.crs = CRS('+proj=tmerc' + f' +lat_0=0 +lon_0={cl} +k=1 +x_0=500000 +y_0=0 ' + ellip_dict[ellipsoid] + ' +units=m +no_defs')
            elif proj_type == 0:
                # 地理坐标系
                self.crs = CRS('+proj=longlat ' + ellip_dict[ellipsoid] + ' +no_defs')
            elif proj_type in (2, 3):
                # 其他投影（Lambert 等），解析中央经线，CRS 置空由调用方处理
                self.file.seek(151)
                cl = struct.unpack('1d', self.file.read(8))[0]
                cl = int(str(cl).split('.')[0][:-4]) + int(str(cl).split('.')[0][-4:-2]) / 60.0 + int(str(cl).split('.')[0][-2:]) / 60.0 / 60
                self.file.seek(175)
                self.crs = None
        # 注意：source_wkid 指定时 CRS 覆盖由 __init__ 统一处理，_parse_crs 不再重复

    def _detect_wkid_from_metadata(self):
        """根据文件元数据推断 EPSG 代码。

        返回 dict，结构如下：
          detected_epsg  : int 或 None  —— 唯一识别到的 EPSG；None 表示无法确定
          confidence     : 'high' | 'medium' | 'low'
          datum          : str  —— 基准面名称
          proj_desc      : str  —— 投影类型描述
          central_meridian: float 或 None
          note           : str  —— 歧义或无法识别时的说明
        """
        pt  = self._raw_proj_type
        ell = self._raw_ellipsoid
        cm  = self._raw_central_meridian

        # 元数据未填充时（理论上不应发生），返回低置信空结果
        if pt is None or ell is None:
            return {
                'detected_epsg': None, 'confidence': 'low',
                'datum': '未知', 'proj_desc': '未知',
                'central_meridian': None,
                'note': '元数据未解析，无法自动检测',
            }

        _DATUM_NAME = {
            1:   'Beijing_1954',
            16:  'Beijing_1954',
            2:   'Xian_1980',
            7:   'WGS84',
            9:   'WGS72',
            116: 'New_Beijing',
        }
        _PROJ_DESC = {
            0: '地理坐标系',
            2: 'Lambert等其他投影',
            3: 'Lambert等其他投影',
            5: '高斯-克吕格',
        }

        result = {
            'detected_epsg':    None,
            'confidence':       'low',
            'datum':            _DATUM_NAME.get(ell, f'未知椭球体({ell})'),
            'proj_desc':        _PROJ_DESC.get(pt, f'未知投影类型({pt})'),
            'central_meridian': cm,
            'note':             '',
        }

        # ── 地理坐标系 ──────────────────────────────────────────────────────
        if pt == 0:
            epsg = _GEO_EPSG.get(ell)
            if epsg:
                result['detected_epsg'] = epsg
                result['confidence']    = 'high'
            else:
                result['note'] = '椭球体类型不在已知列表，无法自动匹配地理坐标系 EPSG'
            return result

        # ── 高斯-克吕格 ─────────────────────────────────────────────────────
        if pt == 5:
            if cm is None:
                result['note'] = '未能读取中央经线，无法自动匹配'
                return result
            if ell not in _GK_EPSG:
                result['note'] = f'椭球体 {ell} 暂未收录高斯-克吕格 EPSG 映射表'
                return result

            cm_int = int(round(cm))
            row = _GK_EPSG[ell].get(cm_int)
            if row is None:
                result['note'] = f'中央经线 {cm_int}° 不在映射表中'
                return result

            epsg_6, epsg_3 = row
            if epsg_6 is not None and epsg_3 is None:
                # 只有6度带
                result['detected_epsg'] = epsg_6
                result['confidence']    = 'high'
            elif epsg_3 is not None and epsg_6 is None:
                # 只有3度带，唯一匹配
                result['detected_epsg'] = epsg_3
                result['confidence']    = 'high'
            else:
                # 6度带与3度带均存在（CM 为6的倍数），无法自动区分
                result['note'] = (
                    f'中央经线 {cm_int}° 同时匹配 6度带(EPSG:{epsg_6}) '
                    f'和 3度带(EPSG:{epsg_3})，'
                    f'请通过"指定坐标系"手动选择'
                )
            return result

        # ── Lambert 及其他投影 ──────────────────────────────────────────────
        if pt in (2, 3):
            result['note'] = '文件元数据缺少标准纬线等参数，无法自动识别具体 EPSG，请手动指定'
            return result

        result['note'] = f'未知 proj_type={pt}，无法自动识别'
        return result

    def _apply_auto_detected_crs(self):
        """调用检测逻辑，若置信度为 high 则将 CRS 升级为对应 EPSG 标准 CRS。

        结果写入 self.crs_detection 供调用方（转换线程）读取日志。
        """
        detection = self._detect_wkid_from_metadata()

        if self._metadata_crs_suspect:
            detection = {
                'detected_epsg': self._inferred_source_epsg,
                'confidence': 'high' if self._inferred_source_epsg is not None else 'low',
                'datum': detection.get('datum', '未知'),
                'proj_desc': '图幅平面坐标（由坐标范围判定）',
                'central_meridian': detection.get('central_meridian'),
                'note': self._spatial_context_note,
            }

        self.crs_detection = detection

        if detection['detected_epsg'] and detection['confidence'] == 'high':
            try:
                self.crs = CRS.from_epsg(detection['detected_epsg'])
            except Exception:
                # EPSG 无效时不覆盖，保留原有 CRS
                detection['note'] += ' (EPSG 加载失败，保留文件内置 CRS)'

    def _build_geodataframe(self):
        """构建 GeoDataFrame。"""
        # 标记是否进行了数据修复
        self._data_repaired = False

        try:
            # 标准流程：直接构建GeoDataFrame
            self.geodataframe = gpd.GeoDataFrame(self.data, geometry=self.geom)
            if self.crs:
                self.geodataframe.crs = self.crs
        except ValueError as e:
            # 只有在出现"Length of values"错误时才进行修复
            if "Length of values" in str(e):
                print(f"检测到数据长度不匹配，进行智能修复...")
                print(f"  属性表记录数: {len(self.data)}")
                print(f"  几何对象数: {len(self.geom)}")
                print(f"  差异: {abs(len(self.data) - len(self.geom))}")

                # 保守的修复策略：取较小的长度
                min_length = min(len(self.data), len(self.geom))
                print(f"  修复策略: 取前{min_length}个有效数据")

                self.data = self.data.iloc[:min_length]
                self.geom = self.geom[:min_length]
                self._data_repaired = True

                # 重新构建GeoDataFrame
                self.geodataframe = gpd.GeoDataFrame(self.data, geometry=self.geom)
                if self.crs:
                    self.geodataframe.crs = self.crs

                print(f"  修复完成 - 属性表: {len(self.data)}, 几何数据: {len(self.geom)}")
            else:
                # 其他ValueError直接抛出
                raise

        # 目标坐标系重投影：若指定了 target_wkid 且源 CRS 已知，则执行 to_crs()
        if self.target_wkid is not None and self.geodataframe.crs is not None:
            try:
                self.geodataframe = self.geodataframe.to_crs(epsg=int(self.target_wkid))
                # 更新 crs 属性以保持一致
                self.crs = self.geodataframe.crs
            except Exception as e:
                # 重投影失败，标记错误供调用方日志记录，不中断流程
                self._reprojection_error = str(e)

        # ── 连接键：layer_key + feat_id ──────────────────────────────────
        # layer_key: 由调用方（export_manager）通过 set_layer_key() 注入；
        #            此处先用空字符串占位，保证字段始终存在。
        # feat_id:   每图层内的顺序整数，可作为稳定连接键。
        n = len(self.geodataframe)
        if 'layer_key' not in self.geodataframe.columns:
            self.geodataframe['layer_key'] = ''
        if 'feat_id' not in self.geodataframe.columns:
            import numpy as np
            self.geodataframe['feat_id'] = np.arange(n, dtype=np.int32)

    def _apply_slib(self):
        """为 GeoDataFrame 附加 slib 符号库字段，并准备 JSON sidecar 数据。

        Shapefile 附加字段（≤10 字符）：
          sl_lib    : str  — 'subgraph' / 'linesty' / 'fillgrph'
          sl_id     : int  — 符号编号（点/面用）
          sl_type   : int  — 线型号（线要素用）
          sl_aux    : int  — 辅助线号（线要素用）
          sl_cov    : int  — 覆盖方式（线要素用）
          sl_parts  : int  — 图元段数（part_count 或 prim_count）
          sl_ok     : int  — 1=查找成功, 0=失败
        """
        import json as _json

        gdf = self.geodataframe
        n = len(gdf)

        # 初始化 slib 字段列（显式 int32，避免 pandas 推断为 int64 导致 ArcMap 不兼容）
        import numpy as np
        sl_lib   = [''] * n
        sl_id    = np.zeros(n, dtype=np.int32)
        sl_type  = np.zeros(n, dtype=np.int32)
        sl_aux   = np.zeros(n, dtype=np.int32)
        sl_cov   = np.zeros(n, dtype=np.int32)
        sl_parts = np.zeros(n, dtype=np.int32)
        sl_ok    = np.zeros(n, dtype=np.int32)

        slib_json_rows = []  # 每行完整符号信息（for JSON sidecar）

        if self.shape_type == 'POINT':
            # 点要素：用「子图号」字段（映射前的原始列名）查 Subgraph.lib
            # 获取子图号列（可能已被重命名为 SubNo 或仍为 子图号）
            sym_col = None
            for cname in ['子图号', 'SubNo']:
                if cname in gdf.columns:
                    sym_col = cname
                    break

            for i in range(n):
                sym_id = 0
                if sym_col is not None:
                    try:
                        sym_id = int(gdf.iloc[i][sym_col])
                    except (ValueError, TypeError):
                        sym_id = 0
                rec = self._slib.lookup_point(sym_id)
                ok  = 1 if rec.get('ok') else 0
                sl_lib[i]   = rec.get('sl_lib', 'subgraph')
                sl_id[i]    = sym_id
                sl_parts[i] = rec.get('part_count', 0)
                sl_ok[i]    = ok
                slib_json_rows.append(rec)

        elif self.shape_type == 'LINE':
            # 线要素：用「线型号 / 辅助线号 / 覆盖方式」查 LINESTY.lib
            for i in range(n):
                row = gdf.iloc[i]
                lt  = int(row.get('线型号', row.get('LineType', 0)) or 0)
                aux = int(row.get('辅助线号', 0) or 0)
                cov = int(row.get('覆盖方式', 0) or 0)
                rec = self._slib.lookup_line(lt, aux, cov)
                ok  = 1 if rec.get('ok') else 0
                sl_lib[i]   = rec.get('sl_lib', 'linesty')
                sl_type[i]  = lt
                sl_aux[i]   = aux
                sl_cov[i]   = cov
                sl_parts[i] = rec.get('prim_count', 0)
                sl_ok[i]    = ok
                slib_json_rows.append(rec)

        elif self.shape_type == 'POLYGON':
            # 面要素：用「填充符号」字段查 Fillgrph.lib
            fill_col = None
            for cname in ['填充符号', 'FillSymbol']:
                if cname in gdf.columns:
                    fill_col = cname
                    break

            for i in range(n):
                fill_id = 0
                if fill_col is not None:
                    try:
                        fill_id = int(gdf.iloc[i][fill_col])
                    except (ValueError, TypeError):
                        fill_id = 0
                rec = self._slib.lookup_fill(fill_id)
                ok  = 1 if rec.get('ok') else 0
                sl_lib[i]   = rec.get('sl_lib', 'fillgrph')
                sl_id[i]    = fill_id
                sl_parts[i] = rec.get('part_count', 0)
                sl_ok[i]    = ok
                slib_json_rows.append(rec)

        # 写入 GeoDataFrame
        gdf['sl_lib']   = sl_lib
        gdf['sl_id']    = sl_id
        gdf['sl_type']  = sl_type
        gdf['sl_aux']   = sl_aux
        gdf['sl_cov']   = sl_cov
        gdf['sl_parts'] = sl_parts
        gdf['sl_ok']    = sl_ok
        self.geodataframe = gdf

        # 保存 JSON sidecar 数据（to_file 时写出）
        self._slib_json_data = slib_json_rows

    def set_layer_key(self, key: str):
        """由外部（export_manager）注入图层唯一键，写入 geodataframe['layer_key']。"""
        if self.geodataframe is not None and 'layer_key' in self.geodataframe.columns:
            self.geodataframe['layer_key'] = key

    def to_file(self, filepath, **kwargs):
        """保存为文件。"""
        # 通用数值字段异常处理函数，阈值严格按shp字段宽度限制（1e12）
        def fix_large_values(df, column_name, threshold=1e12):
            """修复数值字段中的异常大值，保证shp字段宽度安全"""
            if column_name in df.columns:
                col = df[column_name]
                if col.dtype in ['float64', 'float32', 'int64', 'int32']:
                    large_values = (col.abs() > threshold) | (col.isnull())
                    if large_values.any():
                        print(f"检测到{large_values.sum()}个{column_name}值超出shp字段宽度限制，已自动修正")
                        df.loc[large_values, column_name] = 0.0
        # 处理所有数值字段，阈值为1e12
        numeric_columns = self.geodataframe.select_dtypes(include=['float64', 'float32', 'int64', 'int32']).columns
        for col in numeric_columns:
            fix_large_values(self.geodataframe, col, threshold=1e12)
        # 对线宽字段做额外的合理值校验（应为正小数，单位mm；异常值置0）
        for linewid_col in ['线宽', 'LineWid']:
            if linewid_col in self.geodataframe.columns:
                col = self.geodataframe[linewid_col]
                bad = (col < 0) | (col > 10000) | col.isnull()
                if bad.any():
                    print(f"检测到{bad.sum()}个{linewid_col}值异常（负值/超大值），已自动置0")
                    self.geodataframe.loc[bad, linewid_col] = 0.0
        # 处理字段名（转换为英文，避免pyogrio警告）
        if filepath.split('.')[-1] == 'shp':
            self.geodataframe = self._sanitize_field_names(self.geodataframe)
        # 保存文件
        self.geodataframe.to_file(filepath, **kwargs)
        # 写出 slib JSON sidecar
        if self._slib_json_data is not None:
            import json as _json
            json_path = os.path.splitext(filepath)[0] + '.slib.json'
            slib_stats = self._slib.stats() if self._slib is not None else {}
            sidecar = {
                'source_file': os.path.basename(self.filepath),
                'shape_type': self.shape_type,
                'feature_count': len(self._slib_json_data),
                'slib_stats': slib_stats,
                'symbols': self._slib_json_data,
            }
            with open(json_path, 'w', encoding='utf-8') as f:
                _json.dump(sidecar, f, ensure_ascii=False, indent=2)
    
    def _sanitize_field_names(self, df):
        """处理字段名，将中文转换为英文。"""
        # 英文字段名映射
        field_map = {
            'ID': 'ID',
            '面积': 'Area',
            '周长': 'Perimeter',
            'GB': 'GB',
            'Shape_Leng': 'Shape_Leng',
            'Shape_Area': 'Shape_Area',
            'ID-1': 'ID_1',
            '填充颜色': 'FillColor',
            '填充符号': 'FillSymbol',
            '图案高度': 'PatternH',
            '图案宽度': 'PatternW',
            '图案颜色': 'PatternC',
            '坐标X': 'CoordX',
            '坐标Y': 'CoordY',
            '点类型': 'PntType',
            '透明输出': 'TransOut',
            '颜色': 'Color',
            '字符串': 'StrText',
            '字符高度': 'CharH',
            '字符宽度': 'CharW',
            '字符间隔': 'CharSpc',
            '字符串角度': 'StrAng',
            '中文字体': 'FontCN',
            '西文字体': 'FontEN',
            '字形': 'FontSty',
            '排列': 'Arrange',
            '子图号': 'SubNo',
            '子图高': 'SubH',
            '子图宽': 'SubW',
            '子图角度': 'SubAng',
            '子图线宽': 'SubLW',
            '子图辅色': 'SubCol2',
            '圆半径': 'CRadius',
            '圆轮廓颜色': 'CCLR',
            '圆笔宽': 'CPenW',
            '圆填充': 'CFill',
            '弧半径': 'ARadius',
            '弧起始角度': 'AStartAng',
            '弧终止角度': 'AEndAng',
            '弧笔宽': 'APenW',
            '线型': 'LineType',
            '线颜色': 'LineCol',
            '线宽': 'LineWid',
            '线类型': 'LineKind',
            'X系数': 'XFact',
            'Y系数': 'YFact',
            '辅助颜色': 'AuxCol',
            # 新 WL 字段（修正后）
            '线型号': 'LineNo',
            '辅助线号': 'AuxLineNo',
            '覆盖方式': 'CoverMode',
            '线种类': 'LineKind2',
            '辅助色': 'AuxCol2',
            '图层': 'Layer',
            # slib 附加字段（已是 ASCII ≤10 字符，直接保留）
            'sl_lib':   'sl_lib',
            'sl_id':    'sl_id',
            'sl_type':  'sl_type',
            'sl_aux':   'sl_aux',
            'sl_cov':   'sl_cov',
            'sl_parts': 'sl_parts',
            'sl_ok':    'sl_ok',
        }
        
        new_columns = []
        used = set()
        
        for col in df.columns:
            if col in field_map:
                # 使用映射的英文字段名
                eng_col = field_map[col]
                # 处理重复字段名
                if eng_col in used:
                    idx = 1
                    new_eng_col = f"{eng_col}_{idx}"
                    while new_eng_col in used:
                        idx += 1
                        new_eng_col = f"{eng_col}_{idx}"
                    eng_col = new_eng_col
                new_columns.append(eng_col)
                used.add(eng_col)
            else:
                # 对于未映射的字段，使用拼音转换
                try:
                    import pypinyin
                    pinyin = ''.join([i[0] for i in pypinyin.pinyin(str(col), style=pypinyin.NORMAL)])
                    pinyin = ''.join([c if c.isalnum() or c == '_' else '_' for c in pinyin])
                    if len(pinyin) > 10:
                        pinyin = pinyin[:10]
                    if not pinyin:
                        pinyin = 'field'
                    
                    # 处理重复字段名
                    orig = pinyin
                    idx = 1
                    while pinyin in used:
                        suffix = f"_{idx}"
                        pinyin = (orig[:10-len(suffix)] if len(orig) > 10-len(suffix) else orig) + suffix
                        idx += 1
                    
                    new_columns.append(pinyin)
                    used.add(pinyin)
                except ImportError:
                    # 如果没有pypinyin，使用简单的英文转换
                    eng_name = ''.join([c if c.isalnum() or c == '_' else '_' for c in str(col)])
                    if len(eng_name) > 10:
                        eng_name = eng_name[:10]
                    if not eng_name:
                        eng_name = 'field'
                    
                    # 处理重复字段名
                    orig = eng_name
                    idx = 1
                    while eng_name in used:
                        suffix = f"_{idx}"
                        eng_name = (orig[:10-len(suffix)] if len(orig) > 10-len(suffix) else orig) + suffix
                        idx += 1
                    
                    new_columns.append(eng_name)
                    used.add(eng_name)
        
        df.columns = new_columns
        return df

    def __len__(self):
        return len(self.geom)

    def __str__(self):
        return f"MapGIS文件读取器\n{len(self)} 个要素 (类型: {self.shape_type})"

    def __del__(self):
        file_obj = getattr(self, 'file', None)
        if file_obj is not None:
            try:
                file_obj.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__del__()


class InvalidFileError(Exception):
    def __str__(self):
        return "无法识别文件的几何类型"

class InvalidDirectoryError(Exception):
    pass

class TopoError(Exception):
    def __str__(self):
        return "该WP文件存在拓扑错误"

def get_multipolygons(lines):
    """辅助函数：根据线环关系生成多面对象。"""
    n = len(lines)
    relation = np.zeros((n, n))
    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            try:
                inside = shapely.geometry.Polygon(lines[i]).within(shapely.geometry.Polygon(lines[j]))
            except Exception:
                inside = np.array([
                    shapely.geometry.Point(pt).within(shapely.geometry.Polygon(lines[j])) for pt in lines[i]
                ]).any()
            if inside:
                relation[i, j] = 1
    level_0 = {}
    for i in range(n):
        if not (relation[i] == 1).any():
            level_0[i] = [lines[i]]
    for i in range(n):
        if (relation[i] == 1).sum() == 1:
            idx = np.argwhere(relation[i] == 1)[0][0]
            if idx not in level_0:
                level_0[idx] = []
            level_0[idx].append(lines[i])
    if not ((relation == 1).sum(1) == 2).any():
        return [shapely.geometry.Polygon(i[0], i[1:]) for i in level_0.values()]
    else:
        temp = [shapely.geometry.Polygon(i[0], i[1:]) for i in level_0.values()]
        temp.extend(get_multipolygons([lines[i] for i in np.argwhere((relation == 1).sum(1) > 1).flatten()]))
        return temp



def _read_mapgis_spatial_header(filepath):
    """轻量读取文件头中的空间元数据。"""
    result = {
        'proj_type': None,
        'ellipsoid': None,
        'raw_scale': None,
        'central_meridian': None,
        'detection': None,
        'error': None,
    }

    try:
        type_dict = {b'WMAP`D22': 'POINT', b'WMAP`D23': 'POLYGON', b'WMAP`D21': 'LINE'}
        with open(filepath, 'rb') as f:
            magic = f.read(8)
            if magic not in type_dict:
                result['error'] = '无法识别的文件类型'
                return result
            f.read(4)
            f.read(4)
            f.seek(109)
            proj_type = ord(f.read(1))
            ellipsoid = ord(f.read(1))
            result['proj_type'] = proj_type
            result['ellipsoid'] = ellipsoid
            f.seek(143)
            result['raw_scale'] = struct.unpack('1d', f.read(8))[0]
            f.seek(151)
            try:
                raw_cl = struct.unpack('1d', f.read(8))[0]
                cl = (int(str(raw_cl).split('.')[0][:-4])
                      + int(str(raw_cl).split('.')[0][-4:-2]) / 60.0
                      + int(str(raw_cl).split('.')[0][-2:]) / 60.0 / 60)
                result['central_meridian'] = cl if proj_type == 5 else None
            except Exception:
                result['central_meridian'] = None

        _tmp = object.__new__(MapGisReader)
        _tmp._raw_proj_type = proj_type
        _tmp._raw_ellipsoid = ellipsoid
        _tmp._raw_central_meridian = result['central_meridian']
        result['detection'] = _tmp._detect_wkid_from_metadata()

        # 仅基于文件头预检查时，proj_type=0 且比例尺为0的文件很容易误判为经纬度。
        # 这类文件在实际转换阶段还会结合真实坐标范围进一步判断，这里先降为低置信。
        if proj_type == 0 and result['raw_scale'] is not None and result['raw_scale'] <= 0:
            det = result['detection'] or {}
            result['detection'] = {
                'detected_epsg': None,
                'confidence': 'low',
                'datum': det.get('datum', '未知'),
                'proj_desc': '地理坐标系（待核实）',
                'central_meridian': det.get('central_meridian'),
                'note': '文件头地理坐标且比例尺为0，可能为图幅局部坐标；转换时将结合坐标范围进一步判断',
            }
    except Exception as e:
        result['error'] = str(e)

    return result


# ──────────────────────────────────────────────────────────────────────────────
# 轻量 CRS 预检查：仅读文件头元数据，不解析几何，用于转换前的坐标系汇总弹窗
# ──────────────────────────────────────────────────────────────────────────────
def peek_crs(filepath):
    """快速读取 MapGIS 文件的 CRS 元数据，不解析几何数据。"""
    return _read_mapgis_spatial_header(filepath)


# ──────────────────────────────────────────────────────────────────────────────
# MapGIS 工程文件（.mpj）解析器
# ──────────────────────────────────────────────────────────────────────────────
class MapGISProjectReader:
    """
    解析 MapGIS 工程文件（.mpj/.MPJ），提取其中引用的图层路径列表。

    支持的 magic number：
      - b'WMAP\\`D2:'  （实际观测到的格式）
      - b'GDMP\\`D29'  （文档记录的格式）

    文件布局：
      - 头部：1113 字节
      - 偏移 12-13：图层数量（unsigned short，little-endian）
      - 偏移 750-753：第一个 workspace 记录的文件偏移（int，值固定为 1113）
      - 每条 workspace 记录：400 字节，布局如下：
          offset 0   : 类型字节 (1=WT点, 0=WL线, 2=WP面)
          offset 1   : 状态字节
          offset 2-129: 路径字段（多个以 NUL 分隔的字符串，首个通常以 '.\\' 开头）
          offset 130-257: 图层描述
          offset 258-289: 包围盒（4 个 double：xmin, ymin, xmax, ymax）
    """

    MAGIC_VARIANTS = [b'WMAP\x60D2:', b'GDMP\x60D29']
    HEADER_SIZE = 1113
    RECORD_SIZE = 400
    COUNT_OFFSET = 12          # unsigned short: 图层数
    WORKSPACE_PTR_OFFSET = 750  # int: 第一条记录偏移（应=1113）

    # 类型字节 → 文件扩展名
    TYPE_EXT = {1: '.WT', 0: '.WL', 2: '.WP'}

    def __init__(self, mpj_path: str):
        self.mpj_path = os.path.abspath(mpj_path)
        self.mpj_dir = os.path.dirname(self.mpj_path)
        self.layers = []          # list of dict with keys: type, ext, paths, description, bbox
        self._parse()

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------

    def resolve_layer_paths(self) -> list:
        """
        为每个图层解析出实际存在的文件路径（按优先级）。
        返回 list of dict：{'path': str, 'name': str, 'ext': str}
        重名冲突（同文件名不同目录且均存在）的图层会被跳过并打印警告。

        副作用：
          self.last_resolve_report — list of dict，每个图层一条记录：
            {
              'raw_paths': list[str],   # MPJ 中记录的原始路径
              'tried_paths': list[str], # 实际尝试过的候选路径
              'resolved': str | None,   # 最终解析结果（None 表示未找到）
              'skip_reason': str,       # 跳过原因（'not_found' / 'duplicate' / ''）
            }
        """
        self.last_resolve_report = []
        seen_names = {}  # basename.upper() → [resolved_path, ...]

        # 第一轮：收集所有可解析路径 + 诊断信息
        resolve_cache = {}  # id(layer) → (resolved, tried)
        for layer in self.layers:
            resolved, tried = self._resolve_one(layer)
            resolve_cache[id(layer)] = (resolved, tried)
            if resolved is not None:
                basename = os.path.basename(resolved).upper()
                seen_names.setdefault(basename, [])
                seen_names[basename].append(resolved)

        # 第二轮：过滤重名冲突，生成结果 + 诊断报告
        results = []
        for layer in self.layers:
            resolved, tried = resolve_cache[id(layer)]
            raw_paths = layer.get('paths', [])

            if resolved is None:
                report = {
                    'raw_paths': raw_paths,
                    'tried_paths': tried,
                    'resolved': None,
                    'skip_reason': 'not_found',
                }
                self.last_resolve_report.append(report)
                print(f"[MPJ] 跳过（未找到）: {raw_paths} | 尝试过: {tried}")
                continue

            basename = os.path.basename(resolved).upper()
            candidates = seen_names.get(basename, [])
            unique_paths = list(dict.fromkeys(candidates))

            if len(unique_paths) > 1:
                report = {
                    'raw_paths': raw_paths,
                    'tried_paths': tried,
                    'resolved': resolved,
                    'skip_reason': 'duplicate',
                }
                self.last_resolve_report.append(report)
                print(f"[MPJ] 跳过（重名冲突）: {basename} -> {unique_paths}")
                continue

            report = {
                'raw_paths': raw_paths,
                'tried_paths': tried,
                'resolved': resolved,
                'skip_reason': '',
            }
            self.last_resolve_report.append(report)
            results.append({
                'path': resolved,
                'name': os.path.splitext(os.path.basename(resolved))[0],
                'ext': os.path.splitext(resolved)[1].upper().lstrip('.'),
            })

        return results

    @property
    def layer_count(self) -> int:
        return len(self.layers)

    # ------------------------------------------------------------------
    # 内部：解析 MPJ 文件
    # ------------------------------------------------------------------

    def _parse(self):
        with open(self.mpj_path, 'rb') as f:
            header = f.read(self.HEADER_SIZE)

        # 验证 magic number
        magic_ok = any(header.startswith(m) for m in self.MAGIC_VARIANTS)
        if not magic_ok:
            raise ValueError(
                f"不支持的 MPJ 格式（magic={header[:8]!r}），仅支持 WMAP/GDMP 格式"
            )

        # 读取图层数
        count = struct.unpack_from('<H', header, self.COUNT_OFFSET)[0]

        # 读取所有 workspace 记录
        with open(self.mpj_path, 'rb') as f:
            f.seek(self.HEADER_SIZE)
            for _ in range(count):
                raw = f.read(self.RECORD_SIZE)
                if len(raw) < self.RECORD_SIZE:
                    break
                self.layers.append(self._parse_record(raw))

    def _parse_record(self, raw: bytes) -> dict:
        type_byte = raw[0]
        ext = self.TYPE_EXT.get(type_byte, '.WT')

        # 路径字段：offset 2 ~ 129（128 字节），多个 NUL 分隔字符串
        path_field = raw[2:130]
        paths = self._extract_strings(path_field)

        # 描述字段：offset 130 ~ 257
        desc_field = raw[130:258]
        description = self._extract_strings(desc_field)
        description = description[0] if description else ''

        # 包围盒：offset 258 ~ 289（4 doubles）
        try:
            bbox = struct.unpack_from('<4d', raw, 258)
        except struct.error:
            bbox = (0.0, 0.0, 0.0, 0.0)

        return {
            'type': type_byte,
            'ext': ext,
            'paths': paths,
            'description': description,
            'bbox': bbox,
        }

    @staticmethod
    def _extract_strings(field: bytes) -> list:
        """将字节字段按 NUL 分割，解码为字符串列表（去掉空串）。"""
        parts = field.split(b'\x00')
        result = []
        for p in parts:
            s = p.decode('gbk', errors='ignore').strip()
            if s:
                result.append(s)
        return result

    # ------------------------------------------------------------------
    # 内部：路径解析（优先级）
    # ------------------------------------------------------------------

    def _resolve_one(self, layer: dict):
        """
        按优先级解析图层路径。
        返回 (resolved_path_or_None, tried_paths_list)。

        tried_paths 记录了所有实际测试过的候选路径，供上层诊断日志使用。

        优先级：
          1. 原始绝对路径直接存在
          2. 相对于 MPJ 目录拼接（去掉 '.\' 前缀）
          3. 同名文件在 MPJ 目录下递归搜索（大小写不敏感）
        """
        paths = layer.get('paths', [])
        tried = []

        # 1. 原始路径 & 2. 相对路径
        for raw_path in paths:
            # 原始路径直接测试
            tried.append(raw_path)
            if os.path.isfile(raw_path):
                return raw_path, tried
            # 去掉前缀 '.\' 或 './' 后，相对于 mpj 目录
            clean = raw_path.lstrip('.').lstrip('/').lstrip('\\')
            candidate = os.path.join(self.mpj_dir, clean)
            tried.append(candidate)
            if os.path.isfile(candidate):
                return candidate, tried
            # 也尝试大小写变种（Windows 路径在 macOS/Linux 上需忽略大小写）
            lower = os.path.join(self.mpj_dir, clean.lower())
            upper = os.path.join(self.mpj_dir, clean.upper())
            for alt in (lower, upper):
                tried.append(alt)
                if os.path.isfile(alt):
                    return alt, tried

        # 3. 递归搜索同文件名（大小写不敏感）
        if paths:
            target_names = {os.path.basename(p).upper() for p in paths if p}
            matches = []
            for root, dirs, files in os.walk(self.mpj_dir):
                for fn in files:
                    if fn.upper() in target_names:
                        matches.append(os.path.join(root, fn))
            if len(matches) == 1:
                return matches[0], tried
            elif len(matches) > 1:
                # 多个匹配，由 resolve_layer_paths 处理重名逻辑
                return matches[0], tried

        return None, tried
