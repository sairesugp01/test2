"""
競馬予想AI - scraper_v7.py（Scrapling対応版）
最終更新: 2026年2月24日

主な変更点 (v6→v7):
- 【最重要】requests + BeautifulSoup → Scrapling の Fetcher に完全移行
  - EUC-JPの自動デコード対応（response.encoding = 'EUC-JP' 不要）
  - Cloudflare等のbot検知を回避（curl_cffiベースのTLS偽装）
  - セレクタAPIが簡潔（css/xpath/find/find_all → Scraplingネイティブ）
- adaptive=True でセレクタ変更への自動適応（サイト改修への耐性）
- _get_race_last_3f_stats / _parse_shutuba / _get_horse_history をScraplingに書き換え
- requests.Sessionを廃止 → Fetcher.get() に統一（セッション管理は内部で自動）
- v6の全機能（キャッシュ、脚質分析、ペース予測、スコア計算）を完全継承

必要ライブラリ:
  pip install scrapling[all]
  scrapling-install  # Playwright等のブラウザドライバ（必要な場合のみ）
"""

import time
import re
import logging
import statistics
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from collections import Counter

import pandas as pd

# ── Scrapling ─────────────────────────────────────────────────────────────────
from scrapling import Fetcher          # 静的ページ用（curl_cffi / TLS偽装）
# from scrapling import StealthyFetcher  # より強力なbot回避が必要な場合
# from scrapling import PlayWrightFetcher  # JS描画が必要な場合
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from enhanced_scorer_v7 import RaceScorer
except ImportError as e:
    logger.error(f"Import error: {e}")
    raise ImportError("enhanced_scorer_v7.py が必要です")


class NetkeibaRaceScraper:
    """netkeibaスクレイパー v7（Scrapling対応版）"""

    def __init__(self, scraping_delay: float = 1.5, debug_mode: bool = False):
        # ── Scraplingのフェッチャー ────────────────────────────────────────────
        # adaptive=True: 過去の成功セレクタを記憶し、サイト改修後も自動適応
        # stealthy=False → Fetcher で十分。bot検知が厳しい場合は StealthyFetcher へ
        self.fetcher = Fetcher(auto_match=True)
        # ──────────────────────────────────────────────────────────────────────

        self.scorer = RaceScorer(debug_mode=debug_mode)
        self.scraping_delay = scraping_delay
        self.debug_mode = debug_mode
        self.debug_logs: List[str] = []
        self.skip_new_horse = True
        self.cache_hits = 0
        self.api_calls = 0
        self.race_stats_cache: Dict[str, Dict] = {}
        self.progress_callback = None

    # ═══════════════════════════════════════════════════════════════════════════
    # 内部ユーティリティ（v6から継承）
    # ═══════════════════════════════════════════════════════════════════════════

    def _debug_print(self, message: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        if self.debug_mode:
            print(log_entry)
        self.debug_logs.append(log_entry)
        if level == "ERROR":
            logger.error(message)
        elif level == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)

    def _parse_sex_age(self, sex_age_str: str) -> Tuple[Optional[int], Optional[str]]:
        if not sex_age_str:
            return None, None
        import unicodedata
        normalized = unicodedata.normalize('NFKC', sex_age_str).replace(' ', '').replace('\u3000', '')
        match = re.match(r'^([牡牝セ])(\d{1,2})$', normalized)
        if match:
            return int(match.group(2)), match.group(1)
        return None, None

    def _get_course_name(self, race_id: str) -> str:
        venues = {
            "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
            "05": "東京", "06": "中山", "07": "中京", "08": "京都",
            "09": "阪神", "10": "小倉"
        }
        code = race_id[4:6] if len(race_id) >= 6 else ""
        return venues.get(code, "不明")

    # ═══════════════════════════════════════════════════════════════════════════
    # キャッシュ（v6から継承）
    # ═══════════════════════════════════════════════════════════════════════════

    def _init_session_state(self) -> bool:
        try:
            import streamlit as st
            if 'horse_cache_by_name' not in st.session_state:
                st.session_state.horse_cache_by_name = {}
            if 'race_cache' not in st.session_state:
                st.session_state.race_cache = {}
            return True
        except ImportError:
            return False

    def _get_cache_key_by_name(self, horse_name: str) -> str:
        return re.sub(r'\s+', '', horse_name).lower()

    def _get_from_cache(self, horse_name: str) -> Optional[List[Dict]]:
        if not self._init_session_state():
            return None
        try:
            import streamlit as st
            cache_key = self._get_cache_key_by_name(horse_name)
            if cache_key in st.session_state.horse_cache_by_name:
                self.cache_hits += 1
                self._debug_print(f"  📦 キャッシュヒット(馬名): {horse_name}", "DEBUG")
                return st.session_state.horse_cache_by_name[cache_key]
        except Exception as e:
            logger.warning(f"キャッシュ取得エラー: {e}")
        return None

    def _save_to_cache(self, horse_name: str, data: List[Dict]):
        if not self._init_session_state():
            return
        try:
            import streamlit as st
            st.session_state.horse_cache_by_name[self._get_cache_key_by_name(horse_name)] = data
        except Exception as e:
            logger.warning(f"キャッシュ保存エラー: {e}")

    def _check_race_cache(self, race_name: str, horse_names: List[str]) -> Optional[pd.DataFrame]:
        if not self._init_session_state():
            return None
        try:
            import streamlit as st
            race_key = re.sub(r'\s+', '', race_name).lower()
            horse_set = set(self._get_cache_key_by_name(h) for h in horse_names)
            for cached_race, cached_df in st.session_state.race_cache.items():
                if cached_race == race_key:
                    cached_horses = set(self._get_cache_key_by_name(h) for h in cached_df['馬名'].tolist())
                    if horse_set == cached_horses:
                        self._debug_print(f"📦 レースキャッシュヒット: {race_name}", "INFO")
                        return cached_df
        except Exception as e:
            logger.warning(f"レースキャッシュチェックエラー: {e}")
        return None

    def _save_race_cache(self, race_name: str, df: pd.DataFrame):
        if not self._init_session_state():
            return
        try:
            import streamlit as st
            race_key = re.sub(r'\s+', '', race_name).lower()
            st.session_state.race_cache[race_key] = df.copy()
        except Exception as e:
            logger.warning(f"レースキャッシュ保存エラー: {e}")

    def get_cache_stats(self) -> Dict:
        try:
            import streamlit as st
            name_cache_size = len(st.session_state.get('horse_cache_by_name', {}))
            race_cache_size = len(st.session_state.get('race_cache', {}))
        except Exception:
            name_cache_size = race_cache_size = 0
        total = self.cache_hits + self.api_calls
        return {
            'name_cache_size': name_cache_size,
            'race_cache_size': race_cache_size,
            'cache_hits': self.cache_hits,
            'api_calls': self.api_calls,
            'hit_rate': (self.cache_hits / total * 100) if total > 0 else 0,
        }

    def clear_cache(self):
        try:
            import streamlit as st
            st.session_state.horse_cache_by_name = {}
            st.session_state.race_cache = {}
        except Exception:
            pass
        self.cache_hits = 0
        self.api_calls = 0
        self.race_stats_cache = {}
        logger.info("キャッシュをクリアしました")

    # ═══════════════════════════════════════════════════════════════════════════
    # 脚質・ペース分析（v6から継承）
    # ═══════════════════════════════════════════════════════════════════════════

    def _extract_running_style_from_history(self, history: List[Dict]) -> Optional[Dict]:
        if not history:
            return None
        styles = []
        for race in history[:5]:
            corner_pos = race.get('corner_pos', 0) or race.get('position_4c', 0)
            field_size = race.get('field_size', 16)
            last_3f = race.get('last_3f', 0.0)
            race_avg_3f = race.get('race_avg_last_3f', 0.0)
            if corner_pos > 0 and field_size > 0:
                style_info = self.scorer.style_analyzer.classify_running_style(
                    position_4c=corner_pos, field_size=field_size,
                    last_3f=last_3f, race_avg_3f=race_avg_3f
                )
                if style_info and style_info.get('style') != '不明':
                    styles.append(style_info)
        if not styles:
            return None
        style_counts = Counter(s['style'] for s in styles)
        most_common_style = style_counts.most_common(1)[0][0]
        matching_styles = [s for s in styles if s['style'] == most_common_style]
        avg_confidence = sum(s['confidence'] for s in matching_styles) / len(matching_styles)
        consistency = len(matching_styles) / len(styles)
        return {
            'style': most_common_style,
            'confidence': min(avg_confidence * (0.7 + 0.3 * consistency), 0.95)
        }

    def _predict_race_pace(self, horses_running_styles: List[Dict],
                           field_size: int, course: str = '東京') -> Dict:
        if not horses_running_styles:
            return {'pace': 'ミドル', 'front_ratio': 0.30}
        pace_result = self.scorer.style_analyzer.predict_race_pace(
            horses_running_styles, field_size, course
        )
        style_counts = Counter(
            h.get('style', '不明') for h in horses_running_styles
            if h.get('style') != '不明'
        )
        pace_result['distribution'] = {
            '逃げ': style_counts.get('逃げ', 0),
            '先行': style_counts.get('先行', 0),
            '差し': style_counts.get('差し', 0),
            '追込': style_counts.get('追込', 0),
        }
        return pace_result

    # ═══════════════════════════════════════════════════════════════════════════
    # ▼▼▼ Scrapling書き換え：レースページ（出馬表）取得 ▼▼▼
    # ═══════════════════════════════════════════════════════════════════════════

    def _fetch_page(self, url: str, encoding: str = 'EUC-JP'):
        """
        Scraplingでページを取得して Adaptor を返す。
        - curl_cffiベースのTLS偽装でbot検知を回避
        - encoding を明示指定（netkeiba は EUC-JP）
        """
        response = self.fetcher.get(url, timeout=15, encoding=encoding)
        return response  # Scrapling の Response/Adaptor オブジェクト

    def _get_race_info(self, page) -> Tuple[str, int, str, str]:
        """レース名・距離・馬場・コース種別を取得（Scraplingセレクタ版）"""
        # レース名
        race_name_elem = page.css_first('.RaceName')
        if race_name_elem:
            race_name = re.sub(r"出馬表.*", "", race_name_elem.text).strip()
        else:
            h1 = page.css_first('h1')
            race_name = re.sub(r"出馬表.*", "", h1.text).strip() if h1 else "レース"

        # 距離・コース種別・馬場
        race_data_elem = page.css_first('.RaceData01')
        race_distance = 1600
        track_type = "不明"
        baba = "良"

        if race_data_elem:
            text = race_data_elem.text
            dist_match = re.search(r"[芝ダ障](\d+)m", text)
            if dist_match:
                race_distance = int(dist_match.group(1))

            # コース種別（障害を最優先）
            if "障" in text:
                track_type = "障害"
            elif "芝" in text:
                track_type = "芝"
            elif "ダ" in text:
                track_type = "ダート"

            # 馬場状態
            if "不良" in text:
                baba = "不良"
            elif "稍重" in text or "稍" in text:
                baba = "稍重"
            elif "重" in text and "稍" not in text:
                baba = "重"
            else:
                baba = "良"

        # レース名に「障害」があれば上書き
        if "障害" in race_name or "障" in race_name:
            track_type = "障害"

        return race_name, race_distance, track_type, baba

    def _parse_shutuba(self, page) -> List[Dict]:
        """
        出馬表テーブルをScraplingで解析。
        v6の複雑なパターンマッチを css() で簡潔化。
        """
        horse_data = []

        # 出馬表テーブルを取得（複数パターンでフォールバック）
        table = (
            page.css_first('table.Shutuba_Table') or
            page.css_first('table[class*="shutuba" i]') or
            page.css_first('table.RaceList')
        )
        if not table:
            # 「馬名」を含む任意のテーブルにフォールバック
            for t in page.css('table'):
                if t.css_first('th') and ('馬名' in t.html_content or 'horse' in t.html_content.lower()):
                    table = t
                    break

        if not table:
            self._debug_print("❌ 出馬表テーブルが見つかりません", "ERROR")
            return []

        rows = table.css('tr')
        start = 1 if rows and rows[0].css('th') else 0

        for row_idx, row in enumerate(rows[start:], 1):
            cols = row.css('td, th')
            if len(cols) < 5:
                continue
            try:
                info = self._extract_horse_info_scrapling(cols, row_idx)
                if info and info.get("馬名") and info.get("horse_id"):
                    horse_data.append(info)
            except Exception as e:
                if self.debug_mode:
                    self._debug_print(f"  行{row_idx}の解析失敗: {e}", "WARNING")

        return horse_data

    def _extract_horse_info_scrapling(self, cols, row_idx: int) -> Optional[Dict]:
        """
        Scraplingの Adaptor API で馬情報を抽出。
        v6の BeautifulSoup 版より簡潔。
        """
        import unicodedata

        info = {
            "枠": "", "馬番": "", "馬名": "", "性齢": "",
            "斤量": 54.0, "騎手": "", "オッズ": 1.0, "horse_id": ""
        }

        # 馬名・horse_id: /horse/NNNN... のリンク
        for col in cols:
            horse_link = col.css_first('a[href*="/horse/"]')
            if horse_link and not info["馬名"]:
                info["馬名"] = horse_link.text.strip()
                match = re.search(r"/horse/(\d{10,})", horse_link.attrib.get('href', ''))
                if match:
                    info["horse_id"] = match.group(1)

        # 騎手: /jockey/ のリンク
        for col in cols:
            jockey_link = col.css_first('a[href*="/jockey/"]')
            if jockey_link and not info["騎手"]:
                info["騎手"] = jockey_link.text.strip()

        # 枠・馬番（先頭3列）
        for idx in range(min(3, len(cols))):
            text = cols[idx].text.strip()
            if not info["枠"] and len(text) == 1 and text.isdigit() and 1 <= int(text) <= 8:
                info["枠"] = text
            elif not info["馬番"] and len(text) <= 2 and text.isdigit() and 1 <= int(text) <= 18:
                info["馬番"] = text

        # 性齢・斤量
        for col in cols:
            text = col.text.strip()
            norm = unicodedata.normalize('NFKC', text).replace(' ', '').replace('\u3000', '')

            if not info["性齢"]:
                if re.match(r"^[牡牝セ]\d{1,2}$", norm):
                    info["性齢"] = norm
                else:
                    m = re.search(r'([牡牝セ])(\d{1,2})', norm)
                    if m:
                        info["性齢"] = m.group(1) + m.group(2)
                # サブ要素（span/div）も探索
                if not info["性齢"]:
                    for sub in col.css('span, div'):
                        sub_norm = unicodedata.normalize('NFKC', sub.text.strip()).replace(' ', '')
                        if re.match(r"^[牡牝セ]\d{1,2}$", sub_norm):
                            info["性齢"] = sub_norm
                            break

            if info["斤量"] == 54.0:
                wm = re.match(r"^(\d{2}(?:\.\d)?)$", text)
                if wm:
                    val = float(wm.group(1))
                    if 48.0 <= val <= 60.0:
                        info["斤量"] = val

        if not info["馬名"] or not info["horse_id"]:
            return None
        if not info["枠"]:
            info["枠"] = str(row_idx)
        if not info["馬番"]:
            info["馬番"] = str(row_idx)

        return info

    # ═══════════════════════════════════════════════════════════════════════════
    # ▼▼▼ Scrapling書き換え：馬の過去戦績取得 ▼▼▼
    # ═══════════════════════════════════════════════════════════════════════════

    def _get_horse_history(self, horse_id: str, current_weight: float,
                           target_distance: int, target_course: str) -> List[Dict]:
        """
        戦績ページをScraplingで取得・解析。
        v6から変更点:
        - requests → Fetcher.get()
        - BeautifulSoup → Scraplingのcss()/find()
        - EUC-JPは encoding='EUC-JP' で自動処理
        """
        url = f"https://db.netkeiba.com/horse/result/{horse_id}/"
        try:
            page = self._fetch_page(url, encoding='EUC-JP')
        except Exception as e:
            logger.error(f"戦績取得エラー: {e}")
            return []

        # 戦績テーブル
        table = page.css_first('table.db_h_race_results')
        if not table:
            return []

        headers = [th.text.strip() for th in table.css('th')]

        def find_col(keywords):
            for kw in keywords:
                for i, h in enumerate(headers):
                    if kw in h:
                        return i
            return -1

        idx_date     = find_col(["日付"])
        idx_course   = find_col(["開催"])
        idx_race     = find_col(["レース名"])
        idx_dist     = find_col(["距離"])
        idx_chakujun = find_col(["着順"])
        idx_weight   = find_col(["斤量"])
        idx_chakusa  = find_col(["着差"])
        idx_3f       = find_col(["上り"])
        idx_time     = find_col(["タイム", "走破タイム"])
        idx_corner   = find_col(["通過順位", "通過", "コーナー"])
        idx_tosu     = find_col(["頭数", "出走頭数"])

        # デフォルト列インデックス（見つからなかった場合）
        if idx_date     == -1: idx_date     = 0
        if idx_course   == -1: idx_course   = 1
        if idx_race     == -1: idx_race     = 4
        if idx_dist     == -1: idx_dist     = 14
        if idx_chakujun == -1: idx_chakujun = 11
        if idx_weight   == -1: idx_weight   = 13
        if idx_chakusa  == -1: idx_chakusa  = 18
        if idx_3f       == -1: idx_3f       = 20

        rows = table.css('tr')[1:8]  # 最大7行（中止・除外スキップで5走分確保）
        history = []

        _known_courses = [
            "札幌", "函館", "福島", "新潟", "東京", "中山", "中京", "京都", "阪神", "小倉",
            "大井", "川崎", "船橋", "浦和", "門別", "盛岡", "水沢", "金沢", "笠松",
            "名古屋", "園田", "姫路", "高知", "佐賀"
        ]

        for idx, row in enumerate(rows):
            cols = row.css('td')
            if len(cols) <= max(idx_date, idx_course, idx_race, idx_dist,
                                idx_chakujun, idx_weight, idx_chakusa):
                continue
            try:
                # ── 日付 ──────────────────────────────────────────────────────
                date_raw = cols[idx_date].text.strip()
                dm = re.search(r'(\d{4})[年/](\d{1,2})[月/](\d{1,2})', date_raw)
                date = f"{dm.group(1)}/{int(dm.group(2)):02d}/{int(dm.group(3)):02d}" if dm else date_raw

                # ── コース（競馬場名） ─────────────────────────────────────────
                course_raw = cols[idx_course].text.strip()
                course_name = next((c for c in _known_courses if c in course_raw), course_raw)

                # ── レース名・race_id ──────────────────────────────────────────
                race_cell = cols[idx_race]
                race_link = race_cell.css_first('a')
                race_name_hist = race_link.text.strip() if race_link else race_cell.text.strip()
                race_id = ""
                if race_link:
                    href = race_link.attrib.get('href', '')
                    m = re.search(r"race/(\d{12})", href)
                    if m:
                        race_id = m.group(1)

                # ── 距離・コース種別 ────────────────────────────────────────────
                dist_text = cols[idx_dist].text.strip()
                track_type_match = re.match(r"^(芝|ダ|ダート|障)", dist_text)
                if track_type_match:
                    tp = track_type_match.group(1)
                    race_track_type = "芝" if tp == "芝" else "ダート" if tp in ["ダ", "ダート"] else "障害"
                else:
                    race_track_type = "不明"
                dist_m = re.search(r"(\d+)", dist_text)
                distance = int(dist_m.group(1)) if dist_m else 0

                # ── 着順（中止・除外・取消はスキップ） ──────────────────────────
                chakujun_text = cols[idx_chakujun].text.strip()
                if any(kw in chakujun_text for kw in ["中止", "除外", "取消", "取り消"]):
                    continue
                cm = re.search(r"(\d+)", chakujun_text)
                chakujun = int(cm.group(1)) if cm else 99

                # ── 着差 ──────────────────────────────────────────────────────
                chakusa_text = cols[idx_chakusa].text.strip() if idx_chakusa < len(cols) else ""
                winner_margin = 0.0
                if chakujun == 1:
                    goal_time_diff = 0.0
                    try:
                        winner_margin = float(chakusa_text)
                    except Exception:
                        winner_margin = 0.0
                else:
                    try:
                        goal_time_diff = float(chakusa_text)
                    except Exception:
                        goal_time_diff = 0.0

                # ── 斤量 ──────────────────────────────────────────────────────
                try:
                    weight = float(cols[idx_weight].text.strip())
                except Exception:
                    weight = current_weight

                # ── 上がり3F ──────────────────────────────────────────────────
                try:
                    last_3f = float(cols[idx_3f].text.strip()) if idx_3f < len(cols) else 0.0
                except Exception:
                    last_3f = 0.0

                # ── 走破タイム ─────────────────────────────────────────────────
                goal_sec = 0.0
                if idx_time != -1 and idx_time < len(cols):
                    time_raw = cols[idx_time].text.strip()
                    try:
                        if ':' in time_raw:
                            parts = time_raw.split(':')
                            goal_sec = int(parts[0]) * 60 + float(parts[1])
                        else:
                            goal_sec = float(time_raw)
                    except Exception:
                        pass

                # ── 通過順位（4角） ────────────────────────────────────────────
                corner_pos = 0
                if idx_corner != -1 and idx_corner < len(cols):
                    positions = re.findall(r'\d+', cols[idx_corner].text.strip())
                    if positions:
                        corner_pos = int(positions[-1])

                # ── 出走頭数 ──────────────────────────────────────────────────
                field_size = 16
                if idx_tosu != -1 and idx_tosu < len(cols):
                    tm = re.search(r'(\d+)', cols[idx_tosu].text.strip())
                    if tm:
                        field_size = int(tm.group(1))

                # ── レース統計（上がり3F基準値・ラップ） ──────────────────────
                race_stats: Dict = {}
                if race_id and last_3f > 0:
                    if race_id in self.race_stats_cache:
                        race_stats = self.race_stats_cache[race_id]
                        self._debug_print(f"    📦 レース統計キャッシュヒット: {race_id}", "DEBUG")
                    else:
                        time.sleep(0.5)
                        race_stats = self._get_race_last_3f_stats(race_id)
                        if race_stats:
                            self.race_stats_cache[race_id] = race_stats

                lap_times = race_stats.get('lap_times', [])
                late_4f = self._calculate_late_4f_from_laps(lap_times) if lap_times else 0.0
                baba = race_stats.get('baba', '良')

                history.append({
                    'date': date,
                    'race_date': date,
                    'course': course_name,
                    'dist': distance,
                    'dist_text': dist_text,
                    'track_type': race_track_type,
                    'baba': baba,
                    'chakujun': chakujun,
                    'chakusa': chakusa_text,
                    'goal_time_diff': goal_time_diff,
                    'goal_sec': goal_sec,
                    'winner_margin': winner_margin if chakujun == 1 else 0.0,
                    'weight': weight,
                    'last_3f': last_3f,
                    'late_4f': late_4f,
                    'race_name': race_name_hist,
                    'race_avg_last_3f': race_stats.get('avg_last_3f', 0.0),
                    'race_min_last_3f': race_stats.get('min_last_3f', 0.0),
                    'race_max_last_3f': race_stats.get('max_last_3f', 0.0),
                    'race_std_last_3f': race_stats.get('std_last_3f', 0.0),
                    'all_horses_results': race_stats.get('all_horses_results', []),
                    'corner_pos': corner_pos,
                    'position_4c': corner_pos,
                    'field_size': field_size,
                })

            except Exception:
                continue

        return history

    def _get_horse_history_cached(self, horse_id: str, horse_name: str,
                                  current_weight: float,
                                  race_distance: int, course: str) -> List[Dict]:
        cached = self._get_from_cache(horse_name)
        if cached is not None:
            return cached
        self.api_calls += 1
        self._debug_print(f"  🌐 API呼び出し (馬名: {horse_name})", "DEBUG")
        history = self._get_horse_history(horse_id, current_weight, race_distance, course)
        if history:
            self._save_to_cache(horse_name, history)
        time.sleep(self.scraping_delay)
        return history

    # ═══════════════════════════════════════════════════════════════════════════
    # ▼▼▼ Scrapling書き換え：過去レース統計 ▼▼▼
    # ═══════════════════════════════════════════════════════════════════════════

    def _extract_lap_times(self, page) -> List[float]:
        """
        Scraplingの find_by_text / css でラップタイムを抽出。
        """
        lap_times: List[float] = []

        # 方法1: "ラップ"テキストを含む要素を探す
        for elem in page.find_by_text('ラップ', case_sensitive=False):
            raw = elem.text.strip()
            times = re.findall(r'\d+\.\d+', raw)
            if times:
                lap_times = [float(t) for t in times]
                break
            # 次の兄弟要素も確認
            sib = elem.next
            if sib:
                times = re.findall(r'\d+\.\d+', sib.text.strip() if hasattr(sib, 'text') else '')
                if times:
                    lap_times = [float(t) for t in times]
                    break

        if lap_times:
            return lap_times

        # 方法2: テーブルの行でラップタイムを探す
        for row in page.css('table tr'):
            row_text = row.text.strip()
            if 'ラップ' in row_text:
                times = re.findall(r'\d+\.\d+', row_text)
                if len(times) >= 4:
                    return [float(t) for t in times]

        # 方法3: div内を探す
        for div in page.css('div'):
            div_text = div.text.strip()
            if 'ラップ' in div_text and '-' in div_text:
                times = re.findall(r'\d+\.\d+', div_text)
                if len(times) >= 4:
                    return [float(t) for t in times]

        return []

    def _calculate_late_4f_from_laps(self, lap_times: List[float]) -> float:
        if not lap_times or len(lap_times) < 4:
            return 0.0
        return round(sum(lap_times[-4:]), 1)

    def _get_race_last_3f_stats(self, race_id: str) -> Dict:
        """
        過去レースの上がり3F統計を取得（Scrapling版）。
        v6と同一ロジック、BeautifulSoupをScraplingに置き換え。
        """
        url = f"https://db.netkeiba.com/race/{race_id}/"
        try:
            page = self._fetch_page(url, encoding='EUC-JP')
        except Exception:
            return {}

        lap_times = self._extract_lap_times(page)

        # 馬場状態
        race_data = page.css_first('.RaceData01')
        baba = "良"
        if race_data:
            t = race_data.text
            if "不良" in t:
                baba = "不良"
            elif "稍重" in t or "稍" in t:
                baba = "稍重"
            elif "重" in t and "稍" not in t:
                baba = "重"

        table = page.css_first('table.race_table_01')
        if not table:
            return {}

        headers = [th.text.strip() for th in table.css('th')]

        def find_col_idx(keywords):
            for kw in keywords:
                for i, h in enumerate(headers):
                    if kw in h:
                        return i
            return -1

        last_3f_idx  = find_col_idx(["上り", "上がり", "3F"])
        chakujun_idx = find_col_idx(["着順", "着"])
        time_idx     = find_col_idx(["タイム", "走破タイム"])

        if last_3f_idx  == -1: last_3f_idx  = len(headers) - 2
        if chakujun_idx == -1: chakujun_idx = 0
        if time_idx     == -1: time_idx     = 7

        def parse_time_to_sec(t: str) -> Optional[float]:
            t = t.strip()
            if ':' in t:
                parts = t.split(':')
                try:
                    return int(parts[0]) * 60 + float(parts[1])
                except Exception:
                    return None
            try:
                return float(t)
            except Exception:
                return None

        values: List[float] = []
        all_horses_results: List[Dict] = []
        first_place_time: Optional[float] = None

        for row in table.css('tr')[1:]:
            tds = row.css('td')
            if len(tds) <= max(last_3f_idx, chakujun_idx, time_idx):
                continue
            try:
                cm = re.search(r'(\d+)', tds[chakujun_idx].text.strip())
                if not cm:
                    continue
                chakujun = int(cm.group(1))

                goal_sec = parse_time_to_sec(tds[time_idx].text.strip())

                last_3f_raw = re.sub(r"[()（）]", "", tds[last_3f_idx].text.strip())
                try:
                    last_3f = float(last_3f_raw)
                except Exception:
                    last_3f = 0.0

                if chakujun == 1 and goal_sec:
                    first_place_time = goal_sec

                all_horses_results.append({
                    'chakujun': chakujun,
                    'last_3f': last_3f,
                    'goal_sec': goal_sec,
                    'goal_time_diff': 0.0,
                })

                if 30 < last_3f < 50:
                    values.append(last_3f)
            except Exception:
                continue

        if first_place_time:
            for h in all_horses_results:
                if h['chakujun'] == 1:
                    h['goal_time_diff'] = 0.0
                elif h['goal_sec']:
                    h['goal_time_diff'] = round(h['goal_sec'] - first_place_time, 3)

        if not values:
            return {}

        return {
            'avg_last_3f':    round(statistics.mean(values), 2),
            'min_last_3f':    round(min(values), 2),
            'max_last_3f':    round(max(values), 2),
            'median_last_3f': round(statistics.median(values), 2),
            'std_last_3f':    round(statistics.stdev(values), 2) if len(values) > 1 else 0.0,
            'count':          len(values),
            'all_horses_results': all_horses_results,
            'lap_times': lap_times,
            'baba': baba,
        }

    # ═══════════════════════════════════════════════════════════════════════════
    # メインエントリーポイント（v6から構造継承・Scrapling対応）
    # ═══════════════════════════════════════════════════════════════════════════

    def check_if_new_horse_race(self, race_name: str) -> Tuple[bool, str]:
        if '新馬' in race_name:
            return True, f"レース名に'新馬'を検出: {race_name}"
        return False, ""

    def get_race_data(self, race_id: str) -> Dict:
        """レースデータを取得（Scrapling版メインメソッド）"""
        self._debug_print("=" * 70)
        self._debug_print(f"レースID: {race_id} の解析を開始")
        stats = self.get_cache_stats()
        self._debug_print(f"キャッシュ: 馬名{stats['name_cache_size']}件/レース{stats['race_cache_size']}件")
        self._debug_print("=" * 70)

        url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
        course = self._get_course_name(race_id)

        # ── ページ取得（Scrapling） ─────────────────────────────────────────
        try:
            self._debug_print(f"URLアクセス: {url}")
            page = self._fetch_page(url, encoding='EUC-JP')
            self._debug_print("ページ取得成功")
        except Exception as e:
            raise Exception(f"ページ取得失敗: {e}")

        # ── 取りやめ・404 検出 ─────────────────────────────────────────────
        page_text = page.get_all_text()
        if any(kw in page_text for kw in ['取りやめ', '中止', 'レース情報がありません']):
            self._debug_print("⚠️ 【レース取りやめ検出】", "WARNING")
            return {
                "race_name": "レース取りやめ", "distance": 0,
                "track_type": "不明", "course": course,
                "df": pd.DataFrame(), "is_cancelled": True,
                "skip_reason": "レース取りやめ", "debug_logs": self.debug_logs,
            }

        # ── レース基本情報 ─────────────────────────────────────────────────
        race_name, race_distance, track_type, _ = self._get_race_info(page)

        # 障害レース
        if track_type == "障害":
            self._debug_print("🚫 【障害レース検出】予想を中止", "WARNING")
            return {
                "race_name": race_name, "distance": race_distance,
                "track_type": track_type, "course": course,
                "df": pd.DataFrame(), "is_new_horse_race": False,
                "is_障害_race": True, "skip_reason": "障害レース",
                "debug_logs": self.debug_logs,
                "message": "障害レースのため予想を中止しました",
                "cache_stats": self.get_cache_stats(),
            }

        # 新馬戦
        is_new_horse, reason = self.check_if_new_horse_race(race_name)
        if is_new_horse and self.skip_new_horse:
            self._debug_print("🚫 【新馬戦検出】予想を中止", "WARNING")
            return {
                "race_name": race_name, "distance": race_distance,
                "track_type": track_type, "course": course,
                "df": pd.DataFrame(), "is_new_horse_race": True,
                "skip_reason": reason, "debug_logs": self.debug_logs,
                "message": "新馬戦のため予想を中止しました",
                "cache_stats": self.get_cache_stats(),
            }

        self._debug_print(f"【レース情報】レース名: {race_name} / コース: {course} / "
                          f"距離: {race_distance}m / 馬場: {track_type}")

        # ── 出馬表解析 ─────────────────────────────────────────────────────
        horse_data = self._parse_shutuba(page)

        if not horse_data:
            raise Exception("出馬表を取得できませんでした")

        # レースキャッシュチェック
        horse_names = [h['馬名'] for h in horse_data]
        cached_df = self._check_race_cache(race_name, horse_names)
        if cached_df is not None:
            if '総合指数' in cached_df.columns:
                cached_df = cached_df.rename(columns={'総合指数': '指数'})
            if '指数' not in cached_df.columns:
                cached_df['指数'] = 0.0
            return {
                "race_name": race_name, "distance": race_distance,
                "track_type": track_type, "course": course,
                "df": cached_df, "is_new_horse_race": False,
                "from_cache": True, "debug_logs": self.debug_logs,
                "cache_stats": self.get_cache_stats(),
            }

        df = pd.DataFrame(horse_data)
        df["指数"] = 0.0

        # ── 全馬の履歴一括取得＋脚質分析 ────────────────────────────────────
        self._debug_print(f"【馬データ一括取得＋脚質分析】全{len(df)}頭...")
        all_running_styles: List[Dict] = []
        horse_histories: Dict[int, List[Dict]] = {}

        for index, row in df.iterrows():
            if self.progress_callback:
                self.progress_callback(row['馬名'], index + 1, len(df))
            if row.get("horse_id"):
                history = self._get_horse_history_cached(
                    row["horse_id"], row["馬名"],
                    row["斤量"], race_distance, course
                )
                horse_histories[index] = history
                running_style = self._extract_running_style_from_history(history)
                if running_style:
                    all_running_styles.append(running_style)
                    self._debug_print(f"  {row['馬名']:12s}: {running_style['style']} "
                                      f"(信頼度{running_style['confidence']:.2f})")

        # ── ペース予測 ─────────────────────────────────────────────────────
        field_size = len(df)
        pace_prediction = (
            self._predict_race_pace(all_running_styles, field_size, course)
            if all_running_styles else None
        )

        if pace_prediction:
            self._debug_print(f"【ペース予測】{pace_prediction['pace']} / "
                              f"前残り率: {pace_prediction['front_ratio']:.1%}")

        # ── スコア計算 ─────────────────────────────────────────────────────
        for index, row in df.iterrows():
            if not row.get("horse_id"):
                continue
            history = horse_histories.get(index, [])
            self._debug_print(f"【{row['馬名']}】分析開始 (過去{len(history)}走)")
            if not history:
                df.at[index, "指数"] = 0.0
                continue

            running_style_info = self._extract_running_style_from_history(history)
            horse_age, horse_sex = self._parse_sex_age(row.get("性齢", ""))

            analysis = self.scorer.calculate_total_score(
                current_weight=row["斤量"],
                target_course=course,
                target_distance=race_distance,
                history_data=history,
                target_track_type=track_type,
                running_style_info=running_style_info,
                race_pace_prediction=pace_prediction,
                horse_age=horse_age,
                horse_sex=horse_sex,
            )
            df.at[index, "指数"] = analysis["total_score"]

            breakdown_text = self.scorer.format_score_breakdown_verbose(
                result=analysis,
                target_distance=race_distance,
                history_data=history,
                current_weight=row["斤量"],
                target_course=course,
                target_track_type=track_type,
                running_style_info=running_style_info,
                race_pace_prediction=pace_prediction,
                horse_age=horse_age,
                horse_sex=horse_sex,
            )
            for line in breakdown_text.split('\n'):
                self._debug_print(f"  {line}")

        # ── ランキング・印 ─────────────────────────────────────────────────
        df = df.sort_values("指数", ascending=False).reset_index(drop=True)
        marks = []
        for i, row in df.iterrows():
            is_dangerous = row["指数"] <= 0
            if is_dangerous:
                mark = "×" if i <= 5 else ""
            elif i == 0:
                mark = "◎"
            elif i == 1:
                mark = "○"
            elif i == 2:
                mark = "▲"
            elif i <= 5:
                mark = "△"
            else:
                mark = ""
            marks.append(mark)
            self._debug_print(f"  {i+1:2d}位 {'⚠️' if is_dangerous else '  '} {mark:4s} "
                              f"馬番{row['馬番']:>2s} {row['馬名']:12s} "
                              f"指数:{row['指数']:6.1f} 斤量:{row['斤量']:4.1f}kg")
        df["印"] = marks

        # 列名統一（防御的プログラミング）
        if '総合指数' in df.columns:
            df = df.rename(columns={'総合指数': '指数'})
        if '指数' not in df.columns:
            df['指数'] = 0.0

        self._save_race_cache(race_name, df)

        return {
            "race_name": race_name,
            "distance": race_distance,
            "track_type": track_type,
            "course": course,
            "df": df,
            "is_new_horse_race": False,
            "from_cache": False,
            "debug_logs": self.debug_logs,
            "cache_stats": self.get_cache_stats(),
        }


if __name__ == "__main__":
    print("✅ NetkeibaRaceScraper v7（Scrapling対応版）loaded")
