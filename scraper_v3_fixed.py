"""
競馬予想AI - scraper_v3_fixed.py（完全版・列名'指数'統一）
最終更新: 2026年2月5日
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import logging
import statistics
from typing import List, Dict, Optional, Tuple
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from enhanced_scorer_fixed_v4 import EnhancedRaceScorer
except ImportError as e:
    logger.error(f"Import error: {e}")
    raise ImportError("enhanced_scorer_fixed_v4.py が必要です")


class NetkeibaRaceScraper:
    """netkeibaスクレイパー v4（完全版）"""
    
    def __init__(self, scraping_delay: float = 1.0, debug_mode: bool = False):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.scorer = EnhancedRaceScorer(debug_mode=debug_mode)
        self.scraping_delay = scraping_delay
        self.debug_mode = debug_mode
        self.debug_logs = []
        self.skip_new_horse = True  # 新馬戦はスキップする（過去データなし）
        self.cache_hits = 0
        self.api_calls = 0
        self.progress_callback = None  # 進捗コールバック関数

    def _init_session_state(self):
        """Streamlitのsession_stateを初期化"""
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
        """馬名ベースのキャッシュキー"""
        normalized = re.sub(r'\s+', '', horse_name).lower()
        return normalized

    def _get_from_cache(self, horse_name: str) -> Optional[List[Dict]]:
        """馬名ベースでキャッシュ取得"""
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
        """馬名ベースでキャッシュ保存"""
        if not self._init_session_state():
            return
        
        try:
            import streamlit as st
            cache_key = self._get_cache_key_by_name(horse_name)
            st.session_state.horse_cache_by_name[cache_key] = data
            # デバッグログを即座に出力
            import sys
            log_msg = f"  💾 キャッシュ保存(馬名): {horse_name}"
            self._debug_print(log_msg, "DEBUG")
            sys.stdout.flush()  # 即座にフラッシュ
        except Exception as e:
            logger.warning(f"キャッシュ保存エラー: {e}")

    def _check_race_cache(self, race_name: str, horse_names: List[str]) -> Optional[pd.DataFrame]:
        """同じレース名・同じ馬の組み合わせがキャッシュにあるかチェック"""
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
        """レース結果をキャッシュ"""
        if not self._init_session_state():
            return
        
        try:
            import streamlit as st
            race_key = re.sub(r'\s+', '', race_name).lower()
            st.session_state.race_cache[race_key] = df.copy()
            self._debug_print(f"💾 レースキャッシュ保存: {race_name}", "INFO")
        except Exception as e:
            logger.warning(f"レースキャッシュ保存エラー: {e}")

    def get_cache_stats(self) -> Dict:
        """キャッシュ統計を取得"""
        try:
            import streamlit as st
            name_cache_size = len(st.session_state.get('horse_cache_by_name', {}))
            race_cache_size = len(st.session_state.get('race_cache', {}))
        except:
            name_cache_size = 0
            race_cache_size = 0
        
        total = self.cache_hits + self.api_calls
        return {
            'name_cache_size': name_cache_size,
            'race_cache_size': race_cache_size,
            'cache_hits': self.cache_hits,
            'api_calls': self.api_calls,
            'hit_rate': (self.cache_hits / total * 100) if total > 0 else 0
        }

    def clear_cache(self):
        """キャッシュクリア"""
        try:
            import streamlit as st
            st.session_state.horse_cache_by_name = {}
            st.session_state.race_cache = {}
            self.cache_hits = 0
            self.api_calls = 0
            logger.info("キャッシュをクリアしました")
        except Exception as e:
            logger.error(f"キャッシュクリアエラー: {e}")

    def _debug_print(self, message: str, level: str = "INFO"):
        """デバッグ出力"""
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

    def check_if_new_horse_race(self, soup: BeautifulSoup, race_name: str = "") -> Tuple[bool, str]:
        """新馬戦かどうかを判定（レース名のみで判断）"""
        # レース名に「新馬」が含まれる場合のみスキップ
        # 「2歳新馬」「3歳新馬」など
        if '新馬' in race_name:
            return True, f"レース名に'新馬'を検出: {race_name}"
        
        # 「未勝利」はスキップしない
        return False, ""

    def get_race_data(self, race_id: str) -> Dict:
        """レースデータを取得（完全版）"""
        self._debug_print(f"=" * 70)
        self._debug_print(f"レースID: {race_id} の解析を開始")
        stats = self.get_cache_stats()
        self._debug_print(f"キャッシュ: 馬名{stats['name_cache_size']}件/レース{stats['race_cache_size']}件")
        self._debug_print(f"=" * 70)
        
        url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
        
        try:
            self._debug_print(f"URLアクセス: {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            response.encoding = 'EUC-JP'
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='EUC-JP')
            self._debug_print("ページ取得成功")
        except Exception as e:
            raise Exception(f"ページ取得失敗: {e}")

        race_name = self._get_race_name(soup)
        race_distance = self._get_race_distance(soup)
        track_type = self._get_track_type(soup)
        course = self._get_course_name(race_id)

        # 新馬戦判定
        is_new_horse, reason = self.check_if_new_horse_race(soup, race_name)
        
        if is_new_horse and self.skip_new_horse:
            self._debug_print(f"")
            self._debug_print(f"🚫 【新馬戦検出】予想を中止します", "WARNING")
            self._debug_print(f"   理由: {reason}", "WARNING")
            self._debug_print(f"   レース名: {race_name}", "WARNING")
            self._debug_print(f"")
            
            return {
                "race_name": race_name,
                "distance": race_distance,
                "track_type": track_type,
                "course": course,
                "df": pd.DataFrame(),
                "is_new_horse_race": True,
                "skip_reason": reason,
                "debug_logs": self.debug_logs,
                "message": "新馬戦のため予想を中止しました",
                "cache_stats": self.get_cache_stats()
            }

        self._debug_print(f"")
        self._debug_print(f"【レース情報】")
        self._debug_print(f"  レース名: {race_name}")
        self._debug_print(f"  コース: {course}")
        self._debug_print(f"  距離: {race_distance}m")
        self._debug_print(f"  馬場: {track_type}")
        self._debug_print(f"")

        horse_data = self._parse_shutuba(soup)
        
        self._debug_print(f"【取得した馬データ】")
        for i, h in enumerate(horse_data, 1):
            self._debug_print(f"  {i}. 馬番{h.get('馬番', '?')} {h.get('馬名', '不明')} | "
                            f"斤量:{h.get('斤量', '?')}kg | 騎手:{h.get('騎手', '?')}")
        self._debug_print(f"")
        
        if not horse_data:
            raise Exception("出馬表を取得できませんでした")

        # レースキャッシュチェック
        horse_names = [h['馬名'] for h in horse_data]
        cached_df = self._check_race_cache(race_name, horse_names)
        
        if cached_df is not None:
            self._debug_print(f"✅ 同一レースのキャッシュを再利用します", "INFO")
            
            # キャッシュから返す時も列名を保証
            if '総合指数' in cached_df.columns:
                cached_df = cached_df.rename(columns={'総合指数': '指数'})
            if '指数' not in cached_df.columns:
                cached_df['指数'] = 0.0
            
            return {
                "race_name": race_name,
                "distance": race_distance,
                "track_type": track_type,
                "course": course,
                "df": cached_df,
                "is_new_horse_race": False,
                "from_cache": True,
                "debug_logs": self.debug_logs,
                "cache_stats": self.get_cache_stats()
            }

        self._debug_print(f"🐴 {len(horse_data)}頭のデータを取得")
        self._debug_print(f"")

        df = pd.DataFrame(horse_data)
        df["指数"] = 0.0

        for index, row in df.iterrows():
            # 進捗コールバックを呼び出し
            if self.progress_callback:
                self.progress_callback(row['馬名'], index + 1, len(df))
            
            if row.get("horse_id"):
                self._debug_print(f"-" * 60)
                self._debug_print(f"【{row['馬名']}】(馬番:{row['馬番']}) 分析開始")
                self._debug_print(f"  斤量: {row['斤量']}kg | 騎手: {row['騎手']}")
                
                history = self._get_horse_history_cached(
                    row["horse_id"],
                    row["馬名"],
                    row["斤量"],
                    race_distance,
                    course
                )
                
                if history:
                    self._debug_print(f"  過去戦績: {len(history)}レース取得")
                    for idx, race in enumerate(history[:5], 1):
                        last_3f = race.get('last_3f', 0)
                        race_avg = race.get('race_avg_last_3f', 0)
                        
                        dist = race.get('dist', 1600)
                        if race_avg <= 0:
                            if dist <= 1400:
                                race_avg = 34.5
                            elif dist <= 1800:
                                race_avg = 35.0
                            elif dist <= 2200:
                                race_avg = 36.0
                            else:
                                race_avg = 37.0
                        
                        is_fast = last_3f > 0 and last_3f < race_avg
                        fast_mark = "◯" if is_fast else " "
                        
                        weight = race.get('weight', 0)
                        weight_mark = "★" if weight >= 57.0 else " " if weight >= 55.0 else ""
                        
                        self._debug_print(f"    {idx}走前: {race.get('race_name', '不明')[:15]:15s} | "
                                        f"{race.get('dist', '?')}m | "
                                        f"着順:{race.get('chakujun', '?'):>2}着 | "
                                        f"斤量:{weight:>4.1f}kg{weight_mark} | "
                                        f"上がり3F:{last_3f:>5.1f}s ({fast_mark}基準{race_avg:.1f}s)")
                else:
                    self._debug_print(f"  ⚠️ 過去戦績なし（新馬またはデータなし）")
                
                if history:
                    is_short_distance = race_distance <= 1600
                    self._debug_print(f"  評価モード: {'短距離(斤量-タイム評価)' if is_short_distance else '中長距離(従来評価)'}")
                    
                    analysis = self.scorer.calculate_total_score(
                        current_weight=row["斤量"],
                        target_course=course,
                        target_distance=race_distance,
                        history_data=history,
                        target_track_type=track_type
                    )
                    
                    df.at[index, "指数"] = analysis["total_score"]
                    
                    breakdown = analysis.get('breakdown', {})
                    
                    self._debug_print(f"  【スコア内訳】")
                    self._debug_print(f"    上がり3F相対評価: {breakdown.get('last_3f_relative', 0):>6.1f}")
                    if is_short_distance:
                        wt_score = breakdown.get('weight_time_score', 0)
                        self._debug_print(f"    斤量-タイム評価: {wt_score:>6.1f} ←短距離特化")
                        if wt_score > 0:
                            self._debug_print(f"      → 重い斤量で速い上がりを記録")
                    else:
                        self._debug_print(f"    後半4F評価: {breakdown.get('late_4f_turf', 0):>6.1f}")
                        self._debug_print(f"    斤量ペナルティ: {breakdown.get('weight_penalty', 0):>6.1f}")
                    self._debug_print(f"    距離適性: {breakdown.get('distance_suitability', 0):>6.1f}")
                    self._debug_print(f"    コース適性: {breakdown.get('course_suitability', 0):>6.1f}")
                    self._debug_print(f"    {'─' * 20}")
                    self._debug_print(f"    総合スコア: {analysis['total_score']:>6.1f}")
                else:
                    df.at[index, "指数"] = 0.0
                    self._debug_print(f"  ⚠️ 過去戦績なしのため0点")
                
                time.sleep(self.scraping_delay)

        df = df.sort_values("指数", ascending=False).reset_index(drop=True)
        
        # 最終ランキング
        self._debug_print(f"")
        self._debug_print(f"=" * 70)
        self._debug_print(f"【最終ランキング】")
        stats = self.get_cache_stats()
        self._debug_print(f"キャッシュ統計: 馬名{stats['name_cache_size']}件/レース{stats['race_cache_size']}件/ヒット率{stats['hit_rate']:.1f}%")
        self._debug_print(f"=" * 70)
        
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
            
            danger_mark = "⚠️" if is_dangerous else "  "
            self._debug_print(f"  {i+1:2d}位 {danger_mark} {mark:4s} 馬番{row['馬番']:>2s} {row['馬名']:12s} "
                            f"指数:{row['指数']:6.1f} 斤量:{row['斤量']:4.1f}kg")
        self._debug_print(f"=" * 70)
        
        df["印"] = marks

        # レース結果をキャッシュ
        self._save_race_cache(race_name, df)

        # ============================================================
        # 【重要】列名を確実に'指数'に統一（防御的プログラミング）
        # ============================================================
        if '総合指数' in df.columns:
            df = df.rename(columns={'総合指数': '指数'})
        
        if '指数' not in df.columns:
            df['指数'] = 0.0
        # ============================================================

        return {
            "race_name": race_name,
            "distance": race_distance,
            "track_type": track_type,
            "course": course,
            "df": df,
            "is_new_horse_race": False,
            "from_cache": False,
            "debug_logs": self.debug_logs,
            "cache_stats": self.get_cache_stats()
        }

    def _get_horse_history_cached(self, horse_id: str, horse_name: str,
                                  current_weight: float,
                                  race_distance: int, course: str) -> List[Dict]:
        """馬名ベースキャッシュ付き馬データ取得"""
        cached_data = self._get_from_cache(horse_name)
        if cached_data is not None:
            return cached_data
        
        self.api_calls += 1
        self._debug_print(f"  🌐 API呼び出し (馬名: {horse_name})", "DEBUG")
        history = self._get_horse_history(horse_id, current_weight, race_distance, course)
        
        if history:
            self._save_to_cache(horse_name, history)
        
        return history

    def _get_horse_history(self, horse_id: str, current_weight: float,
                          target_distance: int, target_course: str) -> List[Dict]:
        """実際のAPI呼び出し（内部メソッド）"""
        url = f"https://db.netkeiba.com/horse/result/{horse_id}/"
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            
            table = soup.find("table", class_="db_h_race_results")
            if not table:
                return []
            
            headers = [th.text.strip() for th in table.find_all("th")]
            
            def find_col(keywords):
                for kw in keywords:
                    for i, h in enumerate(headers):
                        if kw in h:
                            return i
                return -1
            
            idx_date = find_col(["日付"])
            idx_course = find_col(["開催"])
            idx_race = find_col(["レース名"])
            idx_dist = find_col(["距離"])
            idx_chakujun = find_col(["着順"])
            idx_weight = find_col(["斤量"])
            idx_chakusa = find_col(["着差"])
            idx_3f = find_col(["上り"])
            
            if idx_date == -1: idx_date = 0
            if idx_course == -1: idx_course = 1
            if idx_race == -1: idx_race = 4
            if idx_dist == -1: idx_dist = 14
            if idx_chakujun == -1: idx_chakujun = 11
            if idx_weight == -1: idx_weight = 13
            if idx_chakusa == -1: idx_chakusa = 18
            if idx_3f == -1: idx_3f = 20
            
            rows = table.find_all("tr")[1:6]
            history = []
            
            for idx, row in enumerate(rows):
                cols = row.find_all("td")
                if len(cols) < max(idx_date, idx_course, idx_race, idx_dist, 
                                  idx_chakujun, idx_weight, idx_chakusa) + 1:
                    continue
                
                try:
                    date = cols[idx_date].text.strip().replace("/", ".")
                    course_name = cols[idx_course].text.strip()
                    
                    race_cell = cols[idx_race]
                    race_link = race_cell.find("a")
                    race_name = race_link.get_text(strip=True) if race_link else race_cell.get_text(strip=True)
                    
                    race_id = ""
                    if race_link:
                        href = race_link.get("href", "")
                        match = re.search(r"race/(\d{12})", href)
                        if match:
                            race_id = match.group(1)
                    
                    dist_text = cols[idx_dist].text.strip()
                    
                    # トラックタイプを距離列から直接パース（例: "芝1600", "ダ1200", "障3000"）
                    track_type_match = re.match(r"^(芝|ダ|ダート|障)", dist_text)
                    if track_type_match:
                        track_prefix = track_type_match.group(1)
                        if track_prefix == "芝":
                            race_track_type = "芝"
                        elif track_prefix in ["ダ", "ダート"]:
                            race_track_type = "ダート"
                        elif track_prefix == "障":
                            race_track_type = "障害"
                        else:
                            race_track_type = "不明"
                    else:
                        race_track_type = "不明"
                    
                    dist_match = re.search(r"(\d+)", dist_text)
                    distance = int(dist_match.group(1)) if dist_match else 0
                    
                    chakujun_text = cols[idx_chakujun].text.strip()
                    chakujun_match = re.search(r"(\d+)", chakujun_text)
                    chakujun = int(chakujun_match.group(1)) if chakujun_match else 99
                    
                    chakusa_text = cols[idx_chakusa].text.strip()
                    if not chakusa_text or chakusa_text in ["-", "**", "---"]:
                        chakusa_text = "0.0" if chakujun == 1 else "1.0"
                    
                    weight_text = cols[idx_weight].text.strip()
                    try:
                        weight = float(weight_text)
                    except:
                        weight = current_weight
                    
                    time_3f_text = cols[idx_3f].text.strip() if idx_3f < len(cols) else ""
                    try:
                        last_3f = float(time_3f_text)
                    except:
                        last_3f = 0.0
                    
                    race_stats = {}
                    if race_id and last_3f > 0:
                        time.sleep(0.3)
                        race_stats = self._get_race_last_3f_stats(race_id)
                    
                    history.append({
                        'date': date,
                        'course': course_name,
                        'dist': distance,
                        'track_type': race_track_type,  # 追加: 直接パースしたトラックタイプ
                        'chakujun': chakujun,
                        'chakusa': chakusa_text,
                        'weight': weight,
                        'last_3f': last_3f,
                        'race_name': race_name,
                        'race_avg_last_3f': race_stats.get('avg_last_3f', 0.0),
                        'race_min_last_3f': race_stats.get('min_last_3f', 0.0),
                        'race_max_last_3f': race_stats.get('max_last_3f', 0.0),
                        'race_std_last_3f': race_stats.get('std_last_3f', 0.0),
                    })
                    
                except Exception as e:
                    continue
            
            return history
            
        except Exception as e:
            logger.error(f"戦績取得エラー: {e}")
            return []

    def _get_course_name(self, race_id: str) -> str:
        venues = {
            "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
            "05": "東京", "06": "中山", "07": "中京", "08": "京都",
            "09": "阪神", "10": "小倉"
        }
        code = race_id[4:6] if len(race_id) >= 6 else ""
        return venues.get(code, "不明")

    def _parse_shutuba(self, soup: BeautifulSoup) -> List[Dict]:
        horse_data = []
        
        table = None
        patterns = [
            ("table", {"class_": "Shutuba_Table"}),
            ("table", {"class_": re.compile(r"shutuba", re.I)}),
            ("table", {"class_": "RaceList"}),
            ("table", {"class_": re.compile(r"race.*list", re.I)}),
        ]
        
        for tag, attrs in patterns:
            table = soup.find(tag, attrs)
            if table:
                break
        
        if not table:
            for t in soup.find_all("table"):
                if t.find("th") and ("馬名" in str(t) or "horse" in str(t).lower()):
                    table = t
                    break
        
        if not table:
            self._debug_print("❌ 出馬表テーブルが見つかりません", "ERROR")
            return []

        rows = table.find_all("tr")
        start = 1 if rows and rows[0].find("th") else 0
        
        for row_idx, row in enumerate(rows[start:], 1):
            cols = row.find_all(["td", "th"])
            if len(cols) < 5:
                continue
            
            try:
                info = self._extract_horse_info(cols, row, row_idx)
                if info and info.get("馬名") and info.get("horse_id"):
                    horse_data.append(info)
            except Exception as e:
                if self.debug_mode:
                    self._debug_print(f"  行{row_idx}の解析失敗: {e}", "WARNING")
                continue
        
        return horse_data

    def _extract_horse_info(self, cols, row, row_idx: int) -> Optional[Dict]:
        info = {
            "枠": "", "馬番": "", "馬名": "", "性齢": "",
            "斤量": 54.0, "騎手": "", "オッズ": 1.0, "horse_id": ""
        }
        
        for col in cols:
            if not info["馬名"]:
                link = col.find("a", href=re.compile(r"/horse/\d+"))
                if link:
                    info["馬名"] = link.get_text(strip=True)
                    href = link.get("href", "")
                    match = re.search(r"/horse/(\d{10,})", href)
                    if match:
                        info["horse_id"] = match.group(1)
        
        for col in cols:
            if not info["騎手"]:
                jockey_link = col.find("a", href=re.compile(r"/jockey/"))
                if jockey_link:
                    info["騎手"] = jockey_link.get_text(strip=True)
        
        for idx in range(min(3, len(cols))):
            col = cols[idx]
            text = col.get_text(strip=True)
            
            if not info["枠"] and len(text) == 1 and text.isdigit() and 1 <= int(text) <= 8:
                info["枠"] = text
            elif not info["馬番"] and len(text) <= 2 and text.isdigit() and 1 <= int(text) <= 18:
                info["馬番"] = text
        
        for col in cols:
            text = col.get_text(strip=True)
            
            if not info["性齢"]:
                if re.match(r"^[牡牝セ]\d{1,2}$", text):
                    info["性齢"] = text
            
            if info["斤量"] == 54.0:
                weight_match = re.match(r"^(\d{2}\.\d)$", text)
                if weight_match:
                    val = float(weight_match.group(1))
                    if 48.0 <= val <= 60.0:
                        info["斤量"] = val
                        continue
                
                weight_match = re.match(r"^(\d{2})$", text)
                if weight_match:
                    val = float(weight_match.group(1))
                    if 48.0 <= val <= 60.0:
                        info["斤量"] = val
                        continue
        
        if not info["馬名"] or not info["horse_id"]:
            return None
        
        if not info["枠"]:
            info["枠"] = str(row_idx)
        if not info["馬番"]:
            info["馬番"] = str(row_idx)
        
        return info

    def _get_race_last_3f_stats(self, race_id: str) -> Dict:
        """過去レースの上がり3F統計を取得（そのレース全体の平均）"""
        url = f"https://db.netkeiba.com/race/{race_id}/"
        
        try:
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 404:
                return {}
            
            response.raise_for_status()
            response.encoding = 'EUC-JP'
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='EUC-JP')
            
            table = soup.find("table", class_="race_table_01")
            if not table:
                return {}
            
            headers = table.find_all("th")
            last_3f_idx = -1
            
            for i, th in enumerate(headers):
                text = th.get_text(strip=True)
                if any(kw in text for kw in ["上り", "上がり", "3F"]):
                    last_3f_idx = i
                    break
            
            if last_3f_idx == -1:
                last_3f_idx = len(headers) - 2
            
            values = []
            for row in table.find_all("tr")[1:]:
                tds = row.find_all("td")
                if len(tds) > last_3f_idx:
                    try:
                        text = tds[last_3f_idx].get_text(strip=True)
                        text = re.sub(r"[()（）]", "", text)
                        if text and text != '-':
                            val = float(text)
                            if 30 < val < 50:
                                values.append(val)
                    except:
                        continue
            
            if not values:
                return {}
            
            result = {
                'avg_last_3f': round(statistics.mean(values), 2),
                'min_last_3f': round(min(values), 2),
                'max_last_3f': round(max(values), 2),
                'median_last_3f': round(statistics.median(values), 2),
                'std_last_3f': round(statistics.stdev(values), 2) if len(values) > 1 else 0.0,
                'count': len(values)
            }
            
            return result
            
        except Exception as e:
            return {}

    def _get_race_name(self, soup: BeautifulSoup) -> str:
        elem = soup.find("div", class_="RaceName")
        if elem:
            name = elem.get_text(strip=True)
            name = re.sub(r"出馬表.*", "", name).strip()
            if name:
                return name
        
        for h1 in soup.find_all("h1"):
            name = h1.get_text(strip=True)
            if name and len(name) > 2:
                return re.sub(r"出馬表.*", "", name).strip()
        
        return "レース"

    def _get_race_distance(self, soup: BeautifulSoup) -> int:
        elem = soup.find("div", class_="RaceData01")
        if elem:
            match = re.search(r"[芝ダ障](\d+)m", elem.text)
            if match:
                return int(match.group(1))
        return 1600

    def _get_track_type(self, soup: BeautifulSoup) -> str:
        elem = soup.find("div", class_="RaceData01")
        if elem:
            text = elem.text
            if "芝" in text:
                return "芝"
            elif "ダ" in text or "ダート" in text:
                return "ダート"
            elif "障" in text:
                return "障害"
        return "不明"


if __name__ == "__main__":
    print("✅ NetkeibaRaceScraper v4.2（完全版・列名'指数'統一）loaded")
