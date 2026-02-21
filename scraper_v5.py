"""
競馬予想AI - scraper_v5.py（enhanced_scorer_v7対応版）
最終更新: 2026年2月22日

主な変更点 (v4→v5):
- enhanced_scorer_v7 に対応（インポート変更）
- 過去戦績取得を5走→7行スクレイプに変更（中止除外スキップ込みで5走確保）

主な機能:
1. enhanced_scorer_v6の全機能に対応:
   - 新馬戦2戦目ブースト（着順別ボーナス）
   - 連続大敗ペナルティ（軽減条件付き）
   - 重賞出走ボーナス
   - 長期休養ペナルティ
   - 脚質×展開×コース特性の適合度
   - 後半4F評価（芝中長距離）
   - 斤量×タイム評価（短距離）
2. 脚質分析（通過順位から自動判定）
3. ペース予測（出走頭数・逃げ馬の質を考慮）
4. スコア内訳の見やすい表示
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
    from enhanced_scorer_v7 import RaceScorer
except ImportError as e:
    logger.error(f"Import error: {e}")
    raise ImportError("enhanced_scorer_v7.py が必要です")


class NetkeibaRaceScraper:
    """netkeibaスクレイパー v4（enhanced_scorer_v6対応版）"""
    
    def __init__(self, scraping_delay: float = 1.0, debug_mode: bool = False):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.scorer = RaceScorer(debug_mode=debug_mode)
        self.scraping_delay = scraping_delay
        self.debug_mode = debug_mode
        self.debug_logs = []
        self.skip_new_horse = True  # 新馬戦はスキップする（過去データなし）
        self.cache_hits = 0
        self.api_calls = 0
        self.progress_callback = None  # 進捗コールバック関数

    def _extract_running_style_from_history(self, history: List[Dict]) -> Optional[Dict]:
        """
        過去戦績から脚質を判定
        
        Args:
            history: 過去戦績のリスト
            
        Returns:
            {'style': '逃げ', 'confidence': 0.85, ...} or None
        """
        if not history:
            return None
        
        # 各レースの脚質を判定
        styles = []
        
        for race in history[:5]:  # 直近5走を使用
            corner_pos = race.get('corner_pos', 0) or race.get('position_4c', 0)
            field_size = race.get('field_size', 16)
            last_3f = race.get('last_3f', 0.0)
            race_avg_3f = race.get('race_avg_last_3f', 0.0)
            
            if corner_pos > 0 and field_size > 0:
                # 各レースの脚質を判定
                style_info = self.scorer.style_analyzer.classify_running_style(
                    position_4c=corner_pos,
                    field_size=field_size,
                    last_3f=last_3f,
                    race_avg_3f=race_avg_3f
                )
                
                if style_info and style_info.get('style') != '不明':
                    styles.append(style_info)
        
        if not styles:
            return None
        
        # 最頻出の脚質を採用
        from collections import Counter
        style_counts = Counter(s['style'] for s in styles)
        most_common_style = style_counts.most_common(1)[0][0]
        
        # 該当する脚質の平均信頼度を計算
        matching_styles = [s for s in styles if s['style'] == most_common_style]
        avg_confidence = sum(s['confidence'] for s in matching_styles) / len(matching_styles)
        
        # 一貫性ボーナス（同じ脚質が多いほど信頼度が上がる）
        consistency = len(matching_styles) / len(styles)
        final_confidence = avg_confidence * (0.7 + 0.3 * consistency)
        
        return {
            'style': most_common_style,
            'confidence': min(final_confidence, 0.95)
        }
    
    def _predict_race_pace(self, horses_running_styles: List[Dict], field_size: int, course: str = '東京') -> Dict:
        """
        レース全体のペースを予測
        
        Args:
            horses_running_styles: 各馬の脚質情報リスト
            field_size: 出走頭数
            course: コース名
            
        Returns:
            {'pace': 'ハイ'/'ミドル'/'スロー', ...}
        """
        if not horses_running_styles:
            return {'pace': 'ミドル', 'front_ratio': 0.30}
        
        # RaceScorerのペース予測機能を使用
        pace_result = self.scorer.style_analyzer.predict_race_pace(
            horses_running_styles, field_size, course
        )
        
        # 脚質の分布を計算
        from collections import Counter
        style_counts = Counter(h.get('style', '不明') for h in horses_running_styles if h.get('style') != '不明')
        pace_result['distribution'] = {
            '逃げ': style_counts.get('逃げ', 0),
            '先行': style_counts.get('先行', 0),
            '差し': style_counts.get('差し', 0),
            '追込': style_counts.get('追込', 0)
        }
        
        return pace_result

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
    
    def _parse_sex_age(self, sex_age_str: str) -> Tuple[Optional[int], Optional[str]]:
        """
        性齢文字列を解析
        
        Args:
            sex_age_str: 性齢文字列（例: "牡4", "牝5", "セ7"）
        
        Returns:
            (年齢, 性別) のタプル（例: (4, "牡"), (5, "牝")）
        """
        if not sex_age_str:
            return None, None
        
        # 全角数字・スペースを正規化
        import unicodedata
        normalized = unicodedata.normalize('NFKC', sex_age_str).replace(' ', '').replace('\u3000', '')

        # 正規表現で性別と年齢を抽出
        match = re.match(r'^([牡牝セ])(\d{1,2})$', normalized)
        if match:
            sex = match.group(1)
            age = int(match.group(2))
            return age, sex

        return None, None

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
            self._debug_print(f"  💾 キャッシュ保存(馬名): {horse_name}", "DEBUG")
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
            
            # レース取りやめ・404エラーの検出
            if response.status_code == 404:
                self._debug_print(f"")
                self._debug_print(f"⚠️ 【レース取りやめ検出】このレースは存在しません", "WARNING")
                self._debug_print(f"   ステータスコード: 404", "WARNING")
                self._debug_print(f"")
                return {
                    "race_name": "レース取りやめ",
                    "distance": 0,
                    "track_type": "不明",
                    "course": self._get_course_name(race_id),
                    "df": pd.DataFrame(),
                    "is_cancelled": True,
                    "skip_reason": "レース取りやめ（404エラー）",
                    "debug_logs": self.debug_logs,
                }
            
            response.raise_for_status()
            response.encoding = 'EUC-JP'
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='EUC-JP')
            self._debug_print("ページ取得成功")
            
            # ページ内容で取りやめチェック
            page_text = soup.get_text()
            if '取りやめ' in page_text or '中止' in page_text or 'レース情報がありません' in page_text:
                self._debug_print(f"")
                self._debug_print(f"⚠️ 【レース取りやめ検出】ページ内に取りやめ表示", "WARNING")
                self._debug_print(f"")
                return {
                    "race_name": "レース取りやめ",
                    "distance": 0,
                    "track_type": "不明",
                    "course": self._get_course_name(race_id),
                    "df": pd.DataFrame(),
                    "is_cancelled": True,
                    "skip_reason": "レース取りやめ",
                    "debug_logs": self.debug_logs,
                }
                
        except Exception as e:
            raise Exception(f"ページ取得失敗: {e}")

        race_name = self._get_race_name(soup)
        race_distance = self._get_race_distance(soup)
        track_type = self._get_track_type(soup)
        course = self._get_course_name(race_id)

        # 障害レース判定（予想対象外）
        if track_type == "障害":
            self._debug_print(f"")
            self._debug_print(f"🚫 【障害レース検出】予想を中止します", "WARNING")
            self._debug_print(f"   レース名: {race_name}", "WARNING")
            self._debug_print(f"   障害レースは平地とルールが異なるため予想対象外です", "WARNING")
            self._debug_print(f"")
            return {
                "race_name": race_name,
                "distance": race_distance,
                "track_type": track_type,
                "course": course,
                "df": pd.DataFrame(),
                "is_new_horse_race": False,
                "is_障害_race": True,
                "skip_reason": "障害レース",
                "debug_logs": self.debug_logs,
                "message": "障害レースのため予想を中止しました",
                "cache_stats": self.get_cache_stats()
            }

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
        
        # 【新機能】全馬の脚質を事前に分析してペース予測
        all_running_styles = []
        self._debug_print(f"【脚質分析】全{len(df)}頭の脚質を判定中...")
        
        for index, row in df.iterrows():
            if row.get("horse_id"):
                history = self._get_horse_history_cached(
                    row["horse_id"],
                    row["馬名"],
                    row["斤量"],
                    race_distance,
                    course
                )
                running_style = self._extract_running_style_from_history(history)
                if running_style:
                    all_running_styles.append(running_style)
                    self._debug_print(f"  {row['馬名']:12s}: {running_style['style']} (信頼度{running_style['confidence']:.2f})")
        
        # ペース予測
        field_size = len(df)
        pace_prediction = self._predict_race_pace(all_running_styles, field_size, course) if all_running_styles else None
        
        if pace_prediction:
            self._debug_print(f"")
            self._debug_print(f"【ペース予測】")
            self._debug_print(f"  予想ペース: {pace_prediction['pace']}")
            self._debug_print(f"  前残り率: {pace_prediction['front_ratio']:.1%}")
            self._debug_print(f"  逃げ: {pace_prediction['distribution']['逃げ']}頭 / "
                            f"先行: {pace_prediction['distribution']['先行']}頭 / "
                            f"差し: {pace_prediction['distribution']['差し']}頭 / "
                            f"追込: {pace_prediction['distribution']['追込']}頭")
            self._debug_print(f"  直線長: {pace_prediction.get('straight_length', 400)}m")
        
        self._debug_print(f"")

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
                        
                        goal_diff = race.get('goal_time_diff', 0.0)
                        big_loss_mark = "💀" if goal_diff >= 1.1 else ""
                        self._debug_print(f"    {idx}走前: {race.get('race_name', '不明')[:15]:15s} | "
                                        f"{race.get('dist', '?')}m | "
                                        f"着順:{race.get('chakujun', '?'):>2}着 | "
                                        f"斤量:{weight:>4.1f}kg{weight_mark} | "
                                        f"上がり3F:{last_3f:>5.1f}s ({fast_mark}基準{race_avg:.1f}s) | "
                                        f"着差:{goal_diff:.2f}s{big_loss_mark}")
                else:
                    self._debug_print(f"  ⚠️ 過去戦績なし（新馬またはデータなし）")
                
                if history:
                    # 【新機能】この馬の脚質を取得
                    running_style_info = self._extract_running_style_from_history(history)
                    
                    # 性齢を解析（例: "牡4" → 性別="牡", 年齢=4）
                    sex_age_raw = row.get("性齢", "")
                    horse_age, horse_sex = self._parse_sex_age(sex_age_raw)
                    if horse_age is None:
                        self._debug_print(f"  ⚠️ 性齢パース失敗: '{sex_age_raw}' → フォールバック58kg適用", "WARNING")
                    else:
                        self._debug_print(f"  性齢: {horse_sex}{horse_age}歳 → 斤量基準自動設定", "DEBUG")
                    
                    analysis = self.scorer.calculate_total_score(
                        current_weight=row["斤量"],
                        target_course=course,
                        target_distance=race_distance,
                        history_data=history,
                        target_track_type=track_type,
                        running_style_info=running_style_info,
                        race_pace_prediction=pace_prediction,
                        horse_age=horse_age,
                        horse_sex=horse_sex
                    )
                    
                    df.at[index, "指数"] = analysis["total_score"]
                    
                    # 【新機能】format_score_breakdown_verboseを使用（詳細版）
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

        
        # まず直接float変換を試みる
        try:
            return float(text)
        except ValueError:
            pass
        
        # 日本語特殊表記
        text = text.replace("\u3000", " ").strip()
        special = {
            "ハナ": 0.05, "はな": 0.05,
            "クビ": 0.15, "くび": 0.15,
            "アタマ": 0.10, "あたま": 0.10,
            "大差": 2.5, "だいさ": 2.5,
        }
        for k, v in special.items():
            if k in text:
                return v
        
        # 分数表記 "1/2", "3/4", "1 1/2" など
        import re as _re
        frac_pattern = _re.match(r'^(\d+)\s+(\d+)/(\d+)$', text)  # "1 1/2"
        if frac_pattern:
            whole = int(frac_pattern.group(1))
            num = int(frac_pattern.group(2))
            den = int(frac_pattern.group(3))
            return round((whole + num / den) * 0.6, 2)
        
        frac_only = _re.match(r'^(\d+)/(\d+)$', text)  # "1/2", "3/4"
        if frac_only:
            num = int(frac_only.group(1))
            den = int(frac_only.group(2))
            return round((num / den) * 0.6, 2)
        
        # 整数馬身 "1", "2", "3"
        int_match = _re.match(r'^(\d+)$', text)
        if int_match:
            return round(int(int_match.group(1)) * 0.6, 2)
        
        return 0.0

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
            idx_corner = find_col(["通過順位", "通過", "コーナー"])  # 通過順位（4角など）
            idx_tosu = find_col(["頭数", "出走頭数"])  # 頭数
            
            if idx_date == -1: idx_date = 0
            if idx_course == -1: idx_course = 1
            if idx_race == -1: idx_race = 4
            if idx_dist == -1: idx_dist = 14
            if idx_chakujun == -1: idx_chakujun = 11
            if idx_weight == -1: idx_weight = 13
            if idx_chakusa == -1: idx_chakusa = 18
            if idx_3f == -1: idx_3f = 20
            # 通過順位と頭数はオプション（見つからなくても-1のまま）
            
            rows = table.find_all("tr")[1:8]  # 中止・除外スキップを考慮し7行取得→実質5走確保
            history = []
            
            for idx, row in enumerate(rows):
                cols = row.find_all("td")
                if len(cols) < max(idx_date, idx_course, idx_race, idx_dist, 
                                  idx_chakujun, idx_weight, idx_chakusa) + 1:
                    continue
                
                try:
                    date_raw = cols[idx_date].text.strip()
                    # netkeibaの日付は "2025年11月03日" 形式なので "2025/11/03" に正規化
                    import re as _re
                    _date_match = _re.search(r'(\d{4})[年/](\d{1,2})[月/](\d{1,2})', date_raw)
                    if _date_match:
                        date = f"{_date_match.group(1)}/{int(_date_match.group(2)):02d}/{int(_date_match.group(3)):02d}"
                    else:
                        date = date_raw  # フォールバック
                    course_raw = cols[idx_course].text.strip()
                    # netkeibaの「開催」列は "1東京1" "2中山3" のような形式なので競馬場名だけ抽出
                    _known_courses = ["札幌", "函館", "福島", "新潟", "東京", "中山", "中京", "京都", "阪神", "小倉",
                                      "大井", "川崎", "船橋", "浦和", "門別", "盛岡", "水沢", "金沢", "笠松", "名古屋", "園田", "姫路", "高知", "佐賀"]
                    course_name = course_raw  # フォールバック
                    for _c in _known_courses:
                        if _c in course_raw:
                            course_name = _c
                            break
                    
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
                    # 競走中止・除外・取消は専用コードで管理
                    if "中止" in chakujun_text:
                        chakujun = 0   # 競走中止
                    elif "除外" in chakujun_text:
                        chakujun = 0   # 除外（出走取消含む）
                    elif "取消" in chakujun_text or "取り消" in chakujun_text:
                        chakujun = 0   # 取消
                    else:
                        chakujun_match = re.search(r"(\d+)", chakujun_text)
                        chakujun = int(chakujun_match.group(1)) if chakujun_match else 99

                    # 中止・除外・取消はスキップ（履歴に含めない）
                    if chakujun == 0:
                        logger.info(f"    [{idx+1}走前] {race_name[:15]:15s}: 着順='{chakujun_text}' → スキップ")
                        continue

                    # 着差列: 1着からのタイム差（秒数）のみ使用。変換不可時は0.0でログ出力
                    chakusa_text = cols[idx_chakusa].text.strip() if idx_chakusa < len(cols) else ""
                    if chakujun == 1:
                        goal_time_diff = 0.0
                    else:
                        try:
                            goal_time_diff = float(chakusa_text)
                        except Exception:
                            goal_time_diff = 0.0
                            logger.info(f"    [着差] {idx+1}走前 {race_name[:12]:12s}: "
                                        f"col={idx_chakusa} raw='{chakusa_text}' → 数値変換不可 ⚠️大敗判定スキップ")
                    
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
                    
                    # 通過順位を取得（4角順位など）
                    corner_pos = 0
                    if idx_corner != -1 and idx_corner < len(cols):
                        corner_text = cols[idx_corner].text.strip()
                        # "1-1-1-1"のような形式から最後の数字（4角）を取得
                        positions = re.findall(r'\d+', corner_text)
                        if positions:
                            corner_pos = int(positions[-1])  # 最後の位置（4角）
                    
                    # 頭数を取得
                    field_size = 16  # デフォルト
                    if idx_tosu != -1 and idx_tosu < len(cols):
                        tosu_text = cols[idx_tosu].text.strip()
                        tosu_match = re.search(r'(\d+)', tosu_text)
                        if tosu_match:
                            field_size = int(tosu_match.group(1))
                    
                    race_stats = {}
                    if race_id and last_3f > 0:
                        time.sleep(0.3)
                        race_stats = self._get_race_last_3f_stats(race_id)
                    elif not race_id and self.debug_mode:
                        logger.debug(f"    race_id未取得 → goal_time_diff=0.0（連続大敗判定不可）")
                    
                    # ラップタイムから後半4Fを計算
                    lap_times = race_stats.get('lap_times', [])
                    late_4f = self._calculate_late_4f_from_laps(lap_times) if lap_times else 0.0
                    
                    # 馬場状態を取得
                    baba = race_stats.get('baba', '良')
                    
                    # goal_time_diffは馬の戦績ページの「着差」列（1着基準・秒数）をそのまま使用
                    
                    history.append({
                        'date': date,
                        'race_date': date,  # v6用: YYYY/MM/DD形式
                        'course': course_name,
                        'dist': distance,
                        'track_type': race_track_type,
                        'baba': baba,  # 馬場状態
                        'chakujun': chakujun,
                        'chakusa': chakusa_text,
                        'goal_time_diff': goal_time_diff,  # v6用: 連続大敗ペナルティ
                        'weight': weight,
                        'last_3f': last_3f,
                        'late_4f': late_4f,  # 後半4F（ラップタイムから計算）
                        'race_name': race_name,
                        'race_avg_last_3f': race_stats.get('avg_last_3f', 0.0),
                        'race_min_last_3f': race_stats.get('min_last_3f', 0.0),
                        'race_max_last_3f': race_stats.get('max_last_3f', 0.0),
                        'race_std_last_3f': race_stats.get('std_last_3f', 0.0),
                        'all_horses_results': race_stats.get('all_horses_results', []),
                        'corner_pos': corner_pos,  # v6用: 通過順位（4角）
                        'position_4c': corner_pos,  # v6用: 新馬戦2戦目ブースト
                        'field_size': field_size,
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
                import unicodedata as _ud

                # パターン1: 独立したtdに「牝3」などが入っている場合
                _norm = _ud.normalize('NFKC', text).replace(' ', '').replace('\u3000', '')
                if re.match(r"^[牡牝セ]\d{1,2}$", _norm):
                    info["性齢"] = _norm

                # パターン2: 馬名と同じtdに「スーパーガール牝3」のように含まれる場合
                if not info["性齢"]:
                    m = re.search(r'([牡牝セ])(\d{1,2})', _norm)
                    if m:
                        info["性齢"] = m.group(1) + m.group(2)

                # パターン3: spanなどのサブ要素に性齢が入っている場合
                if not info["性齢"]:
                    for span in col.find_all(['span', 'td', 'div']):
                        _s = _ud.normalize('NFKC', span.get_text(strip=True)).replace(' ', '')
                        if re.match(r"^[牡牝セ]\d{1,2}$", _s):
                            info["性齢"] = _s
                            break
            
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

    def _extract_lap_times(self, soup: BeautifulSoup) -> List[float]:
        """
        レース結果ページからラップタイム（ハロンタイム）を抽出
        
        Returns:
            List[float]: 各ハロン（200m）のタイム（秒）のリスト
                        例: [12.3, 11.2, 11.5, 11.8, 12.0, 11.9, 11.7, 11.4]
        """
        lap_times = []
        
        try:
            # 方法1: "ラップ"というテキストを含む要素を探す
            # netkeibaでは「ラップ」ラベルの後にハロンタイムが並ぶ
            for elem in soup.find_all(text=lambda t: t and "ラップ" in t):
                parent = elem.parent
                if parent:
                    # 次の兄弟要素や親要素のテキストからラップタイムを抽出
                    next_elem = parent.next_sibling
                    if next_elem:
                        lap_text = next_elem.get_text(strip=True) if hasattr(next_elem, 'get_text') else str(next_elem)
                    else:
                        lap_text = parent.get_text(strip=True)
                    
                    # "ラップ"の後のテキストからハロンタイムを抽出
                    # 形式: "12.3-11.2-11.5-11.8" または "12.3 - 11.2 - 11.5"
                    lap_text = lap_text.replace("ラップ", "").strip()
                    
                    # ハイフンまたはスペースで区切られた数値を抽出
                    times = re.findall(r'\d+\.\d+', lap_text)
                    if times:
                        lap_times = [float(t) for t in times]
                        if self.debug_mode:
                            logger.debug(f"  ラップタイム取得: {len(lap_times)}ハロン")
                        break
            
            # 方法2: テーブル内のラップタイム行を探す
            if not lap_times:
                for table in soup.find_all("table"):
                    for row in table.find_all("tr"):
                        row_text = row.get_text(strip=True)
                        if "ラップ" in row_text:
                            # この行からラップタイムを抽出
                            times = re.findall(r'\d+\.\d+', row_text)
                            if len(times) >= 4:  # 少なくとも4ハロン以上
                                lap_times = [float(t) for t in times]
                                if self.debug_mode:
                                    logger.debug(f"  ラップタイム取得（テーブル）: {len(lap_times)}ハロン")
                                break
                    if lap_times:
                        break
            
            # 方法3: div内のラップタイム情報
            if not lap_times:
                for div in soup.find_all("div"):
                    div_text = div.get_text(strip=True)
                    if "ラップ" in div_text and "-" in div_text:
                        times = re.findall(r'\d+\.\d+', div_text)
                        if len(times) >= 4:
                            lap_times = [float(t) for t in times]
                            if self.debug_mode:
                                logger.debug(f"  ラップタイム取得（div）: {len(lap_times)}ハロン")
                            break
        
        except Exception as e:
            if self.debug_mode:
                logger.debug(f"  ラップタイム取得失敗: {e}")
        
        return lap_times
    
    def _calculate_late_4f_from_laps(self, lap_times: List[float]) -> float:
        """
        ラップタイムから後半4F（後半4ハロン = 800m）を計算
        
        Args:
            lap_times: ハロンタイム（200m×nハロン）のリスト
        
        Returns:
            float: 後半4Fのタイム（秒）。計算できない場合は0.0
        """
        if not lap_times or len(lap_times) < 4:
            return 0.0
        
        # 後半4ハロン = 最後の4つのハロンタイムを合計
        late_4f = sum(lap_times[-4:])
        return round(late_4f, 1)
    
    def _get_race_last_3f_stats(self, race_id: str) -> Dict:
        """過去レースの上がり3F統計と出走馬全体のデータを取得（ラップタイム・馬場状態含む）"""
        url = f"https://db.netkeiba.com/race/{race_id}/"
        
        try:
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 404:
                return {}
            
            response.raise_for_status()
            response.encoding = 'EUC-JP'
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='EUC-JP')
            
            # ラップタイムを取得
            lap_times = self._extract_lap_times(soup)
            
            # 馬場状態を取得
            baba = self._get_baba(soup)
            
            table = soup.find("table", class_="race_table_01")
            if not table:
                return {}
            
            headers = table.find_all("th")
            
            # 各列のインデックスを取得
            last_3f_idx = -1
            chakujun_idx = -1
            time_idx = -1  # ゴールタイム列

            for i, th in enumerate(headers):
                text = th.get_text(strip=True)
                if any(kw in text for kw in ["上り", "上がり", "3F"]):
                    last_3f_idx = i
                elif "着順" in text or text == "着":
                    chakujun_idx = i
                elif text == "タイム" or text == "走破タイム":
                    time_idx = i

            if last_3f_idx == -1:
                last_3f_idx = len(headers) - 2
            if chakujun_idx == -1:
                chakujun_idx = 0
            if time_idx == -1:
                time_idx = 7  # デフォルト位置

            def parse_time_to_sec(t):
                """'1:12.3' または '72.3' を秒数(float)に変換"""
                t = t.strip()
                if ':' in t:
                    parts = t.split(':')
                    try:
                        return int(parts[0]) * 60 + float(parts[1])
                    except:
                        return None
                try:
                    return float(t)
                except:
                    return None

            values = []
            all_horses_results = []
            first_place_time = None  # 1着のゴールタイム（秒）

            for row in table.find_all("tr")[1:]:
                tds = row.find_all("td")
                if len(tds) <= max(last_3f_idx, chakujun_idx, time_idx):
                    continue
                try:
                    chakujun_text = tds[chakujun_idx].get_text(strip=True)
                    chakujun_match = re.search(r'(\d+)', chakujun_text)
                    if not chakujun_match:
                        continue
                    chakujun = int(chakujun_match.group(1))

                    time_text = tds[time_idx].get_text(strip=True)
                    goal_sec = parse_time_to_sec(time_text)

                    last_3f_text = re.sub(r"[()（）]", "", tds[last_3f_idx].get_text(strip=True))
                    try:
                        last_3f = float(last_3f_text)
                    except:
                        last_3f = 0.0

                    if chakujun == 1 and goal_sec:
                        first_place_time = goal_sec

                    all_horses_results.append({
                        'chakujun': chakujun,
                        'last_3f': last_3f,
                        'goal_sec': goal_sec,   # 後でgoal_time_diffを計算するため保持
                        'goal_time_diff': 0.0   # 後で上書き
                    })

                    if last_3f > 30 and last_3f < 50:
                        values.append(last_3f)
                except:
                    continue

            # 1着タイムが取れた場合、全馬のgoal_time_diffを計算
            if first_place_time:
                for h in all_horses_results:
                    if h['chakujun'] == 1:
                        h['goal_time_diff'] = 0.0
                    elif h['goal_sec']:
                        h['goal_time_diff'] = round(h['goal_sec'] - first_place_time, 3)
                    else:
                        h['goal_time_diff'] = 0.0
            
            if not values:
                return {}
            
            result = {
                'avg_last_3f': round(statistics.mean(values), 2),
                'min_last_3f': round(min(values), 2),
                'max_last_3f': round(max(values), 2),
                'median_last_3f': round(statistics.median(values), 2),
                'std_last_3f': round(statistics.stdev(values), 2) if len(values) > 1 else 0.0,
                'count': len(values),
                'all_horses_results': all_horses_results,  # 全馬のデータ
                'lap_times': lap_times,  # ラップタイム（200mごと）
                'baba': baba  # 馬場状態
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
        # ① レース名に「障害」が含まれていれば最優先で障害判定
        race_name_elem = soup.find("div", class_="RaceName")
        race_name_text = race_name_elem.get_text(strip=True) if race_name_elem else ""
        if "障害" in race_name_text or "障" in race_name_text:
            return "障害"

        # ② RaceData01 のテキストで判定（障害を芝より先にチェック）
        elem = soup.find("div", class_="RaceData01")
        if elem:
            text = elem.text
            # 障害は「芝」「ダ」も含む複合コースなので最初に判定
            if "障" in text:
                return "障害"
            elif "芝" in text:
                return "芝"
            elif "ダ" in text or "ダート" in text:
                return "ダート"
        return "不明"
    
    def _get_baba(self, soup: BeautifulSoup) -> str:
        """
        馬場状態を取得
        
        Returns:
            str: '良', '稍重', '重', '不良' のいずれか（デフォルト: '良'）
        """
        elem = soup.find("div", class_="RaceData01")
        if elem:
            text = elem.get_text(strip=True)
            # 馬場状態の順序に注意（「稍重」を先にチェック）
            if "不良" in text:
                return "不良"
            elif "重" in text and "稍" not in text:
                return "重"
            elif "稍重" in text or "稍" in text:
                return "稍重"
            elif "良" in text:
                return "良"
        return "良"  # デフォルト


if __name__ == "__main__":
    print("✅ NetkeibaRaceScraper v5（enhanced_scorer_v7対応・過去5走評価版）loaded")
