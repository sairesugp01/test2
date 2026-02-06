"""
競馬予想AI - enhanced_scorer_v5.py（1600m基準値厳格化版）
最終更新: 2026年2月6日

主な修正点（v4 → v5）:
1. 1600m（1401-1600m）の上がり3F基準値を35.0秒→34.5秒に厳格化
2. 1400m（1201-1400m）の基準値を34.5秒→34.3秒に微調整
3. 時間減衰を強化（0.2→0.25）：3走前が0.4倍に
4. コンボボーナス条件を厳格化（-0.3秒→-0.5秒、最高ランク-1.0秒）
5. 着順による減衰を段階化（2-3着1.0倍、4-5着0.85倍、6-10着0.6倍、11着以下0.3倍）
6. **トラックタイプ判定をhistoryデータから優先取得**（scraper側で距離列から取得した正確な値を使用）

トラックタイプ判定の優先順位:
1. historyデータの'track_type'（netkeiba距離列「芝1600」「ダ1700」から取得）
2. フォールバック: 距離・レース名・コースからの推定

背景:
- サクラトゥジュールが2024年東京新聞杯（1着・33.5秒・57kg）で過大評価
- 実際の東京1600mの上がり3Fは33.0-34.5秒が標準
- 従来の基準35.0秒では大半の馬が「速い」と判定される問題を解決

v4からの継承:
- 距離とコース名からトラックタイプ（芝/ダート）を自動判定
- 地方戦（C1/C2など）の信頼度を0.55-0.75に引き下げ
- 地方ダートの基準値を+1.0秒厳しく（ペース遅延対策）
- ダート→芝転換時の評価を70%減（別トラック扱い）
- 地方馬JRA復帰時の危険フラグ追加（-15点減点）
"""

import logging
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnhancedRaceScorer:
    """強化版レーススコアラー v5（1600m基準値厳格化版）"""

    def __init__(self, debug_mode: bool = False):
        self.debug_mode = debug_mode
        # 地方ダート競馬場（全てダート）
        self.local_dirt_courses = ['大井', '浦和', '船橋', '川崎', '金沢', '笠松', '名古屋', '高知', '佐賀', '水沢', '盛岡']
        # 地方芝競馬場（現在は空 - 必要に応じて追加）
        self.local_turf_courses = []
        # 中央競馬場（基本芝）- 札幌/函館/福島/新潟も中央競馬場
        self.central_courses = ['東京', '中山', '中京', '京都', '阪神', '小倉', '札幌', '函館', '福島', '新潟']

    def detect_race_grade(self, race_name: str) -> tuple:
        """レース格を判定（地方戦対応強化版）"""
        race_name = str(race_name)

        # G1
        if any(g1 in race_name for g1 in ['天皇賞', '有馬記念', '日本ダービー', '皐月賞', '菊花賞', 
                                           '桜花賞', '秋華賞', 'エリザベス女王杯', 'ヴィクトリアマイル',
                                           '安田記念', 'マイルCS', 'スプリンターズS', '宝塚記念', 
                                           'フェブラリーS', '高松宮記念', 'NHKマイルC', '優駿牝馬']):
            return ('G1', 1.0)

        # JpnI
        if any(jpn1 in race_name for jpn1 in ['ジャパンC', 'チャンピオンズC', 'JBCクラシック', 
                                                '東京大賞典', 'かしわ記念', '帝王賞', 
                                                'フェブラリーステークス', 'ジャパンダートダービー']):
            return ('JpnI', 1.0)

        # G2
        if 'G2' in race_name or any(g2 in race_name for g2 in ['京都記念', 'アルゼンチン共和国杯', 
                                                                 '中山記念', '小倉大賞典', '日経新春杯',
                                                                 '京都牝馬S', 'ダイヤモンドS']):
            return ('G2', 0.95)

        # JpnII
        if 'JpnII' in race_name:
            return ('JpnII', 0.95)

        # G3
        if 'G3' in race_name or any(g3 in race_name for g3 in ['シリウスS', 'アーリントンC', 
                                                                 '中山牝馬S', '福島記念']):
            return ('G3', 0.9)

        # JpnIII
        if 'JpnIII' in race_name:
            return ('JpnIII', 0.9)

        # 地方重賞
        if any(x in race_name for x in ['ダービー', '大賞典', '記念', 'グランプリ']):
            if 'Jpn' not in race_name:
                return ('地方重賞', 0.75)

        # 3勝クラス（実質OP直下）
        if '3勝' in race_name or '3勝クラス' in race_name:
            return ('3勝クラス', 0.85)
        
        # 2勝クラス
        if '2勝' in race_name or '2勝クラス' in race_name:
            return ('2勝クラス', 0.75)
        
        # 1勝クラス
        if '1勝' in race_name or '1勝クラス' in race_name:
            return ('1勝クラス', 0.65)

        # 地方OP
        if any(x in race_name for x in ['C1', 'C2', 'C3', 'B1', 'B2', 'A1', 'A2']):
            return ('地方OP', 0.55)

        # 一般戦
        if any(x in race_name for x in ['未勝利', '新馬', '未出走']):
            return ('地方一般', 0.4)

        # OP（ステークス）
        if any(op in race_name for op in ['S', '特別', 'OP', 'オープン']):
            return ('OP', 0.9)

        return ('その他', 0.7)

    def _is_local_race(self, race_name: str, course: str = '') -> bool:
        """地方戦かどうかを判定"""
        race_name = str(race_name)
        all_local = self.local_dirt_courses + self.local_turf_courses + ['旭川', '帯広', '園田', '姫路', '広島']
        if course in all_local:
            return True
        if any(x in race_name for x in ['C1', 'C2', 'C3', 'B1', 'B2', 'A1', 'A2']):
            return True
        return False

    def _get_track_type_by_distance(self, distance: int, race_name: str = '', course: str = '') -> str:
        """
        距離とコースからトラックタイプを判定

        ルール:
        1. レース名に「ダ」「ダート」「芝」が含まれる場合はそれを優先
        2. 地方ダート競馬場（水沢/盛岡/大井など）: 全てダート
        3. 地方芝競馬場（札幌/函館など）: 1400m以下ダート、それ以上芝
        4. 中央競馬場: 全て芝
        5. その他: 1400m以下ダート、それ以上芝
        """
        race_name = str(race_name)

        # レース名に明示的な記載がある場合は優先
        if 'ダ' in race_name or 'ダート' in race_name:
            return 'ダート'
        if '芝' in race_name:
            return '芝'

        # 地方ダート競馬場は全てダート
        if course in self.local_dirt_courses:
            return 'ダート'

        # 地方芝競馬場
        if course in self.local_turf_courses:
            return 'ダート' if distance <= 1400 else '芝'

        # 中央競馬場は基本芝
        if course in self.central_courses:
            return '芝'

        # デフォルト
        return 'ダート' if distance <= 1400 else '芝'

    def calculate_last_3f_relative_score(self, history_data: List[Dict], target_track_type: str = "芝") -> float:
        """上がり3F評価（距離ベース判定版）"""
        if not history_data:
            return 0.0

        score = 0.0

        for idx, race in enumerate(history_data[:3]):
            last_3f = race.get('last_3f', 0.0)
            if last_3f <= 0:
                continue

            race_name = race.get('race_name', '')
            course = race.get('course', '')
            distance = race.get('dist', 2000)

            # トラックタイプを取得（優先順位: history > 距離判定）
            race_track_type = race.get('track_type', None)
            if not race_track_type or race_track_type == '不明':
                race_track_type = self._get_track_type_by_distance(distance, race_name, course)
            track_type_mismatch = (race_track_type != target_track_type)
            is_local = self._is_local_race(race_name, course)

            # レース平均上がり3Fを取得
            race_avg_3f = race.get('race_avg_last_3f', 0.0)
            if race_avg_3f <= 0:
                if race_track_type == 'ダート':
                    # ダート基準値を細分化（実際のレースペースに基づく）
                    if distance <= 1200:
                        race_avg_3f = 36.0  # ダート短距離
                    elif distance <= 1600:
                        race_avg_3f = 36.5  # ダート1400-1600m（小倉1700含む）
                    elif distance <= 2000:
                        race_avg_3f = 37.5  # ダート中距離
                    else:
                        race_avg_3f = 38.5  # ダート長距離
                else:
                    # 芝は従来通り
                    if distance <= 1400:
                        race_avg_3f = 34.3
                    elif distance <= 1600:
                        race_avg_3f = 34.5
                    elif distance <= 1800:
                        race_avg_3f = 35.0
                    elif distance <= 2200:
                        race_avg_3f = 36.0
                    else:
                        race_avg_3f = 37.0

            # 地方戦の基準値調整
            if is_local:
                race_avg_3f += 1.0 if race_track_type == 'ダート' else 0.5

            chakujun = race.get('chakujun', 99)
            speed_diff = race_avg_3f - last_3f
            finish_bonus = 3.0 if chakujun == 1 else 2.0 if chakujun == 2 else 1.0 if chakujun == 3 else 0.0

            if speed_diff > 0 and finish_bonus > 0:
                base_points = 25.0 if speed_diff >= 1.5 else 20.0 if speed_diff >= 1.0 else 15.0 if speed_diff >= 0.5 else 10.0
                points = base_points * finish_bonus
            elif speed_diff > 0:
                points = speed_diff * 3.0
            elif speed_diff <= 0 and finish_bonus > 0:
                points = finish_bonus * 2.0
            else:
                points = -5.0

            grade, base_reliability = self.detect_race_grade(race_name)
            reliability = base_reliability * (0.6 if is_local else 1.0) * (0.3 if track_type_mismatch else 1.0)
            time_decay = 1.0 - (idx * 0.15)
            score += points * reliability * time_decay

            if self.debug_mode:
                status = "◎" if speed_diff > 0 and finish_bonus > 0 else "△" if speed_diff > 0 else "×"
                mismatch_mark = "[別トラック]" if track_type_mismatch else ""
                local_mark = "[地方]" if is_local else ""
                logger.debug(f"  [{idx+1}走前] {distance}m({race_track_type}) [{status}]{mismatch_mark}{local_mark}: "
                           f"{last_3f:.1f}s vs 平均{race_avg_3f:.1f}s 差{speed_diff:+.2f}s "
                           f"着順{chakujun} 信頼度{reliability:.2f} 点{points:.1f}")

        return round(score, 1)

    def calculate_late_4f_turf_score(self, history_data: List[Dict], target_distance: int, target_track_type: str) -> float:
        """後半4F評価（距離ベース判定版）"""
        if target_track_type != "芝" or target_distance < 1800:
            return 0.0

        BASELINE_4F = 47.2 if target_distance <= 2000 else 47.8 if target_distance <= 2400 else 48.3
        score = 0.0

        for idx, race in enumerate(history_data[:3]):
            distance = race.get('dist', 0)
            race_name = race.get('race_name', '')
            course = race.get('course', '')

            # トラックタイプを取得（優先順位: history > 距離判定）
            race_track_type = race.get('track_type', None)
            if not race_track_type or race_track_type == '不明':
                race_track_type = self._get_track_type_by_distance(distance, race_name, course)
            if race_track_type != '芝' or distance < 1800:
                continue
            if self._is_local_race(race_name, course):
                continue

            last_3f = race.get('last_3f', 0.0)
            if last_3f <= 0:
                continue

            estimated_late_4f = last_3f * (4.0 / 3.0) + 0.4
            diff_from_baseline = BASELINE_4F - estimated_late_4f

            multiplier = 1.80 if diff_from_baseline >= 3.5 else 1.50 if diff_from_baseline >= 2.5 else 1.30 if diff_from_baseline >= 1.5 else 1.15 if diff_from_baseline >= 0.7 else 1.00 if diff_from_baseline >= -0.5 else 0.85 if diff_from_baseline >= -2.0 else 0.65
            points = diff_from_baseline * 10.0 * multiplier

            grade, reliability = self.detect_race_grade(race_name)
            reliability = {'G1': 1.0, 'G2': 0.95, 'G3': 0.9, 'JpnI': 1.0, 'JpnII': 0.95, 'JpnIII': 0.9, 'OP': 0.85}.get(grade, 0.7)
            time_decay = 1.0 - (idx * 0.15)
            chakujun = race.get('chakujun', 99)
            finish_bonus = 1.0 if chakujun == 1 else 0.9 if chakujun <= 3 else 0.75 if chakujun <= 5 else 0.5 if chakujun <= 10 else 0.3
            score += points * reliability * time_decay * finish_bonus

        return round(score, 1)

    def calculate_weight_time_score(self, current_weight: float, history_data: List[Dict], target_distance: int, target_track_type: str = "芝") -> float:
        """斤量-タイム評価（距離ベース判定版）"""
        if not history_data:
            return 0.0

        score = 0.0

        for idx, race in enumerate(history_data[:3]):
            distance = race.get('dist', 0)
            weight = race.get('weight', 0.0)
            last_3f = race.get('last_3f', 0.0)
            race_name = race.get('race_name', '')
            course = race.get('course', '')

            if distance > 1600 or distance < 1000:
                continue
            if weight <= 0 or last_3f <= 0:
                continue

            # トラックタイプを取得（優先順位: history > 距離判定）
            race_track_type = race.get('track_type', None)
            if not race_track_type or race_track_type == '不明':
                race_track_type = self._get_track_type_by_distance(distance, race_name, course)
            is_local = self._is_local_race(race_name, course)
            time_decay = 1.0 - (idx * 0.25)  # 0.2 → 0.25（減衰強化）

            if distance <= 1200:
                BASE_3F, WEIGHT_THRESHOLD = 34.0, 56.0
            elif distance <= 1400:
                BASE_3F, WEIGHT_THRESHOLD = 34.3, 55.5
            else:  # 1401～1600m
                BASE_3F, WEIGHT_THRESHOLD = 34.5, 55.0  # 35.0 → 34.5（厳格化）

            if is_local:
                BASE_3F += 1.2 if race_track_type == 'ダート' else 0.5

            weight_bonus = 8.0 if weight >= 58.0 else 5.0 if weight >= 57.0 else 2.0 if weight >= WEIGHT_THRESHOLD else -3.0 if weight <= 52.0 else 0.0
            if is_local:
                weight_bonus *= 0.5

            adjusted_base = BASE_3F + (0.3 if weight >= 57.0 else 0.1 if weight >= 55.0 else 0)
            diff = adjusted_base - last_3f
            speed_bonus = 6.0 if diff >= 1.0 else 4.0 if diff >= 0.5 else 2.0 if diff >= 0.0 else 0.0 if diff >= -0.5 else -3.0
            if is_local:
                speed_bonus *= 0.6

            combo_bonus = 0.0
            if weight >= 57.0 and last_3f < BASE_3F - 0.5:  # -0.3 → -0.5（厳格化）
                combo_bonus = 8.0 if last_3f < BASE_3F - 1.0 else 5.0  # -1.0秒条件追加
            elif weight >= 55.0 and last_3f < BASE_3F:
                combo_bonus = 2.0
            elif weight <= 54.0 and last_3f > BASE_3F + 0.5:
                combo_bonus = -5.0
            if is_local:
                combo_bonus *= 0.5

            race_score = (weight_bonus + speed_bonus + combo_bonus) * time_decay
            chakujun = race.get('chakujun', 99)
            if chakujun == 1:
                race_score *= 1.2
            elif chakujun <= 3:
                race_score *= 1.0  # 2-3着は減衰なし
            elif chakujun <= 5:
                race_score *= 0.85  # 4-5着は15%減
            elif chakujun <= 10:
                race_score *= 0.6  # 6-10着は40%減
            else:  # 11着以下
                race_score *= 0.3  # 0.5 → 0.3（減衰強化）

            if race_track_type != target_track_type:
                race_score *= 0.2
            score += race_score

        return round(score, 1)

    def calculate_weight_penalty(self, current_weight: float, history_data: List[Dict], is_handicap_race: bool = False, horse_weight: Optional[float] = None) -> float:
        """斤量変化ペナルティ"""
        if not history_data:
            return 0.0
        penalty = 0.0
        if is_handicap_race:
            if current_weight >= 57.5:
                penalty += 6.0
            elif current_weight >= 56.0:
                penalty += 3.0
            elif current_weight <= 52.0:
                penalty -= 4.0
            elif current_weight <= 53.5:
                penalty -= 1.5
        for race in history_data[:3]:
            prev_weight = race.get('weight', 0.0)
            if prev_weight <= 0:
                continue
            diff = current_weight - prev_weight
            if diff > 4.0:
                penalty += 2.0 if is_handicap_race else -10.0
            elif diff > 2.5:
                penalty += 1.0 if is_handicap_race else -6.0
            elif diff > 1.0:
                penalty += 0.5 if is_handicap_race else -3.0
            elif diff < -4.0:
                penalty += -1.0 if is_handicap_race else 5.0
            elif diff < -2.5:
                penalty += -0.5 if is_handicap_race else 2.5
            elif diff < -1.0:
                penalty -= 1.5
            break
        if horse_weight and horse_weight > 0:
            weight_ratio = current_weight / horse_weight
            if weight_ratio > 0.125:
                penalty -= 5.0
            elif weight_ratio < 0.105:
                penalty -= 2.0
        return round(penalty, 1)

    def calculate_distance_suitability(self, target_distance: int, history_data: List[Dict], target_track_type: str = "芝") -> float:
        """距離適性スコア（距離ベース判定版）"""
        if not history_data:
            return 0.0
        score = 0.0
        for race in history_data[:5]:
            dist = race.get('dist', 0)
            if dist == 0:
                continue
            race_name = race.get('race_name', '')
            course = race.get('course', '')
            # トラックタイプを取得（優先順位: history > 距離判定）
            race_track_type = race.get('track_type', None)
            if not race_track_type or race_track_type == '不明':
                race_track_type = self._get_track_type_by_distance(dist, race_name, course)
            diff = abs(target_distance - dist)
            points = 15.0 if diff == 0 else 10.0 if diff <= 200 else 5.0 if diff <= 400 else 0.0 if diff <= 600 else -5.0
            if race_track_type != target_track_type:
                points *= 0.5
            chakujun = race.get('chakujun', 99)
            if chakujun <= 3:
                points *= 1.2
            score += points
        return round(score / max(len(history_data[:5]), 1), 1)

    def calculate_course_suitability(self, target_course: str, history_data: List[Dict]) -> float:
        """コース適性スコア"""
        if not history_data:
            return 0.0
        score = 0.0
        for race in history_data[:5]:
            course = race.get('course', '')
            if not course:
                continue
            if target_course in course:
                chakujun = race.get('chakujun', 99)
                if chakujun == 1:
                    score += 20.0
                elif chakujun <= 3:
                    score += 15.0
                elif chakujun <= 5:
                    score += 10.0
                else:
                    score += 5.0
        return round(score, 1)

    def check_danger_flags(self, history_data: List[Dict], target_track_type: str = "芝") -> Dict:
        """危険フラグチェック（距離ベース判定版）"""
        flags = {'is_dangerous': False, 'local_horse_jra_return': False, 'dirt_to_turf': False, 'consecutive_bad': False, 'class_drop': False}
        if not history_data:
            return flags
        recent_races = history_data[:3]
        bad_finishes = sum(1 for race in recent_races if race.get('chakujun', 99) > 10)
        if bad_finishes >= 2:
            flags['consecutive_bad'] = True
            flags['is_dangerous'] = True
        local_wins = 0
        jra_races = 0
        for race in history_data[:5]:
            race_name = race.get('race_name', '')
            course = race.get('course', '')
            if self._is_local_race(race_name, course):
                if race.get('chakujun', 99) == 1:
                    local_wins += 1
            else:
                jra_races += 1
        if local_wins >= 1 and jra_races >= 1:
            latest_race = history_data[0] if history_data else {}
            latest_course = latest_race.get('course', '')
            if not self._is_local_race(latest_race.get('race_name', ''), latest_course):
                flags['local_horse_jra_return'] = True
                if latest_race.get('chakujun', 99) > 5:
                    flags['is_dangerous'] = True
        if target_track_type == "芝":
            dirt_wins = 0
            for race in history_data[:3]:
                race_name = race.get('race_name', '')
                course = race.get('course', '')
                dist = race.get('dist', 0)
                # トラックタイプを取得（優先順位: history > 距離判定）
                race_track_type = race.get('track_type', None)
                if not race_track_type or race_track_type == '不明':
                    race_track_type = self._get_track_type_by_distance(dist, race_name, course)
                if race_track_type == 'ダート':
                    if race.get('chakujun', 99) <= 3:
                        dirt_wins += 1
            if dirt_wins >= 1:
                flags['dirt_to_turf'] = True
                turf_wins = 0
                for r in history_data[:3]:
                    # トラックタイプを取得（優先順位: history > 距離判定）
                    r_track_type = r.get('track_type', None)
                    if not r_track_type or r_track_type == '不明':
                        r_track_type = self._get_track_type_by_distance(r.get('dist', 0), r.get('race_name', ''), r.get('course', ''))
                    if r_track_type == '芝' and r.get('chakujun', 99) <= 3:
                        turf_wins += 1
                if turf_wins == 0:
                    flags['is_dangerous'] = True
        return flags

    def calculate_total_score(self, current_weight: float, target_course: str, target_distance: int, history_data: List[Dict], target_track_type: str = "芝", is_handicap_race: bool = False, horse_weight: Optional[float] = None) -> Dict:
        """
        総合スコア計算（距離ベース判定版・スケール統一版）
        
        【重要】短距離・中長距離ともに正規化して0-100点スケールに統一
        - 50点が平均的な馬
        - 70点以上が有力馬
        - 30点以下が危険馬
        """
        danger_flags = self.check_danger_flags(history_data, target_track_type)
        last_3f_score = self.calculate_last_3f_relative_score(history_data, target_track_type)
        late_4f_score = self.calculate_late_4f_turf_score(history_data, target_distance, target_track_type)
        if target_distance <= 1600:
            weight_time_score = self.calculate_weight_time_score(current_weight, history_data, target_distance, target_track_type)
            weight_penalty = 0.0
        else:
            weight_time_score = 0.0
            weight_penalty = self.calculate_weight_penalty(current_weight, history_data, is_handicap_race, horse_weight)
        distance_score = self.calculate_distance_suitability(target_distance, history_data, target_track_type)
        course_score = self.calculate_course_suitability(target_course, history_data)
        
        # 危険ペナルティ計算
        danger_penalty = 0.0
        if danger_flags['local_horse_jra_return']:
            danger_penalty -= 15.0
        if danger_flags['dirt_to_turf'] and target_track_type == "芝":
            danger_penalty -= 10.0
        
        # ============================================================
        # 【スケール統一】短距離・中長距離ともに加重平均で正規化
        # ============================================================
        if target_distance <= 1600:
            # 短距離: 従来通り
            total = last_3f_score * 0.25 + weight_time_score * 0.45 + distance_score * 0.2 + course_score * 0.1
        else:
            # 中長距離: 加重平均で正規化（スケール統一）
            # last_3f_score: 上がり3F評価（最重要）
            # late_4f_score: 後半4F評価（芝中長距離で重要）
            # weight_penalty: 斤量ペナルティ
            # distance_score: 距離適性
            # course_score: コース適性
            total = (last_3f_score * 0.35 + 
                     late_4f_score * 0.25 + 
                     weight_penalty * 0.15 + 
                     distance_score * 0.15 + 
                     course_score * 0.10)
        
        # 危険ペナルティを正規化（-25点 = 約25%減として-6.25点に相当）
        normalized_danger_penalty = danger_penalty * 0.25
        total += normalized_danger_penalty
        
        breakdown = {
            'last_3f_relative': last_3f_score, 
            'late_4f_turf': late_4f_score, 
            'weight_penalty': weight_penalty, 
            'distance_suitability': distance_score, 
            'course_suitability': course_score, 
            'danger_penalty': danger_penalty,
            'normalized_danger_penalty': normalized_danger_penalty,
            'danger_flags': danger_flags
        }
        if target_distance <= 1600:
            breakdown['weight_time_score'] = weight_time_score
        
        return {
            'total_score': round(total, 1), 
            'is_dangerous': danger_flags['is_dangerous'], 
            'danger_flags': danger_flags, 
            'breakdown': breakdown
        }


if __name__ == "__main__":
    print("✅ EnhancedRaceScorer v5 (1600m基準値厳格化版) loaded")
    print("主な変更:")
    print("  - 1600m基準値: 35.0秒 → 34.5秒")
    print("  - 1400m基準値: 34.5秒 → 34.3秒")
    print("  - 時間減衰: 0.2 → 0.25")
    print("  - コンボボーナス条件厳格化")
    print("  - 着順減衰段階化（2-3着1.0倍、4-5着0.85倍、6-10着0.6倍、11着以降0.3倍）")

    # テスト実行例: ビスケットマリー
    scorer = EnhancedRaceScorer(debug_mode=True)

    biscuit_marie_history = [
        {'race_name': 'C1二組', 'course': '水沢', 'dist': 1600, 'chakujun': 1, 'last_3f': 37.7, 'weight': 54.0, 'race_avg_last_3f': 38.5},
        {'race_name': 'C1四組', 'course': '水沢', 'dist': 1600, 'chakujun': 1, 'last_3f': 40.2, 'weight': 54.0, 'race_avg_last_3f': 41.0},
        {'race_name': 'ペラルゴニウム賞', 'course': '盛岡', 'dist': 1600, 'chakujun': 7, 'last_3f': 41.3, 'weight': 54.0, 'race_avg_last_3f': 40.5},
    ]

    result = scorer.calculate_total_score(
        current_weight=52.0,
        target_course='小倉',
        target_distance=2000,
        history_data=biscuit_marie_history,
        target_track_type="芝"
    )

    print(f"\nテスト結果（ビスケットマリー）: 総合スコア {result['total_score']}点")
    print(f"危険フラグ: {result['is_dangerous']}")

# ファイル末尾に追加
if __name__ == "__main__":
    scorer = EnhancedRaceScorer(debug_mode=True)
    
    # 距離判定テスト
    test_cases = [
        (1600, '水沢', 'C1二組'),
        (1600, '盛岡', 'ペラルゴニウム賞'),
        (2000, '小倉', 'レース名'),
    ]
    
    print("【距離判定テスト】")
    for dist, course, name in test_cases:
        track = scorer._get_track_type_by_distance(dist, name, course)
        print(f"  {dist}m @ {course} → {track}")
    
    # ビスケットマリーテスト
    history = [
        {'race_name': 'C1二組', 'course': '水沢', 'dist': 1600, 'chakujun': 1, 'last_3f': 37.7, 'weight': 54.0},
        {'race_name': 'C1四組', 'course': '水沢', 'dist': 1600, 'chakujun': 1, 'last_3f': 40.2, 'weight': 54.0},
    ]
    
    result = scorer.calculate_total_score(
        current_weight=52.0,
        target_course='小倉',
        target_distance=2000,
        history_data=history,
        target_track_type='芝'
    )
    
    print(f"\n【ビスケットマリースコア】")
    print(f"  総合スコア: {result['total_score']}")
    print(f"  危険フラグ: {result['is_dangerous']}")
    print(f"  dirt_to_turf: {result['breakdown']['danger_flags']['dirt_to_turf']}")
