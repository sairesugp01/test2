"""
競馬予想AI - enhanced_scorer_fixed_v4.py（v8統合版・最終調整版）
最終更新: 2026年2月10日

主な機能（v4 + v8統合 + 上がり3F評価改善）:
1. v4の基本ロジック（1600m基準値厳格化、トラックタイプ判定など）を維持
2. v8のペース予測ロジックを統合
   - 出走頭数を考慮した動的判定
   - 逃げ馬の質（信頼度）を重視
   - 前残り率でペースを判定（18頭: 40%超→ハイ、25%未満→スロー）
3. v8のコース分析機能を統合
   - 内外回り判定
   - 馬場状態補正（良/稍重/重/不良）
   - コースタイプ別の上がり3F基準値
4. v8の脚質分析機能を統合
   - 通過順位から脚質を自動判定
   - 脚質×展開×コース特性の適合度ボーナス（距離別ウェイト適用）
5. 【新】上がり3F相対評価の改善
   - 2.0秒圏内の馬の平均上がり3Fと比較
   - レースの実際の競争状況を反映
   - 【修正】地方戦の評価をさらに厳格化（信頼度0.4）
   - 【追加】1200m・1400mは1.3秒以内の基準を適用

v4からの継承:
- 距離とコース名からトラックタイプ（芝/ダート）を自動判定
- 地方戦（C1/C2など）の信頼度を0.4に引き下げ（厳格化）
- 地方ダートの基準値を+1.0秒厳しく（ペース遅延対策）
- ダート→芝転換時の評価を70%減（別トラック扱い）
- 地方馬JRA復帰時の危険フラグ追加（-15点減点）
"""

import logging
from typing import List, Dict, Optional, Tuple
from collections import Counter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CourseAnalyzer:
    """コース分析クラス（v8から統合）"""
    
    COURSE_TYPE_CLASSIFICATION = {
        '東京': 'long_straight',
        '新潟外': 'long_straight',
        '京都外': 'standard',
        '阪神外': 'standard',
        '中京': 'standard',
        '京都内': 'tight',
        '阪神内': 'tight',
        '新潟内': 'tight',
        '中山': 'hilly',
        '札幌': 'tight',
        '小倉': 'tight',
        '函館': 'tight',
        '福島': 'standard',
    }
    
    BABA_ADJUSTMENT = {
        'long_straight': {'良': 0.0, '稍重': 0.4, '重': 1.0, '不良': 1.8},
        'standard':      {'良': 0.0, '稍重': 0.5, '重': 1.2, '不良': 2.0},
        'hilly':         {'良': 0.0, '稍重': 0.6, '重': 1.5, '不良': 2.5},
        'tight':         {'良': 0.0, '稍重': 0.7, '重': 1.8, '不良': 3.0},
    }
    
    BASELINE_3F = {
        '東京': {
            1400: 33.3, 1600: 33.8, 1800: 34.5, 2000: 35.0, 2400: 35.5, 3400: 37.0
        },
        '京都外': {
            1400: 33.8, 1600: 34.3, 1800: 34.8, 2000: 35.2, 2200: 35.5, 2400: 36.0, 3000: 37.5
        },
        '京都内': {
            1200: 34.5, 1400: 35.0, 1600: 35.5, 1800: 36.0, 2000: 36.5
        },
        '阪神外': {
            1400: 33.5, 1600: 34.0, 1800: 34.3, 2000: 34.8, 2200: 35.2, 2400: 35.5, 3000: 37.0
        },
        '阪神内': {
            1200: 34.8, 1400: 35.2, 1600: 35.5, 1800: 36.0, 2000: 36.5
        },
        '新潟外': {
            1600: 33.0, 1800: 33.5, 2000: 33.8, 2200: 34.5, 2400: 35.0
        },
        '新潟内': {
            1000: 34.0, 1200: 34.5, 1400: 35.0, 1600: 35.5, 1800: 36.0
        },
        '中山': {
            1200: 35.0, 1600: 35.8, 1800: 36.2, 2000: 36.5, 2200: 37.0, 2500: 37.5
        },
        '中京': {
            1200: 34.3, 1400: 34.8, 1600: 35.2, 1800: 35.5, 2000: 36.0, 2200: 36.5
        },
        '札幌': {
            1200: 35.0, 1500: 35.5, 1800: 36.0, 2000: 36.5, 2600: 38.0
        },
        '小倉': {
            1200: 35.0, 1700: 35.8, 1800: 36.0, 2000: 36.5, 2600: 38.0
        },
        '函館': {
            1200: 35.2, 1800: 36.0, 2000: 36.5, 2600: 38.0
        },
        '福島': {
            1200: 34.8, 1700: 35.5, 1800: 35.8, 2000: 36.2, 2600: 37.5
        },
    }
    
    @staticmethod
    def detect_track_variant(course: str, distance: int, distance_text: str = '') -> str:
        """内外回りを判定"""
        if '外' in distance_text or '外回り' in distance_text:
            return f'{course}外'
        elif '内' in distance_text or '内回り' in distance_text:
            return f'{course}内'
        
        if course == '京都':
            if distance <= 1400:
                return '京都内'
            elif distance >= 2200:
                return '京都外'
            else:
                return '京都外'
        
        elif course == '阪神':
            if distance <= 1400:
                return '阪神内'
            elif distance >= 2200:
                return '阪神外'
            else:
                return '阪神外'
        
        elif course == '新潟':
            if distance <= 1600:
                return '新潟内'
            elif distance >= 1800:
                return '新潟外'
            else:
                return '新潟内'
        
        return course
    
    @staticmethod
    def get_baseline_3f(course: str, distance: int, distance_text: str = '', 
                        baba: str = '良') -> float:
        """上がり3Fの基準値を取得（馬場状態補正込み）"""
        detailed_course = CourseAnalyzer.detect_track_variant(course, distance, distance_text)
        
        course_baselines = CourseAnalyzer.BASELINE_3F.get(detailed_course, {})
        
        if distance in course_baselines:
            baseline = course_baselines[distance]
        else:
            distances = sorted(course_baselines.keys())
            if not distances:
                baseline = 34.5 if distance <= 1800 else 35.5 if distance <= 2200 else 36.5
            else:
                closest = min(distances, key=lambda x: abs(x - distance))
                baseline = course_baselines[closest]
                diff = distance - closest
                baseline += diff * 0.001
        
        course_type = CourseAnalyzer.COURSE_TYPE_CLASSIFICATION.get(detailed_course, 'standard')
        baba_offset = CourseAnalyzer.BABA_ADJUSTMENT.get(course_type, {}).get(baba, 0.0)
        
        return round(baseline + baba_offset, 1)


class RunningStyleAnalyzer:
    """脚質分析クラス（v8から統合・ペース予測改善版）"""
    
    COURSE_CHARACTERISTICS = {
        '東京': {'straight': 525, 'favor': ['差し', '追込']},
        '京都': {'straight': 403, 'favor': ['先行', '差し']},
        '阪神': {'straight': 356, 'favor': ['先行']},
        '中山': {'straight': 310, 'favor': ['逃げ', '先行']},
        '新潟': {'straight': 659, 'favor': ['差し', '追込']},
        '小倉': {'straight': 293, 'favor': ['逃げ', '先行']},
        '福島': {'straight': 292, 'favor': ['逃げ', '先行']},
        '函館': {'straight': 262, 'favor': ['先行']},
        '札幌': {'straight': 266, 'favor': ['先行']},
        '中京': {'straight': 412, 'favor': ['差し']},
    }
    
    @staticmethod
    def classify_running_style(passing_positions: List[int], field_sizes: List[int] = None) -> Dict:
        """通過順位履歴から脚質を判定"""
        if not passing_positions:
            return {'style': '不明', 'confidence': 0.0, 'avg_position': 0.0, 'avg_position_rate': 0.0}
        
        avg_pos = sum(passing_positions) / len(passing_positions)
        
        if field_sizes and len(field_sizes) == len(passing_positions):
            position_rates = [pos / size for pos, size in zip(passing_positions, field_sizes)]
            avg_rate = sum(position_rates) / len(position_rates)
        else:
            avg_rate = avg_pos / 16.0
        
        variance = sum((p - avg_pos) ** 2 for p in passing_positions) / len(passing_positions)
        std_dev = variance ** 0.5
        
        data_confidence = min(len(passing_positions) / 5.0, 1.0)
        stability_confidence = max(0.5, 1.0 - (std_dev / 5.0))
        confidence = data_confidence * stability_confidence
        
        if avg_rate <= 0.20 or avg_pos <= 3.0:
            style = '逃げ'
        elif avg_rate <= 0.45 or avg_pos <= 7.0:
            style = '先行'
        elif avg_rate <= 0.75 or avg_pos <= 12.0:
            style = '差し'
        else:
            style = '追込'
        
        return {
            'style': style,
            'confidence': round(confidence, 2),
            'avg_position': round(avg_pos, 1),
            'avg_position_rate': round(avg_rate, 2),
            'races_analyzed': len(passing_positions)
        }
    
    @staticmethod
    def predict_race_pace(horses_running_styles: List[Dict], field_size: int = None) -> Dict:
        """
        【改善版】レース展開を予測（v8から統合）
        
        改善点:
        1. 出走頭数（field_size）を考慮した動的判定
        2. 逃げ馬の「質」（信頼度）を重視
        3. 前残り率（逃げ+先行の割合）でペースを判定
        
        Args:
            horses_running_styles: 各馬の脚質情報リスト
                                   [{'style': '逃げ', 'confidence': 0.85}, ...]
            field_size: 出走頭数（省略時は馬数から自動計算）
        
        Returns:
            {'pace': 'ハイ'/'ミドル'/'スロー', 'front_runners': int, ...}
        """
        if not horses_running_styles:
            return {'pace': 'ミドル', 'front_runners': 0, 'closers': 0, 
                    'expected_tempo': '平均的', 'front_ratio': 0.0, 'field_size': 0,
                    'high_confidence_escapes': 0, 
                    'distribution': {'逃げ': 0, '先行': 0, '差し': 0, '追込': 0}}
        
        if field_size is None:
            field_size = len(horses_running_styles)
        
        escape_count = 0
        front_count = 0
        mid_count = 0
        close_count = 0
        high_confidence_escapes = 0
        
        for horse in horses_running_styles:
            style = horse.get('style', '不明')
            confidence = horse.get('confidence', 0.0)
            
            if style == '逃げ':
                escape_count += 1
                if confidence >= 0.7:
                    high_confidence_escapes += 1
            elif style == '先行':
                front_count += 1
            elif style == '差し':
                mid_count += 1
            elif style == '追込':
                close_count += 1
        
        front_runners = escape_count + front_count
        closers = mid_count + close_count
        
        front_ratio = front_runners / field_size if field_size > 0 else 0.0
        
        if field_size >= 16:
            high_threshold = 0.40
            low_threshold = 0.25
        elif field_size >= 12:
            high_threshold = 0.45
            low_threshold = 0.30
        else:
            high_threshold = 0.50
            low_threshold = 0.35
        
        if high_confidence_escapes >= 2:
            pace = 'ハイ'
            tempo = '速いペース、前崩れリスク'
        elif escape_count >= 2 and front_ratio > high_threshold:
            pace = 'ハイ'
            tempo = '逃げ争い、前崩れリスク'
        elif front_ratio > high_threshold:
            pace = 'ハイ'
            tempo = '前に馬が多く、スピード勝負'
        elif escape_count == 0 and front_ratio < low_threshold:
            pace = 'スロー'
            tempo = '逃げ不在、瞬発力勝負'
        else:
            pace = 'ミドル'
            tempo = '平均的なペース、総合力勝負'
        
        return {
            'pace': pace,
            'front_runners': front_runners,
            'closers': closers,
            'expected_tempo': tempo,
            'front_ratio': round(front_ratio, 2),
            'field_size': field_size,
            'high_confidence_escapes': high_confidence_escapes,
            'distribution': {
                '逃げ': escape_count,
                '先行': front_count,
                '差し': mid_count,
                '追込': close_count
            }
        }
    
    @staticmethod
    def calculate_style_match_bonus(horse_style: str, race_pace: str, 
                                    course_name: str, distance: int) -> float:
        """脚質×展開×コース特性の適合度ボーナス（v8から統合）"""
        bonus = 0.0
        
        pace_bonus = {
            'ハイ': {'逃げ': -4.0, '先行': -2.5, '差し': +6.0, '追込': +7.0},
            'ミドル': {'逃げ': +4.0, '先行': +5.0, '差し': +4.0, '追込': +2.5},
            'スロー': {'逃げ': +7.0, '先行': +6.0, '差し': -2.5, '追込': -5.0}
        }
        bonus += pace_bonus.get(race_pace, {}).get(horse_style, 0.0)
        
        course_info = RunningStyleAnalyzer.COURSE_CHARACTERISTICS.get(course_name, {})
        favored_styles = course_info.get('favor', [])
        
        if horse_style in favored_styles:
            bonus += 2.0
        
        if distance <= 1400:
            if horse_style in ['逃げ', '先行']:
                bonus += 1.0
        elif distance >= 2400:
            if horse_style in ['差し', '追込']:
                bonus += 1.0
        
        return round(bonus, 1)


class EnhancedRaceScorer:
    """強化版レーススコアラー v4 + v8統合版"""

    def __init__(self, debug_mode: bool = False):
        self.debug_mode = debug_mode
        # v8から統合したアナライザー
        self.style_analyzer = RunningStyleAnalyzer()
        self.course_analyzer = CourseAnalyzer()
        # v4の既存データ
        self.local_dirt_courses = ['大井', '浦和', '船橋', '川崎', '金沢', '笠松', '名古屋', '高知', '佐賀', '水沢', '盛岡']
        self.local_turf_courses = []
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

    def _get_default_baseline_3f(self, distance: int, track_type: str) -> float:
        """距離とトラックタイプからデフォルトの上がり3F基準値を取得"""
        if track_type == 'ダート':
            if distance <= 1200:
                return 36.0
            elif distance <= 1600:
                return 36.5
            elif distance <= 2000:
                return 37.5
            else:
                return 38.5
        else:
            if distance <= 1400:
                return 34.3
            elif distance <= 1600:
                return 34.5
            elif distance <= 1800:
                return 35.0
            elif distance <= 2200:
                return 36.0
            else:
                return 37.0

    def calculate_last_3f_relative_score(self, history_data: List[Dict], target_track_type: str = "芝") -> float:
        """
        【改善版】上がり3F相対評価（2.0秒圏内の馬の平均と比較・地方戦厳格化・短距離1.3秒基準）
        
        変更点:
        - 2.0秒圏内の馬（±2.0秒）の平均上がり3Fと比較
        - 地方戦の信頼度を0.4に引き下げ（厳格化）
        - 1200m・1400mは1.3秒以内の基準を追加（短距離の特殊性を考慮）
        """
        if not history_data:
            return 0.0

        score = 0.0
        THRESHOLD = 2.0  # 2.0秒圏内

        for idx, race in enumerate(history_data[:3]):
            my_last_3f = race.get('last_3f', 0.0)
            if my_last_3f <= 0:
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

            # 【新】2.0秒圏内の馬の平均上がり3Fを計算
            all_horses_results = race.get('all_horses_results', [])
            
            if not all_horses_results:
                # フォールバック：従来のレース平均または固定値
                race_avg_3f = race.get('race_avg_last_3f', 0.0)
                if race_avg_3f <= 0:
                    race_avg_3f = self._get_default_baseline_3f(distance, race_track_type)
                    if is_local:
                        race_avg_3f += 1.0 if race_track_type == 'ダート' else 0.5
                comparison_type = "レース平均/固定値"
            else:
                # 2.0秒圏内の馬を抽出
                nearby_horses_3f = []
                for horse_result in all_horses_results:
                    horse_3f = horse_result.get('last_3f', 0.0)
                    goal_diff = horse_result.get('goal_time_diff', 99)
                    
                    if abs(goal_diff) <= THRESHOLD and horse_3f > 0:
                        nearby_horses_3f.append(horse_3f)
                
                if nearby_horses_3f:
                    race_avg_3f = sum(nearby_horses_3f) / len(nearby_horses_3f)
                    comparison_type = f"{THRESHOLD}秒圏内{len(nearby_horses_3f)}頭平均"
                else:
                    # 圏内に馬がいない場合は全体平均
                    valid_3f = [h['last_3f'] for h in all_horses_results if h.get('last_3f', 0) > 0]
                    race_avg_3f = sum(valid_3f) / len(valid_3f) if valid_3f else self._get_default_baseline_3f(distance, race_track_type)
                    comparison_type = "レース全体平均（圏内なし）"

            chakujun = race.get('chakujun', 99)
            speed_diff = race_avg_3f - my_last_3f
            
            # 【追加】1200m・1400mは1.3秒以内の基準を適用
            if distance <= 1400:
                # 短距離は1.3秒以内の基準で評価（より厳格に）
                if speed_diff >= 1.3:
                    base_points = 25.0
                elif speed_diff >= 0.8:
                    base_points = 20.0
                elif speed_diff >= 0.4:
                    base_points = 15.0
                elif speed_diff >= 0.0:
                    base_points = 10.0
                else:
                    base_points = -5.0
            else:
                # 従来の基準（1600m以上）
                if speed_diff >= 1.5:
                    base_points = 25.0
                elif speed_diff >= 1.0:
                    base_points = 20.0
                elif speed_diff >= 0.5:
                    base_points = 15.0
                elif speed_diff >= 0.0:
                    base_points = 10.0
                else:
                    base_points = -5.0
            
            finish_bonus = 3.0 if chakujun == 1 else 2.0 if chakujun == 2 else 1.0 if chakujun == 3 else 0.0

            if speed_diff > 0 and finish_bonus > 0:
                points = base_points * finish_bonus
            elif speed_diff > 0:
                points = speed_diff * 3.0
            elif speed_diff <= 0 and finish_bonus > 0:
                points = finish_bonus * 2.0
            else:
                points = -5.0

            grade, base_reliability = self.detect_race_grade(race_name)
            # 【修正】地方戦の信頼度を0.4に引き下げ（厳格化）
            reliability = base_reliability * (0.4 if is_local else 1.0) * (0.3 if track_type_mismatch else 1.0)
            time_decay = 1.0 - (idx * 0.15)
            score += points * reliability * time_decay

            if self.debug_mode:
                status = "◎" if speed_diff > 0 and finish_bonus > 0 else "△" if speed_diff > 0 else "×"
                mismatch_mark = "[別トラック]" if track_type_mismatch else ""
                local_mark = "[地方]" if is_local else ""
                short_mark = "[短距離1.3s基準]" if distance <= 1400 else ""
                logger.debug(f"  [{idx+1}走前] {distance}m({race_track_type}) [{status}]{mismatch_mark}{local_mark}{short_mark}: "
                           f"{comparison_type} 基準{race_avg_3f:.2f}s vs 自身{my_last_3f:.2f}s 差{speed_diff:+.2f}s "
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

    def format_score_breakdown(self, result: Dict, target_distance: int) -> str:
        """
        スコア内訳を見やすくフォーマット
        
        Args:
            result: calculate_total_scoreの戻り値
            target_distance: 対象距離
        
        Returns:
            フォーマット済みの文字列
        """
        breakdown = result['breakdown']
        is_short = target_distance <= 1600
        
        lines = ["【スコア内訳】"]
        
        if is_short:
            # 短距離（≤1600m）
            lines.append(f"  上がり3F相対評価:   {breakdown['last_3f_relative']:.1f} (×0.25 = {breakdown['last_3f_relative'] * 0.25:.1f})")
            lines.append(f"  斤量-タイム評価:   {breakdown.get('weight_time_score', 0):.1f} (×0.45 = {breakdown.get('weight_time_score', 0) * 0.45:.1f}) ←短距離特化")
            lines.append(f"  距離適性:          {breakdown['distance_suitability']:.1f} (×0.20 = {breakdown['distance_suitability'] * 0.20:.1f})")
            lines.append(f"  コース適性:        {breakdown['course_suitability']:.1f} (×0.10 = {breakdown['course_suitability'] * 0.10:.1f})")
        else:
            # 中長距離（>1600m）
            lines.append(f"  上がり3F相対評価:   {breakdown['last_3f_relative']:.1f} (×0.35 = {breakdown['last_3f_relative'] * 0.35:.1f})")
            lines.append(f"  後半4F評価:        {breakdown['late_4f_turf']:.1f} (×0.25 = {breakdown['late_4f_turf'] * 0.25:.1f}) ←中長距離特化")
            lines.append(f"  斤量ペナルティ:    {breakdown['weight_penalty']:.1f} (×0.15 = {breakdown['weight_penalty'] * 0.15:.1f})")
            lines.append(f"  距離適性:          {breakdown['distance_suitability']:.1f} (×0.15 = {breakdown['distance_suitability'] * 0.15:.1f})")
            lines.append(f"  コース適性:        {breakdown['course_suitability']:.1f} (×0.10 = {breakdown['course_suitability'] * 0.10:.1f})")
        
        # 共通項目
        if breakdown.get('running_style_bonus', 0) != 0:
            lines.append(f"  脚質×展開ボーナス:  {breakdown['running_style_bonus']:+.1f} (距離別ウェイト適用)")
        
        if breakdown.get('normalized_danger_penalty', 0) != 0:
            lines.append(f"  危険ペナルティ:    {breakdown['normalized_danger_penalty']:+.1f} (元: {breakdown['danger_penalty']:.1f})")
        
        lines.append("  ────────────────────")
        lines.append(f"  総合スコア:        {result['total_score']:.1f}")
        
        if result['is_dangerous']:
            lines.append("  ⚠️ 危険フラグあり")
            if breakdown['danger_flags'].get('local_horse_jra_return'):
                lines.append("    - 地方馬JRA復帰")
            if breakdown['danger_flags'].get('dirt_to_turf'):
                lines.append("    - ダート→芝転換")
            if breakdown['danger_flags'].get('consecutive_bad'):
                lines.append("    - 連続不振")
        
        return "\n".join(lines)

    def calculate_total_score(self, current_weight: float, target_course: str, target_distance: int, history_data: List[Dict], target_track_type: str = "芝", is_handicap_race: bool = False, horse_weight: Optional[float] = None, running_style_info: Optional[Dict] = None, race_pace_prediction: Optional[Dict] = None) -> Dict:
        """
        総合スコア計算（v4 + v8統合版・最終調整版）
        
        【重要】短距離・中長距離ともに正規化して0-100点スケールに統一
        - 50点が平均的な馬
        - 70点以上が有力馬
        - 30点以下が危険馬
        
        【v8統合】脚質×展開のボーナスを追加（距離別ウェイト適用）
        - running_style_info: 馬の脚質情報 {'style': '逃げ', 'confidence': 0.85}
        - race_pace_prediction: レース展開予測 {'pace': 'ハイ', ...}
        
        【改善】上がり3F評価を2.0秒圏内の馬の平均と比較する方式に変更
        【修正】地方戦の信頼度を0.4に引き下げ（厳格化）
        【追加】1200m・1400mは1.3秒以内の基準を適用
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
        
        # 【v8統合】脚質×展開ボーナス計算（距離別ウェイト適用）
        running_style_bonus = 0.0
        if running_style_info and race_pace_prediction:
            horse_style = running_style_info.get('style', '不明')
            confidence = running_style_info.get('confidence', 0.0)
            pace = race_pace_prediction.get('pace', 'ミドル')
            
            if confidence >= 0.5 and horse_style != '不明':
                raw_bonus = self.style_analyzer.calculate_style_match_bonus(
                    horse_style, pace, target_course, target_distance
                )
                
                # 【推奨設定】距離別ウェイト
                if target_distance <= 1600:
                    style_weight = 0.20  # 短距離：ペースが命
                elif target_distance <= 2200:
                    style_weight = 0.10  # 中距離：バランス重視
                else:
                    style_weight = 0.05  # 長距離：スタミナ重視
                
                running_style_bonus = raw_bonus * confidence * style_weight
        
        # ============================================================
        # 【スケール統一】短距離・中長距離ともに加重平均で正規化
        # ============================================================
        if target_distance <= 1600:
            # 短距離: 従来通り
            total = last_3f_score * 0.25 + weight_time_score * 0.45 + distance_score * 0.2 + course_score * 0.1
        else:
            # 中長距離: 加重平均で正規化（スケール統一）
            total = (last_3f_score * 0.35 + 
                     late_4f_score * 0.25 + 
                     weight_penalty * 0.15 + 
                     distance_score * 0.15 + 
                     course_score * 0.10)
        
        # 危険ペナルティを正規化（-25点 = 約25%減として-6.25点に相当）
        normalized_danger_penalty = danger_penalty * 0.25
        total += normalized_danger_penalty
        
        # 【v8統合】脚質ボーナスを加算（距離別ウェイト済み）
        total += running_style_bonus
        
        breakdown = {
            'last_3f_relative': last_3f_score, 
            'late_4f_turf': late_4f_score, 
            'weight_penalty': weight_penalty, 
            'distance_suitability': distance_score, 
            'course_suitability': course_score, 
            'danger_penalty': danger_penalty,
            'normalized_danger_penalty': normalized_danger_penalty,
            'running_style_bonus': running_style_bonus,
            'danger_flags': danger_flags,
            # 計算式の内訳を追加
            'calculation_detail': {
                'is_short_distance': target_distance <= 1600,
                'weights': {
                    'last_3f': 0.25 if target_distance <= 1600 else 0.35,
                    'weight_time': 0.45 if target_distance <= 1600 else 0.0,
                    'late_4f': 0.0 if target_distance <= 1600 else 0.25,
                    'weight_penalty': 0.0 if target_distance <= 1600 else 0.15,
                    'distance': 0.2 if target_distance <= 1600 else 0.15,
                    'course': 0.1 if target_distance <= 1600 else 0.10,
                    'running_style': 0.20 if target_distance <= 1600 else 0.10 if target_distance <= 2200 else 0.05
                }
            }
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
    print("✅ EnhancedRaceScorer v4 + v8統合版（最終調整版）loaded")
    print("主な機能:")
    print("  【v4の基本ロジック】")
    print("  - 1600m基準値: 34.5秒（厳格化）")
    print("  - 1400m基準値: 34.3秒")
    print("  - トラックタイプ判定（historyデータ優先）")
    print("  - 地方戦の信頼度引き下げ（0.4に厳格化）")
    print()
    print("  【v8統合機能】")
    print("  - ペース予測改善（出走頭数・逃げ馬の質を考慮）")
    print("  - コース分析（内外回り判定・馬場状態補正）")
    print("  - 脚質分析（通過順位から自動判定）")
    print("  - 脚質×展開×コース特性の適合度ボーナス（距離別ウェイト適用）")
    print()
    print("  【新機能】")
    print("  - 上がり3F評価: 2.0秒圏内の馬の平均と比較")
    print("  - 地方戦信頼度: 0.4に引き下げ（厳格化）")
    print("  - 短距離1.3秒基準: 1200m・1400mは1.3秒以内で高評価")
    print("  - 脚質ボーナス: 短距離20%、中距離10%、長距離5%のウェイト")
    print()
    
    # テスト実行
    scorer = EnhancedRaceScorer(debug_mode=True)
    
    # ペース予測テスト
    print("【ペース予測テスト】")
    test_horses = [
        {'style': '逃げ', 'confidence': 0.85},
        {'style': '先行', 'confidence': 0.75},
        {'style': '先行', 'confidence': 0.70},
        {'style': '差し', 'confidence': 0.80},
        {'style': '差し', 'confidence': 0.75},
    ]
    pace_pred = scorer.style_analyzer.predict_race_pace(test_horses, field_size=18)
    print(f"  18頭立て、逃げ1頭・先行2頭: {pace_pred['pace']}ペース")
    print(f"  前残り率: {pace_pred['front_ratio']}")
    print()
    
    # コース分析テスト
    print("【コース分析テスト】")
    baseline_3f = scorer.course_analyzer.get_baseline_3f('東京', 1600, '芝1600', '良')
    print(f"  東京芝1600m・良馬場の上がり3F基準値: {baseline_3f}秒")
    baseline_3f_heavy = scorer.course_analyzer.get_baseline_3f('中山', 2000, '芝2000', '重')
    print(f"  中山芝2000m・重馬場の上がり3F基準値: {baseline_3f_heavy}秒")
    print()
    
    # 脚質ボーナステスト（距離別ウェイト確認）
    print("【脚質ボーナステスト（距離別ウェイト）】")
    
    # 短距離（1400m）
    bonus_short = scorer.style_analyzer.calculate_style_match_bonus('差し', 'ハイ', '東京', 1400)
    weighted_short = bonus_short * 0.8 * 0.20  # confidence 0.8 × 短距離ウェイト0.20
    print(f"  差し馬×ハイペース×東京1400m: 生ボーナス{bonus_short:+.1f} → 実適用{weighted_short:+.2f}（短距離20%）")
    
    # 中距離（2000m）
    bonus_mid = scorer.style_analyzer.calculate_style_match_bonus('差し', 'ハイ', '東京', 2000)
    weighted_mid = bonus_mid * 0.8 * 0.10  # confidence 0.8 × 中距離ウェイト0.10
    print(f"  差し馬×ハイペース×東京2000m: 生ボーナス{bonus_mid:+.1f} → 実適用{weighted_mid:+.2f}（中距離10%）")
    
    # 長距離（3000m）
    bonus_long = scorer.style_analyzer.calculate_style_match_bonus('差し', 'ハイ', '京都', 3000)
    weighted_long = bonus_long * 0.8 * 0.05  # confidence 0.8 × 長距離ウェイト0.05
    print(f"  差し馬×ハイペース×京都3000m: 生ボーナス{bonus_long:+.1f} → 実適用{weighted_long:+.2f}（長距離5%）")
    print()
    
    # 距離判定テスト
    print("【トラックタイプ判定テスト】")
    test_cases = [
        (1600, '水沢', 'C1二組'),
        (1600, '盛岡', 'ペラルゴニウム賞'),
        (2000, '小倉', 'レース名'),
    ]
    for dist, course, name in test_cases:
        track = scorer._get_track_type_by_distance(dist, name, course)
        print(f"  {dist}m @ {course} → {track}")
    print()
    
    # 【重要】ウインドファルクステスト（地方馬・短距離・1.3秒基準適用）
    print("【ウインドファルクステスト - 地方馬・短距離・1.3秒基準】")
    wind_falx_history = [
        {
            'race_name': '最後までおトクなSP', 
            'course': '水沢', 
            'dist': 1200, 
            'chakujun': 1, 
            'last_3f': 38.1, 
            'weight': 57.0, 
            'race_avg_last_3f': 38.8,
            'all_horses_results': [
                {'last_3f': 38.1, 'goal_time_diff': 0.0},
                {'last_3f': 38.3, 'goal_time_diff': 0.2},
                {'last_3f': 38.5, 'goal_time_diff': 0.4},
                {'last_3f': 38.8, 'goal_time_diff': 0.7},
            ]
        },
        {
            'race_name': 'グランシャリオドリー', 
            'course': '水沢', 
            'dist': 1200, 
            'chakujun': 1, 
            'last_3f': 38.0, 
            'weight': 57.0, 
            'race_avg_last_3f': 38.6,
            'all_horses_results': [
                {'last_3f': 38.0, 'goal_time_diff': 0.0},
                {'last_3f': 38.2, 'goal_time_diff': 0.2},
                {'last_3f': 38.4, 'goal_time_diff': 0.4},
                {'last_3f': 38.6, 'goal_time_diff': 0.6},
            ]
        },
        {
            'race_name': 'ホクレンSS特別', 
            'course': '水沢', 
            'dist': 1200, 
            'chakujun': 2, 
            'last_3f': 38.8, 
            'weight': 57.0, 
            'race_avg_last_3f': 39.5,
            'all_horses_results': [
                {'last_3f': 38.5, 'goal_time_diff': -0.3},
                {'last_3f': 38.8, 'goal_time_diff': 0.0},
                {'last_3f': 39.0, 'goal_time_diff': 0.2},
                {'last_3f': 39.5, 'goal_time_diff': 0.7},
            ]
        },
        {
            'race_name': '3歳以上C3ー2C4', 
            'course': '水沢', 
            'dist': 1200, 
            'chakujun': 1, 
            'last_3f': 37.6, 
            'weight': 57.0, 
            'race_avg_last_3f': 38.0,
            'all_horses_results': [
                {'last_3f': 37.6, 'goal_time_diff': 0.0},
                {'last_3f': 37.9, 'goal_time_diff': 0.3},
                {'last_3f': 38.0, 'goal_time_diff': 0.4},
            ]
        },
        {
            'race_name': '農業大事じゃあーりま', 
            'course': '水沢', 
            'dist': 1200, 
            'chakujun': 3, 
            'last_3f': 38.8, 
            'weight': 57.0, 
            'race_avg_last_3f': 39.6,
            'all_horses_results': [
                {'last_3f': 38.5, 'goal_time_diff': -0.3},
                {'last_3f': 38.6, 'goal_time_diff': -0.2},
                {'last_3f': 38.8, 'goal_time_diff': 0.0},
                {'last_3f': 39.6, 'goal_time_diff': 0.8},
            ]
        },
    ]
    
    result = scorer.calculate_total_score(
        current_weight=58.0,  # 斤量増
        target_course='小倉',
        target_distance=1200,  # 短距離（1.3秒基準適用）
        history_data=wind_falx_history,
        target_track_type='芝',
        running_style_info={'style': '先行', 'confidence': 0.75},
        race_pace_prediction={'pace': 'ミドル', 'front_ratio': 0.35}
    )
    
    print(scorer.format_score_breakdown(result, 1200))
    print(f"\n  ※ 地方戦信頼度0.4、短距離1.3秒基準適用、斤量57→58kg増")
    print()
    
    # 短距離の例も表示
    print("【短距離スコアテスト例（JRA馬）】")
    short_history = [
        {
            'race_name': '3歳未勝利', 
            'course': '東京', 
            'dist': 1400, 
            'chakujun': 2, 
            'last_3f': 33.5, 
            'weight': 54.0, 
            'race_avg_last_3f': 34.0,
            'all_horses_results': [
                {'last_3f': 33.2, 'goal_time_diff': -0.3},
                {'last_3f': 33.5, 'goal_time_diff': 0.0},
                {'last_3f': 33.8, 'goal_time_diff': 0.3},
                {'last_3f': 34.0, 'goal_time_diff': 0.5},
            ]
        },
        {
            'race_name': '3歳新馬', 
            'course': '中山', 
            'dist': 1200, 
            'chakujun': 1, 
            'last_3f': 34.2, 
            'weight': 54.0, 
            'race_avg_last_3f': 35.0,
            'all_horses_results': [
                {'last_3f': 34.2, 'goal_time_diff': 0.0},
                {'last_3f': 34.5, 'goal_time_diff': 0.3},
                {'last_3f': 34.8, 'goal_time_diff': 0.6},
            ]
        },
    ]
    
    short_result = scorer.calculate_total_score(
        current_weight=55.0,
        target_course='東京',
        target_distance=1400,
        history_data=short_history,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.80},
        race_pace_prediction={'pace': 'ハイ'}
    )
    
    print(scorer.format_score_breakdown(short_result, 1400))
    print(f"\n  ※ JRA戦信頼度1.0、短距離1.3秒基準適用")
