"""
競馬予想AI - enhanced_scorer_v7.py（阪神内外修正＋新馬ボーナス削減版）
最終更新: 2026年2月22日

V6からの改善点:
12. 阪神2200mを内回りに修正
   - 旧: distance >= 2200 → 阪神外（誤り）
   - 新: distance == 2200 → 阪神内（正しい内回りコース）
   - 阪神3000m等は引き続き阪神外

13. 新馬戦2戦目ブーストの過大評価を削減
   - ベースボーナス: 1着+5→+3、2着+2.5→+1.5、3着+1→+0.5
   - 上がり上位ボーナス: +5→+2点
   - 後方差しボーナス: +3→+1.5点
   - 最大合計: +13→+6.5点
   - 背景: 新馬1戦のみの馬がG1実績馬より高く評価される逆転現象を防止
"""

import logging
from typing import List, Dict, Optional, Tuple
from collections import Counter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CourseAnalyzer:
    """コース分析クラス"""

    # コースレコード一覧（芝・秒換算）
    # キー: (コース名, 距離)  ※コース名はdetect_track_variant後の詳細名
    COURSE_RECORDS: dict = {
        # 東京
        ('東京',   1400): 79.8,
        ('東京',   1600): 90.5,
        ('東京',   1800): 103.5,
        ('東京',   2000): 116.1,
        ('東京',   2400): 141.2,
        ('東京',   3400): 208.4,
        # 中山
        ('中山',   1200): 67.3,
        ('中山',   1600): 91.7,
        ('中山',   1800): 106.1,
        ('中山',   2000): 118.0,
        ('中山',   2200): 131.0,
        ('中山',   2500): 150.7,
        # 阪神外
        ('阪神外', 1600): 91.3,
        ('阪神外', 1800): 103.2,
        ('阪神外', 2000): 115.5,
        ('阪神外', 2400): 140.9,
        # 阪神内
        ('阪神内', 1200): 67.4,
        ('阪神内', 1400): 79.3,
        ('阪神内', 1800): 106.0,
        ('阪神内', 2000): 118.0,
        ('阪神内', 2200): 131.5,
        # 京都外
        ('京都外', 1600): 90.5,
        ('京都外', 1800): 103.5,
        ('京都外', 2000): 116.8,
        ('京都外', 2200): 130.5,
        ('京都外', 2400): 143.5,
        ('京都外', 3000): 179.0,
        ('京都外', 3200): 192.5,
        # 京都内
        ('京都内', 1200): 68.5,
        ('京都内', 1400): 81.0,
        ('京都内', 1600): 92.5,
        ('京都内', 1800): 105.5,
        ('京都内', 2000): 118.5,
        # 新潟外
        ('新潟外', 1600): 90.5,
        ('新潟外', 1800): 104.5,
        ('新潟外', 2000): 117.5,
        ('新潟外', 2200): 130.5,
        ('新潟外', 2400): 144.0,
        # 新潟内
        ('新潟内', 1000): 55.5,
        ('新潟内', 1200): 67.5,
        ('新潟内', 1400): 81.5,
        ('新潟内', 2000): 118.5,
        # 中京
        ('中京',   1200): 68.3,
        ('中京',   1400): 81.5,
        ('中京',   1600): 93.5,
        ('中京',   2000): 119.3,
        # 小倉
        ('小倉',   1200): 67.9,
        ('小倉',   1800): 106.5,
        ('小倉',   2000): 119.5,
        ('小倉',   2600): 159.5,
        # 福島
        ('福島',   1200): 68.0,
        ('福島',   1700): 101.8,
        ('福島',   1800): 107.0,
        ('福島',   2000): 119.8,
        ('福島',   2600): 160.0,
        # 札幌
        ('札幌',   1200): 68.6,
        ('札幌',   1500): 89.0,
        ('札幌',   1800): 107.2,
        ('札幌',   2000): 120.0,
        ('札幌',   2600): 160.5,
        # 函館
        ('函館',   1200): 68.3,
        ('函館',   1800): 107.5,
        ('函館',   2000): 120.2,
        ('函館',   2600): 161.0,
    }

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
            # 外回りコース（「外」表記 + 2200・2400・3000・3200は外回り固定）
            1400: 33.8, 1600: 34.3, 1800: 34.8, 2000: 35.2, 2200: 35.5, 2400: 36.0, 3000: 37.5, 3200: 37.8
        },
        '京都内': {
            # 内回りコース（「外」表記なし・2000m以下）
            1200: 34.5, 1400: 35.0, 1600: 35.5, 1800: 36.0, 2000: 36.5
        },
        '阪神外': {
            # 外回りコース: 1600, 1800, 2000, 2400（1400外は実質なし・2200は内回り・3000mは存在しない）
            1600: 34.0, 1800: 34.3, 2000: 34.8, 2400: 35.5
        },
        '阪神内': {
            # 内回りコース: 1200, 1400, 1800, 2000, 2200（阪神大賞典）
            1200: 34.8, 1400: 35.2, 1800: 36.0, 2000: 36.5, 2200: 36.8
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
            # 外回り固定距離: 2200, 2400, 3000, 3200（内回りコースが存在しない）
            # 内回り固定距離: 1200
            # 1400・1600: 新馬・未勝利→内回り／1勝クラス以上→外回り
            #             netkeibaは外回りのみ「外」表記のため、distance_textで正しく判定される
            # 1800・2000: 内回りのみ存在
            if distance in [2200, 2400, 3000, 3200]:
                return '京都外'
            else:
                return '京都内'   # 1200〜2000は内回りデフォルト（外回りはdistance_textの「外」で判定済み）
        
        elif course == '阪神':
            # 内回り: 1200, 1400, 1800, 2000, 2200（阪神大賞典）
            # 外回り: 1600, 1800, 2000, 2400
            # ※1800・2000は内外両方存在するが、distance_textに情報がない場合は外回りをデフォルトとする
            if distance <= 1400:
                return '阪神内'
            elif distance == 2200:
                return '阪神内'   # 阪神大賞典（GII）は内回り
            elif distance == 2400:
                return '阪神外'   # 宝塚記念（GI）は外回り
            else:
                return '阪神外'   # 1600・1800・2000のデフォルトは外回り
        
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
    """脚質分析クラス（コース特性強化版）"""
    
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
    
    # 【新設】コース×距離別の脚質ボーナスウェイト
    COURSE_DISTANCE_STYLE_WEIGHTS = {
        '東京': {
            1400: {'逃げ': 0.08, '先行': 0.12, '差し': 0.20, '追込': 0.15},
            1600: {'逃げ': 0.05, '先行': 0.10, '差し': 0.20, '追込': 0.15},
            1800: {'逃げ': 0.03, '先行': 0.08, '差し': 0.15, '追込': 0.12},
            2000: {'逃げ': 0.03, '先行': 0.08, '差し': 0.15, '追込': 0.12},
            2400: {'逃げ': 0.02, '先行': 0.05, '差し': 0.10, '追込': 0.08},
        },
        '中山': {
            1200: {'逃げ': 0.20, '先行': 0.20, '差し': 0.10, '追込': 0.05},
            1600: {'逃げ': 0.15, '先行': 0.20, '差し': 0.10, '追込': 0.05},
            1800: {'逃げ': 0.12, '先行': 0.18, '差し': 0.12, '追込': 0.08},
            2000: {'逃げ': 0.12, '先行': 0.18, '差し': 0.12, '追込': 0.08},
            2500: {'逃げ': 0.08, '先行': 0.12, '差し': 0.10, '追込': 0.08},
        },
        '新潟': {
            1600: {'逃げ': 0.03, '先行': 0.08, '差し': 0.20, '追込': 0.18},
            1800: {'逃げ': 0.03, '先行': 0.08, '差し': 0.18, '追込': 0.15},
            2000: {'逃げ': 0.02, '先行': 0.05, '差し': 0.15, '追込': 0.12},
        },
        '京都': {
            1400: {'逃げ': 0.10, '先行': 0.15, '差し': 0.15, '追込': 0.10},
            1600: {'逃げ': 0.08, '先行': 0.15, '差し': 0.15, '追込': 0.10},
            1800: {'逃げ': 0.05, '先行': 0.12, '差し': 0.12, '追込': 0.08},
            2000: {'逃げ': 0.05, '先行': 0.12, '差し': 0.12, '追込': 0.08},
        },
        '阪神': {
            1400: {'逃げ': 0.12, '先行': 0.18, '差し': 0.12, '追込': 0.08},
            1600: {'逃げ': 0.10, '先行': 0.15, '差し': 0.12, '追込': 0.08},
            1800: {'逃げ': 0.08, '先行': 0.12, '差し': 0.10, '追込': 0.08},
            2000: {'逃げ': 0.08, '先行': 0.12, '差し': 0.10, '追込': 0.08},
        },
        '小倉': {
            1200: {'逃げ': 0.20, '先行': 0.20, '差し': 0.10, '追込': 0.05},
            1700: {'逃げ': 0.15, '先行': 0.18, '差し': 0.10, '追込': 0.05},
            1800: {'逃げ': 0.12, '先行': 0.15, '差し': 0.10, '追込': 0.05},
            2000: {'逃げ': 0.10, '先行': 0.12, '差し': 0.10, '追込': 0.08},
        },
        '中京': {
            1200: {'逃げ': 0.12, '先行': 0.15, '差し': 0.15, '追込': 0.10},
            1400: {'逃げ': 0.10, '先行': 0.12, '差し': 0.18, '追込': 0.12},
            1600: {'逃げ': 0.08, '先行': 0.10, '差し': 0.18, '追込': 0.12},
            1800: {'逃げ': 0.05, '先行': 0.08, '差し': 0.15, '追込': 0.10},
            2000: {'逃げ': 0.05, '先行': 0.08, '差し': 0.15, '追込': 0.10},
        },
        '福島': {
            1200: {'逃げ': 0.18, '先行': 0.18, '差し': 0.10, '追込': 0.05},
            1700: {'逃げ': 0.15, '先行': 0.18, '差し': 0.12, '追込': 0.08},
            1800: {'逃げ': 0.12, '先行': 0.15, '差し': 0.12, '追込': 0.08},
            2000: {'逃げ': 0.10, '先行': 0.12, '差し': 0.12, '追込': 0.08},
        },
        '札幌': {
            1200: {'逃げ': 0.15, '先行': 0.18, '差し': 0.12, '追込': 0.08},
            1500: {'逃げ': 0.12, '先行': 0.15, '差し': 0.12, '追込': 0.08},
            1800: {'逃げ': 0.10, '先行': 0.12, '差し': 0.12, '追込': 0.10},
            2000: {'逃げ': 0.08, '先行': 0.10, '差し': 0.12, '追込': 0.10},
        },
        '函館': {
            1200: {'逃げ': 0.15, '先行': 0.18, '差し': 0.10, '追込': 0.05},
            1800: {'逃げ': 0.10, '先行': 0.12, '差し': 0.12, '追込': 0.08},
            2000: {'逃げ': 0.08, '先行': 0.10, '差し': 0.12, '追込': 0.10},
        },
    }
    
    @staticmethod
    def classify_running_style(position_4c: int, field_size: int, last_3f: float, 
                               race_avg_3f: float = 0) -> Dict:
        """4角位置と上がり3Fから脚質を判定"""
        if position_4c <= 0 or field_size <= 0:
            return {'style': '不明', 'confidence': 0.0}
        
        position_ratio = position_4c / field_size
        
        if position_ratio <= 0.15:
            style = '逃げ'
            confidence = 0.90
        elif position_ratio <= 0.35:
            style = '先行'
            confidence = 0.85
        elif position_ratio <= 0.65:
            if race_avg_3f > 0 and last_3f < race_avg_3f - 0.3:
                style = '差し'
                confidence = 0.80
            else:
                style = '差し'
                confidence = 0.70
        else:
            if race_avg_3f > 0 and last_3f < race_avg_3f - 0.5:
                style = '追込'
                confidence = 0.85
            else:
                style = '追込'
                confidence = 0.75
        
        return {'style': style, 'confidence': confidence}
    
    @staticmethod
    def predict_race_pace(horses: List[Dict], field_size: int = 16, 
                         course: str = '東京') -> Dict:
        """レースのペースを予測（コース特性考慮）"""
        if not horses:
            return {'pace': 'ミドル', 'front_ratio': 0.30}
        
        style_counts = Counter(h.get('style', '不明') for h in horses if h.get('style') != '不明')
        front_runners = style_counts.get('逃げ', 0) + style_counts.get('先行', 0)
        
        # コース特性を取得
        course_info = RunningStyleAnalyzer.COURSE_CHARACTERISTICS.get(course, {})
        straight_length = course_info.get('straight', 400)
        
        # 直線の長さでペース判定基準を調整
        if straight_length >= 500:
            # 長い直線（東京、新潟）: 差し・追込が届きやすい
            if front_runners <= 2:
                pace = 'スロー'
                front_ratio = 0.30
            elif front_runners <= 4:
                pace = 'ミドル'
                front_ratio = 0.45
            else:
                pace = 'ハイ'
                front_ratio = 0.55
        elif straight_length <= 320:
            # 短い直線（中山、小倉、福島、函館、札幌）: 前残り重視
            if front_runners <= 1:
                pace = 'スロー'
                front_ratio = 0.45
            elif front_runners <= 3:
                pace = 'ミドル'
                front_ratio = 0.55
            else:
                pace = 'ハイ'
                front_ratio = 0.65
        else:
            # 標準的な直線（京都、阪神、中京）
            if front_runners <= 2:
                pace = 'スロー'
                front_ratio = 0.35
            elif front_runners <= 4:
                pace = 'ミドル'
                front_ratio = 0.50
            else:
                pace = 'ハイ'
                front_ratio = 0.60
        
        return {
            'pace': pace,
            'front_ratio': front_ratio,
            'straight_length': straight_length
        }
    
    @staticmethod
    def calculate_style_match_bonus(style: str, pace: str, course: str = '東京', 
                                   distance: int = 1600) -> float:
        """脚質×ペース×コースの相性ボーナス"""
        bonus_map = {
            'スロー': {'逃げ': 15.0, '先行': 12.0, '差し': 10.0, '追込': 8.0},
            'ミドル': {'逃げ': 10.0, '先行': 12.0, '差し': 12.0, '追込': 10.0},
            'ハイ': {'逃げ': 5.0, '先行': 8.0, '差し': 15.0, '追込': 18.0}
        }
        
        base_bonus = bonus_map.get(pace, {}).get(style, 0.0)
        
        # コース特性による補正
        course_info = RunningStyleAnalyzer.COURSE_CHARACTERISTICS.get(course, {})
        favored_styles = course_info.get('favor', [])
        
        if style in favored_styles:
            base_bonus *= 1.2
        
        return base_bonus
    
    @staticmethod
    def get_style_weight(course: str, distance: int, style: str) -> float:
        """コース×距離別の脚質ボーナスウェイトを取得"""
        course_weights = RunningStyleAnalyzer.COURSE_DISTANCE_STYLE_WEIGHTS.get(course, {})
        
        # 指定距離のウェイトを取得
        if distance in course_weights:
            return course_weights[distance].get(style, 1.0)
        
        # 最も近い距離のウェイトを使用
        distances = sorted(course_weights.keys())
        if not distances:
            return 1.0
        
        closest_distance = min(distances, key=lambda x: abs(x - distance))
        return course_weights[closest_distance].get(style, 1.0)


class RaceScorer:
    """レーススコアリングクラス（V6: 新馬戦2戦目ブースト追加版）"""
    
    def __init__(self, debug_mode: bool = False):
        self.debug_mode = debug_mode
        self.style_analyzer = RunningStyleAnalyzer()
        self.course_analyzer = CourseAnalyzer()
    
    def detect_race_grade(self, race_name: str):
        if not race_name:
            return "不明", 0.60

        name = race_name.upper()

        # 長い方から判定（GIII/JpnIIIが先、GI/JpnIより前）
        if "G3" in name or "GIII" in name or "JPNIII" in name:
            return "G3", 0.90
        elif "G2" in name or "GII" in name or "JPNII" in name:
            return "G2", 0.95
        elif "G1" in name or "GI" in name or "JPNI" in name:
            return "G1", 1.00
        elif "OP" in name or "オープン" in race_name or "（L）" in race_name or "(L)" in race_name:
            return "OP", 0.85
        elif "3勝クラス" in race_name or "1600万下" in race_name:
            return "3勝", 0.80
        elif "2勝クラス" in race_name or "1000万下" in race_name:
            return "2勝", 0.75
        elif "1勝クラス" in race_name or "500万下" in race_name:
            return "1勝", 0.70
        elif "未勝利" in race_name or "新馬" in race_name:
            return "未勝利", 0.65
        else:
            return "不明", 0.60

    
    def _is_local_race(self, race_name: str, course: str) -> bool:
        """地方競馬かどうか判定（交流重賞は除外）"""
        # 交流重賞は地方競馬場開催でも地方扱いしない
        exchange_race_keywords = ['JpnI', 'JpnII', 'JpnIII', 'Jpn1', 'Jpn2', 'Jpn3']
        if any(keyword in race_name for keyword in exchange_race_keywords):
            return False
        
        local_courses = ['大井', '川崎', '船橋', '浦和', '門別', '盛岡', '水沢', '金沢', '笠松', '名古屋', '園田', '姫路', '高知', '佐賀']
        return any(lc in course or lc in race_name for lc in local_courses)
    
    def _get_track_type_by_distance(self, distance: int, race_name: str, course: str) -> str:
        """距離からトラックタイプを推定"""
        if 'ダ' in race_name or 'ダート' in race_name:
            return 'ダート'
        elif '障' in race_name:
            return '障害'
        
        if distance <= 1200:
            return '芝' if 'ダ' not in course else 'ダート'
        elif distance <= 1600:
            return '芝'
        elif distance >= 3000:
            return '障害' if '障' in race_name else '芝'
        else:
            return '芝'
    
    def _get_default_baseline_3f(self, distance: int, track_type: str) -> float:
        """デフォルトの上がり3F基準値"""
        if track_type == 'ダート':
            if distance <= 1400:
                return 37.5
            elif distance <= 1800:
                return 38.0
            else:
                return 38.5
        else:
            if distance <= 1400:
                return 34.5
            elif distance <= 1800:
                return 35.0
            elif distance <= 2200:
                return 35.5
            else:
                return 36.5
    
    def _calculate_distance_score(self, history_data: List[Dict], target_distance: int) -> float:
        """
        距離適性スコア（重み付き合計版・直近5走評価）

        距離の近さ（基礎点）× 成績係数 × 時系列重み を正規化し最大15点にキャップ。
        直近ほど重要なため時系列重みを設定。

        基礎点（距離差・線形補間）:
          0m差    -> 15.0点
          100m差  -> 12.5点
          200m差  -> 10.0点  ← 200m以内は線形補間
          201-400m差 -> 10.0点（境界を自然につなぐ）
          401-600m差 ->  5.0点
          600m超     ->  0.0点

        時系列重み: 1走前:1.0 / 2走前:0.8 / 3走前:0.6 / 4走前:0.5 / 5走前:0.4

        成績係数: 着差0.3s以内->1.00 / 0.6s->0.85 / 1.0s->0.70
                  1.5s->0.50 / 2.5s->0.30 / 超->0.10

        最終スコア: 重み付き正規化後、最大15点キャップ
        """
        if not history_data:
            return 0.0

        TIME_WEIGHTS = [1.0, 0.8, 0.6, 0.5, 0.4]

        weighted_score = 0.0
        weighted_denom = 0.0

        for idx, race in enumerate(history_data[:5]):
            dist = race.get('dist', 0)
            if dist <= 0:
                continue

            time_w = TIME_WEIGHTS[idx]
            diff = abs(target_distance - dist)

            # 線形補間: 0m差→15点、200m差→10点、それ以外はステップ
            if diff <= 200:
                base_pts = 15.0 - (diff / 200.0) * 5.0   # 0m→15.0, 100m→12.5, 200m→10.0
            elif diff <= 400:
                base_pts = 10.0
            elif diff <= 600:
                base_pts = 5.0
            else:
                base_pts = 0.0

            chakujun  = race.get('chakujun', 99)
            time_diff = race.get('goal_time_diff', None)

            # 除外・中止・取消（99着または0着）はスキップ
            if chakujun == 0 or chakujun >= 90:
                continue

            if time_diff is not None and time_diff != 0:
                margin = abs(float(time_diff))
                if margin <= 0.3:
                    coef = 1.00
                elif margin <= 0.6:
                    coef = 0.85
                elif margin <= 1.0:
                    coef = 0.70
                elif margin <= 1.5:
                    coef = 0.50
                elif margin <= 2.5:
                    coef = 0.30
                else:
                    coef = 0.10
            else:
                if chakujun == 1:
                    coef = 1.00
                elif chakujun == 2:
                    coef = 0.85
                elif chakujun == 3:
                    coef = 0.70
                elif chakujun <= 5:
                    coef = 0.50
                elif chakujun <= 9:
                    coef = 0.30
                else:
                    coef = 0.10

            weighted_score += base_pts * coef * time_w
            weighted_denom += time_w

        if weighted_denom == 0.0:
            return 0.0

        raw = weighted_score / weighted_denom
        return round(min(raw, 15.0), 1)
    
    # JRA競馬場リスト（地方との区別用）
    JRA_COURSES  = ['札幌', '函館', '福島', '新潟', '東京', '中山', '中京', '京都', '阪神', '小倉']
    LOCAL_COURSES = ['大井', '川崎', '船橋', '浦和', '門別', '盛岡', '水沢', '金沢', '笠松', '名古屋', '園田', '姫路', '高知', '佐賀']

    def _is_jra_course(self, course: str) -> bool:
        return any(jra in course for jra in self.JRA_COURSES)

    def _is_local_course(self, course: str) -> bool:
        return any(lc in course for lc in self.LOCAL_COURSES)

    def _calculate_course_score(self, history_data: List[Dict], target_course: str,
                                target_track_type: str = None) -> float:
        """
        コース適性スコア（重み付き合計版・直近5走評価）

        基礎点（競馬場・トラック一致度）:
          同競馬場 × 同トラック           -> 15点
          同競馬場 × トラック違い（芝/ダ）-> 10点
          JRA別競馬場 × 同トラック        ->  5点
          地方別競馬場 × 同トラック        ->  3点
          JRA/地方の混在                 ->  0点

        時系列重み: 1走前:1.0 / 2走前:0.8 / 3走前:0.6 / 4走前:0.5 / 5走前:0.4

        最終スコア: 重み付き正規化後、最大15点キャップ
        """
        if not history_data:
            return 0.0

        TIME_WEIGHTS = [1.0, 0.8, 0.6, 0.5, 0.4]

        weighted_score = 0.0
        weighted_denom = 0.0

        for idx, race in enumerate(history_data[:5]):
            course     = race.get('course', '')
            track_type = race.get('track_type', '')
            chakujun   = race.get('chakujun', 99)
            time_diff  = race.get('goal_time_diff', None)

            # 除外・中止・取消（99着または0着）はスキップ
            if chakujun == 0 or chakujun >= 90:
                continue

            time_w = TIME_WEIGHTS[idx]

            same_course = (course == target_course)
            same_track  = (not target_track_type or not track_type
                           or track_type == target_track_type)

            # 同競馬場のみ点数付与。別競馬場は0点（中山≠東京）
            if same_course and same_track:
                base_pts = 15.0
            elif same_course and not same_track:
                base_pts = 8.0
            else:
                base_pts = 0.0

            if time_diff is not None and time_diff != 0:
                margin = abs(float(time_diff))
                if margin <= 0.3:
                    coef = 1.00
                elif margin <= 0.6:
                    coef = 0.85
                elif margin <= 1.0:
                    coef = 0.70
                elif margin <= 1.5:
                    coef = 0.50
                elif margin <= 2.5:
                    coef = 0.30
                else:
                    coef = 0.10
            else:
                if chakujun == 1:
                    coef = 1.00
                elif chakujun == 2:
                    coef = 0.85
                elif chakujun == 3:
                    coef = 0.70
                elif chakujun <= 5:
                    coef = 0.50
                elif chakujun <= 9:
                    coef = 0.30
                else:
                    coef = 0.10

            weighted_score += base_pts * coef * time_w
            weighted_denom += time_w

        if weighted_denom == 0.0:
            return 0.0

        raw = weighted_score / weighted_denom
        return round(min(raw, 15.0), 1)
    
    def _calculate_weight_penalty(self, current_weight: float, horse_age: int = None, 
                                  horse_sex: str = None) -> float:
        """
        斤量ペナルティ（基準斤量からの増減で計算）
        
        基準斤量:
        - 4歳以上 牡馬・セン馬: 58kg
        - 4歳以上 牝馬: 56kg
        - 3歳 牡馬・セン馬: 57kg
        - 3歳 牝馬: 55kg
        - 2歳 牡馬・牝馬: 55kg
        
        ペナルティ:
        - 基準より1kg増 → -1.0点
        - 基準より1kg減 → +1.0点
        """
        if current_weight <= 0:
            return 0.0
        
        # 基準斤量を決定
        if horse_age is not None and horse_sex is not None:
            if horse_age >= 4:
                # 4歳以上
                if horse_sex in ['牝', '牝馬', 'メス', 'F', 'f']:
                    baseline_weight = 56.0
                else:
                    # 牡馬・セン馬
                    baseline_weight = 58.0
            elif horse_age == 3:
                # 3歳
                if horse_sex in ['牝', '牝馬', 'メス', 'F', 'f']:
                    baseline_weight = 55.0
                else:
                    baseline_weight = 57.0
            elif horse_age == 2:
                # 2歳（牡馬・牝馬共に55kg）
                baseline_weight = 55.0
            else:
                # 1歳以下（通常ありえないが）
                baseline_weight = 55.0
        else:
            # 年齢・性別不明の場合はデフォルト（4歳以上牡馬）
            baseline_weight = 58.0
        
        # 基準斤量との差分
        weight_diff = current_weight - baseline_weight
        
        # 1kgあたり1点の増減（増えるとマイナス、減るとプラス）
        penalty = -weight_diff
        
        if self.debug_mode:
            logger.debug(f"  斤量評価: 現在{current_weight:.1f}kg vs 基準{baseline_weight:.1f}kg → 差分{weight_diff:+.1f}kg → {penalty:+.1f}点")
        
        return round(penalty, 1)
    
    def _calculate_layoff_penalty(self, history_data: List[Dict]) -> float:
        """長期休養ペナルティ"""
        if not history_data:
            return 0.0
        
        from datetime import datetime
        
        last_race = history_data[0]
        race_date_str = last_race.get('race_date', '')
        
        if not race_date_str:
            return 0.0
        
        try:
            race_date = datetime.strptime(race_date_str, '%Y/%m/%d')
            current_date = datetime.now()
            days_since = (current_date - race_date).days
            months_since = days_since / 30.44
            
            if days_since <= 120:  # 4ヶ月未満（= 3ヶ月以内）はペナルティなし
                penalty = 0.0
            elif months_since <= 4:
                penalty = -4.0
            elif months_since <= 5:
                penalty = -6.0
            elif months_since <= 6:
                penalty = -8.0
            elif months_since <= 7:
                penalty = -10.0
            elif months_since <= 8:
                penalty = -11.0
            elif months_since <= 9:
                penalty = -12.0
            elif months_since <= 10:
                penalty = -14.0
            elif months_since <= 11:
                penalty = -16.0
            else:
                penalty = -20.0
            
            if self.debug_mode and penalty < 0:
                logger.debug(f"  長期休養ペナルティ: {months_since:.1f}ヶ月ぶり → {penalty:.1f}点")
            
            return penalty
            
        except Exception as e:
            if self.debug_mode:
                logger.debug(f"  日付解析エラー: {e}")
            return 0.0
    
    def _calculate_grade_race_bonus(self, history_data: List[Dict]) -> float:
        """重賞出走ボーナス"""
        bonus = 0.0
        
        for idx, race in enumerate(history_data[:5]):
            race_name = race.get('race_name', '')
            chakujun = race.get('chakujun', 99)

            # 除外・中止・取消はスキップ（出走できていないため重賞ボーナス対象外）
            if chakujun == 0 or chakujun >= 90:
                continue

            grade, _ = self.detect_race_grade(race_name)
            
            if grade not in ['G1', 'G2', 'G3']:
                continue
            
            # グレード別のボーナス
            if grade == 'G1':
                if chakujun == 1:
                    race_bonus = 10.0
                elif chakujun in [2, 3]:
                    race_bonus = 8.0
                else:
                    race_bonus = 5.0
            elif grade == 'G2':
                if chakujun == 1:
                    race_bonus = 7.0
                elif chakujun in [2, 3]:
                    race_bonus = 5.0
                else:
                    race_bonus = 3.0
            elif grade == 'G3':
                if chakujun == 1:
                    race_bonus = 5.0
                elif chakujun in [2, 3]:
                    race_bonus = 3.0
                else:
                    race_bonus = 2.0
            else:
                race_bonus = 0.0
            
            time_decay = 1.0 - (idx * 0.15)
            bonus += race_bonus * time_decay
            
            if self.debug_mode:
                logger.debug(f"  重賞出走ボーナス: {race_name} {grade} {chakujun}着 → +{race_bonus * time_decay:.1f}点")
        
        return round(bonus, 1)
    
    
    def count_consecutive_big_losses(self, history: List[Dict], threshold: float = 1.1) -> int:
        """
        連続大敗回数をカウント
        
        Args:
            history: レース履歴
            threshold: 大敗判定の着差閾値（秒）
        
        Returns:
            連続大敗回数
        """
        count = 0
        for race in history:
            diff = race.get("goal_time_diff")
            if diff is not None and diff >= threshold:
                count += 1
            else:
                break
        return count
    
    def _should_reduce_big_loss_penalty(self, history_data: List[Dict], loss_index: int) -> bool:
        """
        大敗ペナルティを軽減すべきか判定
        
        軽減条件:
        1. 距離の大幅変更（600m以上）
        2. 中止 → 大敗（実質1走扱い）
        3. 休養明け初戦の大敗
        
        Args:
            history_data: レース履歴
            loss_index: 大敗レースのインデックス
        
        Returns:
            True: ペナルティ軽減, False: 通常ペナルティ
        """
        if loss_index >= len(history_data):
            return False
        
        loss_race = history_data[loss_index]
        loss_distance = loss_race.get('dist', 0)
        
        # 1. 距離の大幅変更（600m以上）
        if loss_index + 1 < len(history_data):
            prev_race = history_data[loss_index + 1]
            prev_distance = prev_race.get('dist', 0)
            
            if abs(loss_distance - prev_distance) >= 600:
                if self.debug_mode:
                    logger.debug(f"    大敗ペナルティ軽減: 距離変更 {prev_distance}m → {loss_distance}m")
                return True
        
        # 2. 中止 → 大敗（スクレイパーで中止はスキップされるが、手動データ等の互換のため残す）
        if loss_index + 1 < len(history_data):
            prev_race = history_data[loss_index + 1]
            prev_chakujun = prev_race.get('chakujun', 0)
            
            # 着順0=中止/除外/取消
            if prev_chakujun == 0:
                if self.debug_mode:
                    logger.debug(f"    大敗ペナルティ軽減: 前走中止/除外/取消")
                return True
        
        # 3. 休養明け初戦の大敗
        if loss_index + 1 < len(history_data):
            from datetime import datetime
            
            loss_date_str = loss_race.get('race_date', '')
            prev_race = history_data[loss_index + 1]
            prev_date_str = prev_race.get('race_date', '')
            
            if loss_date_str and prev_date_str:
                try:
                    loss_date = datetime.strptime(loss_date_str, '%Y/%m/%d')
                    prev_date = datetime.strptime(prev_date_str, '%Y/%m/%d')
                    days_since = (loss_date - prev_date).days
                    
                    # 4ヶ月以上の休養明け
                    if days_since >= 120:
                        if self.debug_mode:
                            logger.debug(f"    大敗ペナルティ軽減: 休養明け初戦（{days_since}日ぶり）")
                        return True
                except Exception:
                    pass
        
        return False
    
    def _calculate_consecutive_big_loss_penalty(self, history_data: List[Dict]) -> float:
        """
        連続大敗ペナルティの計算
        
        基準:
        - 1回: -3点（軽微）
        - 2連続: -8点（危険）
        - 3連続以上: -15点（原則消し）
        
        ペナルティ軽減条件:
        - 距離の大幅変更（600m以上）
        - 中止 → 大敗
        - 休養明け初戦の大敗
        """
        if not history_data:
            return 0.0
        
        # 連続大敗回数をカウント
        consecutive_losses = self.count_consecutive_big_losses(history_data, threshold=1.1)
        
        if consecutive_losses == 0:
            return 0.0
        
        # 軽減条件をチェック
        # 最新の大敗から順にチェックし、軽減対象が見つかったらそこで連続をリセット
        reduced_count = 0
        for i in range(consecutive_losses):
            should_reduce = self._should_reduce_big_loss_penalty(history_data, i)
            
            if not should_reduce:
                reduced_count += 1
            else:
                # 軽減対象の大敗が見つかったら、ここで連続をリセット
                if self.debug_mode:
                    logger.debug(f"    大敗{i+1}回目（{i+1}走前）: 軽減対象のため、ここで連続リセット")
                break
        
        # 実質的な連続大敗回数でペナルティを計算
        if reduced_count == 0:
            penalty = 0.0
        elif reduced_count == 1:
            penalty = -3.0
        elif reduced_count == 2:
            penalty = -8.0
        else:
            penalty = -15.0
        
        if self.debug_mode and penalty < 0:
            logger.debug(f"  連続大敗ペナルティ: {consecutive_losses}回連続 → 実質{reduced_count}回 → {penalty:.1f}点")
        
        return penalty

    def _calculate_winning_streak_bonus(self, history_data: List[Dict]) -> float:
        """
        【新】連勝着差ボーナスの計算

        直近3走の「1着かつ着差」を時系列重み付きで累積評価。
        楽勝（0.5s以上）が続くほど高得点。上限+10点。

        着差スコア（1走ごと）:
          1着 かつ winner_margin >= 0.5s → +4.0点（楽勝）
          1着 かつ winner_margin >= 0.2s → +2.5点（明確な差）
          1着 かつ winner_margin <  0.2s → +1.5点（接戦 or データなし）
          1着以外                        →  ループ打ち切り（連勝終了）

        時系列重み: 1走前×1.0 / 2走前×0.7 / 3走前×0.5
        上限: +10.0点
        """
        if not history_data:
            return 0.0

        TIME_WEIGHTS = [1.0, 0.7, 0.5]
        bonus = 0.0

        for idx, race in enumerate(history_data[:3]):
            chakujun = race.get('chakujun', 99)
            if chakujun != 1:
                break

            # scraper_v6以降は winner_margin に勝ち着差が入る
            margin = race.get('winner_margin', 0.0)
            if margin == 0.0:
                margin = abs(float(race.get('goal_time_diff', 0.0)))

            if margin >= 0.5:
                pts = 4.0
            elif margin >= 0.2:
                pts = 2.5
            else:
                pts = 1.5

            w = TIME_WEIGHTS[idx]
            bonus += pts * w

            if self.debug_mode:
                label = "楽勝" if margin >= 0.5 else "明確差" if margin >= 0.2 else "接戦"
                logger.debug(
                    f"  連勝ボーナス [{idx+1}走前] 1着 着差{margin:.2f}s ({label}) "
                    f"→ {pts:.1f}×{w:.1f} = {pts*w:.2f}点"
                )

        result = round(min(bonus, 10.0), 1)
        if self.debug_mode and result > 0:
            logger.debug(f"  連勝着差ボーナス 合計: +{result}点（上限10点）")
        return result

    def _calculate_shinba_second_race_boost(self, history_data: List[Dict]) -> float:
        """
        【新】新馬戦2戦目ブーストの計算
        
        条件:
        - 総走数が1回（新馬戦のみ）
        - 最初のレースが「新馬」
        
        ブースト内容:
        - ベースボーナス（着順別）※V7削減版:
          * 1着: +3点（旧+5点）
          * 2着: +1.5点（旧+2.5点）
          * 3着: +0.5点（旧+1点）
          * 4着以下: 0点
        - 上がり上位（レース平均より0.5秒以上速い）: +2点（旧+5点）
        - 道中後方（4角位置が後ろ50%以内） → 直線伸び: +1.5点（旧+3点）
        - 最大合計: +6.5点（旧+13点）　※G1実績馬との逆転防止のため削減
        """
        if not history_data or len(history_data) != 1:
            return 0.0
        
        first_race = history_data[0]
        race_name = first_race.get('race_name', '')
        
        # 新馬戦かどうか判定
        if '新馬' not in race_name:
            return 0.0
        
        if self.debug_mode:
            logger.debug("  ★ 新馬戦→2戦目ブースト適用")
        
        # ベースボーナス（着順別）※V7で削減（過大評価防止）
        chakujun = first_race.get('chakujun', 99)
        if chakujun == 1:
            boost = 3.0
            if self.debug_mode:
                logger.debug("    新馬1着ベースボーナス: +3点")
        elif chakujun == 2:
            boost = 1.5
            if self.debug_mode:
                logger.debug("    新馬2着ベースボーナス: +1.5点")
        elif chakujun == 3:
            boost = 0.5
            if self.debug_mode:
                logger.debug("    新馬3着ベースボーナス: +0.5点")
        else:
            boost = 0.0
            if self.debug_mode:
                logger.debug("    新馬4着以下: ベースボーナスなし")
        
        # 上がり3F評価
        my_last_3f = first_race.get('last_3f', 0.0)
        all_horses_results = first_race.get('all_horses_results', [])

        # 着差（goal_time_diff）を取得：大敗時はボーナス制限に使用
        my_goal_time_diff = first_race.get('goal_time_diff', 0.0)
        # goal_time_diffは勝ち馬との差（正=負け）。scraper側の格納方法に合わせて絶対値で扱う
        margin = abs(my_goal_time_diff) if my_goal_time_diff else 0.0

        if my_last_3f > 0 and all_horses_results:
            # レース平均上がり3Fを計算
            valid_3f = [h.get('last_3f', 0) for h in all_horses_results if h.get('last_3f', 0) > 0]
            if valid_3f:
                race_avg_3f = sum(valid_3f) / len(valid_3f)
                speed_diff = race_avg_3f - my_last_3f

                # 0.5秒以上速い場合は上がり上位と判定
                if speed_diff >= 0.5:
                    # ── 着差による上がりボーナス制限 ──────────────────────────
                    # 大差負けで上がり最速でも「末脚が届いていない」ため減額
                    # 着差1.1s以上: ボーナス無効（大敗扱い・連続大敗ペナルティと整合）
                    # 着差0.7s以上: ボーナス半減（脚は使えているが届かなかった）
                    # 着差0.7s未満: 通常通り+5点
                    if margin >= 1.1:
                        agari_bonus = 0.0
                        margin_note = f"着差{margin:.2f}s大敗のためボーナス無効"
                    elif margin >= 0.7:
                        agari_bonus = 1.0
                        margin_note = f"着差{margin:.2f}sのため半減"
                    else:
                        agari_bonus = 2.0  # V7: +5→+2点に削減
                        margin_note = ""

                    boost += agari_bonus
                    if self.debug_mode:
                        note = f" [{margin_note}]" if margin_note else ""
                        logger.debug(
                            f"    新馬で上がり上位（平均{race_avg_3f:.2f}s vs 自身{my_last_3f:.2f}s、"
                            f"差{speed_diff:+.2f}s）: +{agari_bonus:.1f}点{note}"
                        )

        # 道中後方 → 直線伸び評価
        position_4c = first_race.get('position_4c', 0)
        field_size = first_race.get('field_size', 0)

        if position_4c > 0 and field_size > 0:
            position_ratio = position_4c / field_size

            # 4角位置が後ろ50%以内（道中後方）
            if position_ratio > 0.50:
                # かつ上がりが速い（レース平均より速い）
                if my_last_3f > 0 and all_horses_results:
                    valid_3f = [h.get('last_3f', 0) for h in all_horses_results if h.get('last_3f', 0) > 0]
                    if valid_3f:
                        race_avg_3f = sum(valid_3f) / len(valid_3f)
                        if my_last_3f < race_avg_3f:
                            # 着差1.1s以上の大敗では後方差しボーナスも無効
                            if margin >= 1.1:
                                if self.debug_mode:
                                    logger.debug(
                                        f"    道中後方（{position_4c}/{field_size}番手）→"
                                        f"着差{margin:.2f}s大敗のため後方差しボーナス無効"
                                    )
                            else:
                                boost += 1.5  # V7: +3→+1.5点に削減
                                if self.debug_mode:
                                    logger.debug(f"    道中後方（{position_4c}/{field_size}番手）→直線伸び: +1.5点")
        
        if self.debug_mode:
            logger.debug(f"  → 新馬戦2戦目ブースト合計: +{boost:.1f}点")
        
        return boost
    
    def calculate_last_3f_relative_score(self, history_data: List[Dict], target_track_type: str,
                                        target_course: str = '東京', target_distance: int = 1600,
                                        target_baba: str = '良') -> float:
        """上がり3F相対評価スコア"""
        score = 0.0
        
        for idx, race in enumerate(history_data[:5]):
            distance = race.get('dist', 0)
            my_last_3f = race.get('last_3f', 0.0)
            race_name = race.get('race_name', '')
            course = race.get('course', '')
            baba = race.get('baba', '良')

            # 除外・中止・取消はスキップ
            _chakujun_check = race.get('chakujun', 99)
            if _chakujun_check == 0 or _chakujun_check >= 90:
                continue

            if distance <= 0 or my_last_3f <= 0:
                continue
            
            race_track_type = race.get('track_type', None)
            if not race_track_type or race_track_type == '不明':
                race_track_type = self._get_track_type_by_distance(distance, race_name, course)
            
            track_type_mismatch = (race_track_type != target_track_type)
            is_local = self._is_local_race(race_name, course)
            
            all_horses_results = race.get('all_horses_results', [])
            
            # デフォルトの基準値を設定
            race_avg_3f = self._get_default_baseline_3f(distance, race_track_type)
            comparison_type = "デフォルト基準値"
            
            if all_horses_results:
                goal_times = [h.get('goal_time_diff', 0) for h in all_horses_results if h.get('goal_time_diff', 0) != 0]
                
                if goal_times:
                    my_goal_time = next((h['goal_time_diff'] for h in all_horses_results 
                                       if h.get('last_3f', 0) == my_last_3f), None)
                    
                    if my_goal_time is None:
                        valid_3f = [h['last_3f'] for h in all_horses_results if h.get('last_3f', 0) > 0]
                        if valid_3f:
                            race_avg_3f = sum(valid_3f) / len(valid_3f)
                            comparison_type = "レース全体平均"
                    else:
                        THRESHOLD = 2.0
                        nearby_horses_3f = []
                        
                        for horse in all_horses_results:
                            horse_3f = horse.get('last_3f', 0)
                            goal_diff = horse.get('goal_time_diff', 0) - my_goal_time
                            
                            if abs(goal_diff) <= THRESHOLD and horse_3f > 0:
                                nearby_horses_3f.append(horse_3f)
                        
                        if nearby_horses_3f:
                            race_avg_3f = sum(nearby_horses_3f) / len(nearby_horses_3f)
                            comparison_type = f"{THRESHOLD}秒圏内{len(nearby_horses_3f)}頭平均"
                        else:
                            valid_3f = [h['last_3f'] for h in all_horses_results if h.get('last_3f', 0) > 0]
                            if valid_3f:
                                race_avg_3f = sum(valid_3f) / len(valid_3f)
                                comparison_type = "レース全体平均（圏内なし）"
            
            chakujun = race.get('chakujun', 99)
            speed_diff = race_avg_3f - my_last_3f
            
            # 短距離は1.3秒以内の基準で評価
            if distance <= 1400:
                if speed_diff >= 1.3:
                    base_points = 15.0
                elif speed_diff >= 0.8:
                    base_points = 12.0
                elif speed_diff >= 0.4:
                    base_points = 8.0
                elif speed_diff >= 0.0:
                    base_points = 5.0
                else:
                    base_points = -3.0
            else:
                if speed_diff >= 1.5:
                    base_points = 15.0
                elif speed_diff >= 1.0:
                    base_points = 12.0
                elif speed_diff >= 0.5:
                    base_points = 8.0
                elif speed_diff >= 0.0:
                    base_points = 5.0
                else:
                    base_points = -3.0
            
            # 重賞レースでの着順評価を緩和
            grade, base_reliability = self.detect_race_grade(race_name)
            
            if grade in ['G1', 'G2', 'G3']:
                if chakujun == 1:
                    finish_bonus = 3.0
                elif chakujun in [2, 3]:
                    finish_bonus = 2.0
                elif chakujun in [4, 5]:
                    finish_bonus = 1.5
                else:
                    finish_bonus = 0.0
            else:
                if chakujun == 1:
                    finish_bonus = 3.0
                elif chakujun == 2:
                    finish_bonus = 2.0
                elif chakujun == 3:
                    finish_bonus = 1.0
                else:
                    finish_bonus = 0.0
            
            # ポイント計算
            if speed_diff > 0 and finish_bonus > 0:
                points = base_points + (finish_bonus * 5.0)
            elif speed_diff > 0:
                points = base_points * 0.5
            elif speed_diff <= 0 and finish_bonus > 0:
                points = finish_bonus * 3.0
            else:
                points = -3.0
            
            reliability = base_reliability * (0.4 if is_local else 1.0) * (0.3 if track_type_mismatch else 1.0)
            time_decay = 1.0 - (idx * 0.15)
            score += points * reliability * time_decay
            
            if self.debug_mode:
                status = "◎" if speed_diff > 0 and finish_bonus > 0 else "△" if speed_diff > 0 else "×"
                mismatch_mark = "[別トラック]" if track_type_mismatch else ""
                local_mark = "[地方]" if is_local else ""
                short_mark = "[短距離1.3s基準]" if distance <= 1400 else ""
                grade_mark = f"[{grade}]" if grade in ['G1', 'G2', 'G3'] else ""
                logger.debug(f"  [{idx+1}走前] {distance}m({race_track_type}) [{status}]{grade_mark}{mismatch_mark}{local_mark}{short_mark}: "
                           f"{comparison_type} 基準{race_avg_3f:.2f}s vs 自身{my_last_3f:.2f}s 差{speed_diff:+.2f}s "
                           f"着順{chakujun} bonus{finish_bonus:.1f} 信頼度{reliability:.2f} 点{points:.1f}")
        
        return round(score, 1)
    
    def calculate_total_score(self, current_weight: float, target_course: str, target_distance: int,
                            history_data: List[Dict], target_track_type: str = "芝",
                            running_style_info: Dict = None, race_pace_prediction: Dict = None,
                            target_baba: str = "良", horse_age: int = None, horse_sex: str = None) -> Dict:
        """総合スコアを計算"""
        
        # 1. 上がり3F相対評価
        last_3f_score = self.calculate_last_3f_relative_score(
            history_data, target_track_type, target_course, target_distance, target_baba
        )
        
        # 2. 距離適性スコア
        distance_score = self._calculate_distance_score(history_data, target_distance)
        
        # 3. コース適性スコア
        course_score = self._calculate_course_score(history_data, target_course, target_track_type)
        
        # 4. 斤量評価
        weight_penalty = self._calculate_weight_penalty(current_weight, horse_age, horse_sex)
        
        # 5. 後半4F評価
        late_4f_score = self._calculate_late_4f_score(history_data, target_distance, target_track_type)
        
        # 6. 長期休養ペナルティ
        layoff_penalty = self._calculate_layoff_penalty(history_data)
        
        # 7. 重賞出走ボーナス
        grade_race_bonus = self._calculate_grade_race_bonus(history_data)
        
        # 8. 【新】新馬戦2戦目ブースト
        shinba_boost = self._calculate_shinba_second_race_boost(history_data)
        
        # 9. 【新】連続大敗ペナルティ
        consecutive_loss_penalty = self._calculate_consecutive_big_loss_penalty(history_data)

        # 10. 【新】連勝着差ボーナス
        winning_streak_bonus = self._calculate_winning_streak_bonus(history_data)

        # 11. 【新】コースレコード比較スコア
        cr_score = self._calculate_course_record_score(
            history_data, target_course, target_distance, target_track_type)

        # 12. 脚質ボーナス
        style_bonus = 0.0
        if running_style_info and race_pace_prediction:
            style = running_style_info.get('style', '')
            confidence = running_style_info.get('confidence', 0.0)
            pace = race_pace_prediction.get('pace', 'ミドル')
            
            raw_bonus = self.style_analyzer.calculate_style_match_bonus(style, pace, target_course, target_distance)
            style_weight = self.style_analyzer.get_style_weight(target_course, target_distance, style)
            style_bonus = raw_bonus * confidence * style_weight
            
            if self.debug_mode:
                logger.debug(f"  脚質ボーナス: {style}×{pace}×{target_course}{target_distance}m")
                logger.debug(f"    生ボーナス{raw_bonus:+.1f} × 信頼度{confidence:.2f} × ウェイト{style_weight:.2f} = {style_bonus:+.2f}")
        
        # 13. 危険フラグ
        danger_flags = self._check_danger_flags(history_data, target_course, target_track_type)
        danger_penalty = -15.0 if danger_flags['local_to_jra'] else 0.0
        
        # スコア正規化
        is_long_distance = target_distance >= 1800 and target_track_type == "芝"
        
        normalized_3f = min(last_3f_score / 150.0 * 100, 100)
        normalized_distance = min(distance_score / 15.0 * 100, 100)
        normalized_course = min(course_score / 15.0 * 100, 100)
        normalized_style = min(style_bonus / 20.0 * 100, 100)
        normalized_late_4f = min(late_4f_score / 50.0 * 100, 100) if late_4f_score != 0 else 0
        
        # 総合スコア計算
        if is_long_distance:
            weight_3f = 0.30
            weight_late_4f = 0.20
            weight_distance = 0.15
            weight_course = 0.10
            weight_style = 0.15
            
            total = (
                normalized_3f * weight_3f +
                normalized_late_4f * weight_late_4f +
                normalized_distance * weight_distance +
                normalized_course * weight_course +
                normalized_style * weight_style +
                weight_penalty +
                layoff_penalty +
                grade_race_bonus +
                shinba_boost +
                consecutive_loss_penalty +
                winning_streak_bonus +  # 【新】連勝着差ボーナス
                cr_score +              # 【新】コースレコード比較スコア
                danger_penalty
            )
        else:
            # 斤量×タイム評価を廃止し、上がり3F・距離・コース・脚質に重みを再配分
            weight_3f = 0.45
            weight_distance = 0.20
            weight_course = 0.15
            weight_style = 0.20
            
            total = (
                normalized_3f * weight_3f +
                normalized_distance * weight_distance +
                normalized_course * weight_course +
                normalized_style * weight_style +
                weight_penalty +
                layoff_penalty +
                grade_race_bonus +
                shinba_boost +
                consecutive_loss_penalty +
                winning_streak_bonus +  # 【新】連勝着差ボーナス
                cr_score +              # 【新】コースレコード比較スコア
                danger_penalty
            )
        
        return {
            'total_score': round(total, 1),
            'last_3f_score': last_3f_score,
            'distance_score': distance_score,
            'course_score': course_score,
            'style_bonus': round(style_bonus, 1),
            'late_4f_score': late_4f_score,
            'weight_penalty': weight_penalty,
            'layoff_penalty': layoff_penalty,
            'grade_race_bonus': grade_race_bonus,
            'shinba_boost': shinba_boost,
            'consecutive_loss_penalty': consecutive_loss_penalty,
            'winning_streak_bonus': winning_streak_bonus,
            'cr_score': cr_score,
            'danger_penalty': danger_penalty,
            'danger_flags': danger_flags
        }

    def calculate_total_score_verbose(self, current_weight: float, target_course: str, target_distance: int,
                                      history_data: List[Dict], target_track_type: str = "芝",
                                      running_style_info: Dict = None, race_pace_prediction: Dict = None,
                                      target_baba: str = "良", horse_age: int = None, horse_sex: str = None) -> str:
        """スコア計算して詳細ログを直接返すショートカット"""
        result = self.calculate_total_score(
            current_weight=current_weight,
            target_course=target_course,
            target_distance=target_distance,
            history_data=history_data,
            target_track_type=target_track_type,
            running_style_info=running_style_info,
            race_pace_prediction=race_pace_prediction,
            target_baba=target_baba,
            horse_age=horse_age,
            horse_sex=horse_sex,
        )
        return self.format_score_breakdown_verbose(
            result=result,
            target_distance=target_distance,
            history_data=history_data,
            current_weight=current_weight,
            target_course=target_course,
            target_track_type=target_track_type,
            running_style_info=running_style_info,
            race_pace_prediction=race_pace_prediction,
            horse_age=horse_age,
            horse_sex=horse_sex,
        )
    
    def _calculate_course_record_score(self, history_data: List[Dict],
                                        target_course: str, target_distance: int,
                                        target_track_type: str) -> float:
        """
        【新】コースレコード比較スコア

        同距離・同トラックタイプの過去走を「走ったコースのCR」と比較してスコア化。
        コースが違っても距離さえ合えば評価可能（コース補正は自動的に織り込まれる）。

        ロジック:
          diff = goal_sec - CR(走ったコース・走った距離)
          → コースが違っても「そのコースでのCRからの差」で公平比較

        スコア（diff に応じて）:
          diff <= 0.0s  → 10.0点（CR更新・更新水準）
          diff <= 1.0s  → 10.0 - diff * 2.0       (1.0s差→8.0点)
          diff <= 2.0s  → 8.0  - (diff-1.0) * 2.0 (2.0s差→6.0点)
          diff <= 4.0s  → 6.0  - (diff-2.0) * 2.5 (4.0s差→1.0点)
          diff >  4.0s  → 0.0点

        時系列重み: 1走前×1.0 / 2走前×0.7 / 3走前×0.5 / 4走前×0.4 / 5走前×0.3
        上限: +10.0点
        距離差ペナルティ: 200m差ごとに重み×0.8（最大200m差まで許容）
        """
        if not history_data or target_track_type == 'ダート':
            return 0.0

        TIME_WEIGHTS = [1.0, 0.7, 0.5, 0.4, 0.3]

        bonus = 0.0
        evaluated = 0

        for idx, race in enumerate(history_data[:5]):
            race_course  = race.get('course', '')
            race_dist    = race.get('dist', 0)
            race_track   = race.get('track_type', '')
            goal_sec     = race.get('goal_sec', 0.0)
            dist_text    = race.get('dist_text', '')
            chakujun     = race.get('chakujun', 99)

            # 除外・中止はスキップ
            if chakujun == 0 or chakujun >= 90:
                continue

            # goal_secがない場合（scraper_v5）: all_horses_resultsから逆算
            if goal_sec <= 0:
                all_results = race.get('all_horses_results', [])
                # 1着馬のgoal_secを取得
                first_sec = next(
                    (h.get('goal_sec', 0) for h in all_results if h.get('chakujun') == 1),
                    0.0
                )
                if first_sec > 0:
                    goal_time_diff = race.get('goal_time_diff', None)
                    if goal_time_diff is not None:
                        goal_sec = first_sec + abs(float(goal_time_diff))
                    elif chakujun == 1:
                        goal_sec = first_sec

            # トラックタイプ一致必須（芝/ダートは比較不可）
            if race_track != target_track_type:
                continue
            # 距離差200m以内まで許容
            dist_diff = abs(race_dist - target_distance)
            if dist_diff > 200:
                continue
            if goal_sec <= 0:
                continue

            # 走ったコースのCRを取得
            race_variant = CourseAnalyzer.detect_track_variant(race_course, race_dist, dist_text)
            cr = CourseAnalyzer.COURSE_RECORDS.get((race_variant, race_dist), 0.0)
            if cr <= 0:
                cr = CourseAnalyzer.COURSE_RECORDS.get((race_course, race_dist), 0.0)
            if cr <= 0:
                continue   # CRデータなし → スキップ

            diff = goal_sec - cr
            if diff <= 0:
                pts = 10.0
            elif diff <= 1.0:
                pts = 10.0 - diff * 2.0
            elif diff <= 2.0:
                pts = 8.0 - (diff - 1.0) * 2.0
            elif diff <= 4.0:
                pts = 6.0 - (diff - 2.0) * 2.5
            else:
                pts = 0.0

            # 距離差ペナルティ（200m差で重み×0.8）
            dist_penalty = 0.8 if dist_diff == 200 else 1.0

            w = TIME_WEIGHTS[idx] * dist_penalty
            bonus += pts * w
            evaluated += 1

            if self.debug_mode:
                dist_note = f"(距離差{dist_diff}m補正)" if dist_diff > 0 else ""
                logger.debug(
                    f"  CRスコア [{idx+1}走前] {race_course}{race_dist}m "
                    f"走破{goal_sec:.1f}s CR({race_variant}){cr:.1f}s 差{diff:+.2f}s "
                    f"→ {pts:.1f}×{w:.2f} = {pts*w:.2f}点{dist_note}"
                )

        if evaluated == 0:
            return 0.0

        result = round(min(bonus, 10.0), 1)
        if self.debug_mode and result > 0:
            logger.debug(f"  CRスコア 合計: +{result}点（{evaluated}走評価・上限10点）")
        return result

    def _calculate_late_4f_score(self, history_data: List[Dict], target_distance: int, 
                                  target_track_type: str) -> float:
        """後半4F評価（芝中長距離専用）- 実データ使用"""
        if target_track_type != "芝" or target_distance < 1800:
            return 0.0
        
        BASELINE_4F = 47.2 if target_distance <= 2000 else 47.8 if target_distance <= 2400 else 48.3
        score = 0.0
        
        for idx, race in enumerate(history_data[:5]):
            distance = race.get('dist', 0)
            race_name = race.get('race_name', '')
            course = race.get('course', '')
            
            race_track_type = race.get('track_type', None)
            if not race_track_type or race_track_type == '不明':
                race_track_type = self._get_track_type_by_distance(distance, race_name, course)
            
            if race_track_type != '芝' or distance < 1800:
                continue
            
            if self._is_local_race(race_name, course):
                continue
            
            # 実際の後半4Fデータを使用（ラップタイムから計算）
            late_4f = race.get('late_4f', 0.0)
            
            # 後半4Fデータがない場合は上がり3Fから推定（フォールバック）
            if late_4f <= 0:
                last_3f = race.get('last_3f', 0.0)
                if last_3f <= 0:
                    continue
                # 推定式: 後半4F ≒ 上がり3F × 4/3 + 0.4秒
                late_4f = last_3f * (4.0 / 3.0) + 0.4
                if self.debug_mode:
                    logger.debug(f"    後半4F推定値使用: {late_4f:.1f}秒 (上がり3F {last_3f:.1f}秒から)")
            else:
                if self.debug_mode:
                    logger.debug(f"    後半4F実データ使用: {late_4f:.1f}秒")
            
            diff_from_baseline = BASELINE_4F - late_4f
            
            if diff_from_baseline >= 3.5:
                multiplier = 1.80
            elif diff_from_baseline >= 2.5:
                multiplier = 1.50
            elif diff_from_baseline >= 1.5:
                multiplier = 1.30
            elif diff_from_baseline >= 0.7:
                multiplier = 1.15
            elif diff_from_baseline >= -0.5:
                multiplier = 1.00
            elif diff_from_baseline >= -2.0:
                multiplier = 0.85
            else:
                multiplier = 0.65
            
            points = diff_from_baseline * 10.0 * multiplier
            
            grade, _ = self.detect_race_grade(race_name)
            reliability_map = {
                'G1': 1.0, 'G2': 0.95, 'G3': 0.9,
                'JpnI': 1.0, 'JpnII': 0.95, 'JpnIII': 0.9,
                'OP': 0.85
            }
            reliability = reliability_map.get(grade, 0.7)
            
            time_decay = 1.0 - (idx * 0.15)
            
            chakujun = race.get('chakujun', 99)
            if chakujun == 1:
                finish_bonus = 1.0
            elif chakujun <= 3:
                finish_bonus = 0.9
            elif chakujun <= 5:
                finish_bonus = 0.75
            elif chakujun <= 10:
                finish_bonus = 0.5
            else:
                finish_bonus = 0.3
            
            score += points * reliability * time_decay * finish_bonus
        
        return round(score, 1)
    
    
    def _check_danger_flags(self, history_data: List[Dict], target_course: str, 
                           target_track_type: str) -> Dict:
        """危険フラグチェック"""
        flags = {
            'local_to_jra': False,
            'track_change': False,
            'long_layoff': False
        }
        
        if not history_data:
            return flags
        
        # 地方→JRA転入チェック
        # 直近5走中4走以上が地方 かつ 直近1走もしくは2走がJRAなら「転入済み」として除外
        recent_races = history_data[:5]
        local_count = sum(1 for race in recent_races
                         if self._is_local_race(race.get('race_name', ''), race.get('course', '')))

        if local_count >= 4:
            # 前走（1走前）がJRAなら転入済み→フラグなし
            last_race = history_data[0]
            last_is_local = self._is_local_race(
                last_race.get('race_name', ''), last_race.get('course', ''))
            if last_is_local:
                # 前走も地方→まだ転入していない→フラグあり
                flags['local_to_jra'] = True
            # 前走がJRAなら転入済みとみなしてフラグなし
        
        return flags
    
    def format_score_breakdown(self, result: Dict, target_distance: int) -> str:
        """スコア内訳をフォーマット（詳細版）"""
        lines = []
        lines.append(f"【総合スコア: {result['total_score']:.1f}点】")
        lines.append(f"  上がり3F評価: {result['last_3f_score']:.1f}点")

        if target_distance >= 1800 and result['late_4f_score'] != 0:
            lines.append(f"  後半4F評価: {result['late_4f_score']:.1f}点")

        lines.append(f"  距離適性: {result['distance_score']:.1f}点")
        lines.append(f"  コース適性: {result['course_score']:.1f}点")
        lines.append(f"  脚質ボーナス: {result['style_bonus']:.1f}点")

        if result['weight_penalty'] != 0:
            if result['weight_penalty'] > 0:
                lines.append(f"  斤量軽減ボーナス: +{result['weight_penalty']:.1f}点")
            else:
                lines.append(f"  斤量増ペナルティ: {result['weight_penalty']:.1f}点")

        if result['layoff_penalty'] != 0:
            lines.append(f"  長期休養ペナルティ: {result['layoff_penalty']:.1f}点")
        else:
            lines.append(f"  長期休養ペナルティ: 0.0点（直近4ヶ月以内）")

        if result['grade_race_bonus'] != 0:
            lines.append(f"  重賞出走ボーナス: +{result['grade_race_bonus']:.1f}点")

        if result.get('shinba_boost', 0) != 0:
            lines.append(f"  ★新馬戦2戦目ブースト: +{result['shinba_boost']:.1f}点")

        wsb = result.get('winning_streak_bonus', 0)
        if wsb > 0:
            lines.append(f"  🔥連勝着差ボーナス: +{wsb:.1f}点")

        crs = result.get('cr_score', 0)
        if crs > 0:
            lines.append(f"  ⏱️CRスコア: +{crs:.1f}点")
            TIME_WEIGHTS_CR = [1.0, 0.7, 0.5, 0.4, 0.3]
            for idx, race in enumerate(history_data[:5]):
                rc   = race.get('course', '')
                rd   = race.get('dist', 0)
                rt   = race.get('track_type', '')
                ck   = race.get('chakujun', 99)
                dtxt = race.get('dist_text', '')
                if ck == 0 or ck >= 90:
                    continue
                if rt != target_track_type:
                    continue
                ddiff = abs(rd - target_distance)
                if ddiff > 200:
                    continue
                # goal_sec取得（v5互換フォールバック）
                gs = race.get('goal_sec', 0.0)
                if gs <= 0:
                    all_res = race.get('all_horses_results', [])
                    fs = next((h.get('goal_sec', 0) for h in all_res if h.get('chakujun') == 1), 0.0)
                    if fs > 0:
                        gtd = race.get('goal_time_diff', None)
                        gs = fs + abs(float(gtd)) if gtd is not None else (fs if ck == 1 else 0.0)
                if gs <= 0:
                    continue
                rv = CourseAnalyzer.detect_track_variant(rc, rd, dtxt)
                cr_val = CourseAnalyzer.COURSE_RECORDS.get((rv, rd), 0.0) or \
                         CourseAnalyzer.COURSE_RECORDS.get((rc, rd), 0.0)
                dist_pen = 0.8 if ddiff == 200 else 1.0
                w = TIME_WEIGHTS_CR[idx] * dist_pen
                if cr_val > 0:
                    diff_cr = gs - cr_val
                    lines.append(
                        f"    {idx+1}走前 {rc}{rd}m: 走破{gs:.1f}s / CR({rv}){cr_val:.1f}s 差{diff_cr:+.2f}s"
                        + (f" (距離差{ddiff}m補正×{dist_pen})" if ddiff > 0 else "")
                    )
                else:
                    lines.append(f"    {idx+1}走前 {rc}{rd}m: 走破{gs:.1f}s（CRデータなし）")
        elif 'cr_score' in result:
            lines.append(f"  ⏱️CRスコア: 0.0点（同距離の走破タイムなし or データ未取得）")

        # 連続大敗ペナルティの詳細ログ
        clp = result.get('consecutive_loss_penalty', 0)
        detail = result.get('consecutive_loss_detail', {})
        if detail:
            consec = detail.get('consecutive_losses', 0)
            reduced = detail.get('reduced_count', 0)
            reasons = detail.get('reduce_reasons', [])  # list of str, one per race
            if clp != 0:
                lines.append(f"  連続大敗ペナルティ: {clp:.1f}点")
                lines.append(f"    → 連続大敗{consec}回検出 / 軽減後実質{reduced}回")
                for r in reasons:
                    lines.append(f"       {r}")
            else:
                if consec > 0:
                    lines.append(f"  連続大敗ペナルティ: 0.0点（{consec}回大敗あり・全て軽減）")
                    for r in reasons:
                        lines.append(f"       {r}")
                else:
                    lines.append(f"  連続大敗ペナルティ: 0.0点（大敗なし）")
        else:
            if clp != 0:
                lines.append(f"  連続大敗ペナルティ: {clp:.1f}点")
            else:
                lines.append(f"  連続大敗ペナルティ: 0.0点")

        if result['danger_penalty'] != 0:
            lines.append(f"  危険フラグペナルティ: {result['danger_penalty']:.1f}点")

        return "\n".join(lines)

    def format_score_breakdown_verbose(self, result: Dict, target_distance: int,
                                       history_data: List[Dict] = None,
                                       current_weight: float = 0,
                                       target_course: str = '',
                                       target_track_type: str = '芝',
                                       running_style_info: Dict = None,
                                       race_pace_prediction: Dict = None,
                                       horse_age: int = None,
                                       horse_sex: str = None) -> str:
        """スコア内訳を超詳細にフォーマット（計算根拠まで全表示）"""
        from datetime import datetime

        THRESHOLD = 1.1  # 大敗判定閾値

        lines = []
        sep = "=" * 60
        lines.append(sep)
        lines.append(f"【総合スコア: {result['total_score']:.1f}点】")
        lines.append(sep)

        # ─── 上がり3F評価 ────────────────────────────────────────
        lines.append(f"\n▼ 上がり3F評価: {result['last_3f_score']:.1f}点")
        if history_data:
            for idx, race in enumerate(history_data[:5]):
                dist = race.get('dist', 0)
                my_3f = race.get('last_3f', 0.0)
                race_name = race.get('race_name', '')
                course = race.get('course', '')
                chakujun = race.get('chakujun', 99)
                if dist <= 0 or my_3f <= 0:
                    continue
                # 基準値取得
                baseline = self._get_default_baseline_3f(dist, race.get('track_type', target_track_type))
                ahrs = race.get('all_horses_results', [])
                if ahrs:
                    valid = [h['last_3f'] for h in ahrs if h.get('last_3f', 0) > 0]
                    if valid:
                        baseline = sum(valid) / len(valid)
                speed_diff = baseline - my_3f
                grade, _ = self.detect_race_grade(race_name)
                grade_mark = f"[{grade}]" if grade in ['G1','G2','G3'] else ""
                lines.append(f"  {idx+1}走前 {race_name}{grade_mark} {course}{dist}m: "
                             f"基準{baseline:.2f}s vs 自身{my_3f:.2f}s 差{speed_diff:+.2f}s "
                             f"({chakujun}着)")
        else:
            lines.append("  （履歴データなし）")

        # ─── 後半4F評価 ────────────────────────────────────────
        if target_distance >= 1800:
            lines.append(f"\n▼ 後半4F評価: {result['late_4f_score']:.1f}点")
            if result['late_4f_score'] == 0:
                lines.append("  （ダート or 1800m未満 or 対象レースなし）")

        # ─── 距離適性 ────────────────────────────────────────
        lines.append(f"\n▼ 距離適性: {result['distance_score']:.1f}点")
        if history_data:
            for idx, race in enumerate(history_data[:5]):
                dist       = race.get('dist', 0)
                chakujun   = race.get('chakujun', 99)
                time_diff  = race.get('goal_time_diff', None)
                diff       = abs(target_distance - dist)

                # 除外・中止はスキップ
                if chakujun == 0 or chakujun >= 90:
                    lines.append(f"  {idx+1}走前 {dist}m: 除外/中止 → スキップ")
                    continue

                # 線形補間: 0m→15点、200m→10点
                if diff <= 200:
                    base_pts = 15.0 - (diff / 200.0) * 5.0
                elif diff <= 400:
                    base_pts = 10.0
                elif diff <= 600:
                    base_pts = 5.0
                else:
                    lines.append(f"  {idx+1}走前 {dist}m: 距離差{diff}m → 0点（適性外）")
                    continue

                if time_diff is not None and time_diff != 0:
                    margin = abs(float(time_diff))
                    if margin <= 0.3:
                        coef, coef_note = 1.00, f"着差{margin:.2f}s(僅差)"
                    elif margin <= 0.6:
                        coef, coef_note = 0.85, f"着差{margin:.2f}s"
                    elif margin <= 1.0:
                        coef, coef_note = 0.70, f"着差{margin:.2f}s"
                    elif margin <= 1.5:
                        coef, coef_note = 0.50, f"着差{margin:.2f}s(大敗気味)"
                    elif margin <= 2.5:
                        coef, coef_note = 0.30, f"着差{margin:.2f}s(大敗)"
                    else:
                        coef, coef_note = 0.10, f"着差{margin:.2f}s(惨敗)"
                else:
                    if chakujun == 1:
                        coef, coef_note = 1.00, f"{chakujun}着"
                    elif chakujun == 2:
                        coef, coef_note = 0.85, f"{chakujun}着"
                    elif chakujun == 3:
                        coef, coef_note = 0.70, f"{chakujun}着"
                    elif chakujun <= 5:
                        coef, coef_note = 0.50, f"{chakujun}着"
                    elif chakujun <= 9:
                        coef, coef_note = 0.30, f"{chakujun}着"
                    else:
                        coef, coef_note = 0.10, f"{chakujun}着(惨敗)"

                pts = base_pts * coef
                lines.append(
                    f"  {idx+1}走前 {dist}m: 距離差{diff}m→基礎{base_pts:.0f}点"
                    f" × {coef_note}({coef:.2f}) = {pts:.1f}点"
                )

        # ─── コース適性 ────────────────────────────────────────
        lines.append(f"\n▼ コース適性: {result['course_score']:.1f}点")
        if history_data:
            for idx, race in enumerate(history_data[:5]):
                course     = race.get('course', '')
                tt         = race.get('track_type', '')
                chakujun   = race.get('chakujun', 99)
                time_diff  = race.get('goal_time_diff', None)

                same_course = (course == target_course)
                same_track  = (not target_track_type or not tt or tt == target_track_type)

                # 同競馬場のみ点数付与。別競馬場は0点（中山≠東京）
                if same_course and same_track:
                    base_pts   = 15.0
                    match_note = "同場同トラック"
                elif same_course and not same_track:
                    base_pts   = 8.0
                    match_note = f"同場・トラック違い({tt})"
                else:
                    lines.append(f"  {idx+1}走前 {course}({tt}): 別競馬場 → 0点（{course}≠{target_course}）")
                    continue

                if time_diff is not None and time_diff != 0:
                    margin = abs(float(time_diff))
                    if margin <= 0.3:
                        coef, coef_note = 1.00, f"着差{margin:.2f}s(僅差)"
                    elif margin <= 0.6:
                        coef, coef_note = 0.85, f"着差{margin:.2f}s"
                    elif margin <= 1.0:
                        coef, coef_note = 0.70, f"着差{margin:.2f}s"
                    elif margin <= 1.5:
                        coef, coef_note = 0.50, f"着差{margin:.2f}s(大敗気味)"
                    elif margin <= 2.5:
                        coef, coef_note = 0.30, f"着差{margin:.2f}s(大敗)"
                    else:
                        coef, coef_note = 0.10, f"着差{margin:.2f}s(惨敗)"
                else:
                    coef_map = {1: (1.00,"1着"), 2: (0.85,"2着"), 3: (0.70,"3着")}
                    if chakujun in coef_map:
                        coef, coef_note = coef_map[chakujun]
                    elif chakujun <= 5:
                        coef, coef_note = 0.50, f"{chakujun}着"
                    elif chakujun <= 9:
                        coef, coef_note = 0.30, f"{chakujun}着"
                    else:
                        coef, coef_note = 0.10, f"{chakujun}着(惨敗)"

                pts = base_pts * coef
                lines.append(
                    f"  {idx+1}走前 {course}({tt}): {match_note}→基礎{base_pts:.0f}点"
                    f" × {coef_note}({coef:.2f}) = {pts:.1f}点"
                )

        # ─── 脚質ボーナス ────────────────────────────────────────
        lines.append(f"\n▼ 脚質ボーナス: {result['style_bonus']:.1f}点")
        if running_style_info and race_pace_prediction:
            style = running_style_info.get('style', '')
            conf = running_style_info.get('confidence', 0.0)
            pace = race_pace_prediction.get('pace', 'ミドル')
            raw = self.style_analyzer.calculate_style_match_bonus(style, pace, target_course, target_distance)
            weight = self.style_analyzer.get_style_weight(target_course, target_distance, style)
            lines.append(f"  脚質:{style} × ペース:{pace} × {target_course}{target_distance}m")
            lines.append(f"  生ボーナス{raw:+.1f} × 信頼度{conf:.2f} × ウェイト{weight:.2f} = {result['style_bonus']:+.2f}点")
        else:
            lines.append("  （脚質情報なし）")

        # ─── 斤量評価 ────────────────────────────────────────
        wp = result['weight_penalty']
        lines.append(f"\n▼ 斤量評価: {wp:+.1f}点")
        if current_weight > 0:
            if horse_age and horse_sex:
                if horse_age >= 4:
                    base = 56.0 if horse_sex in ['牝','牝馬','F','f'] else 58.0
                elif horse_age == 3:
                    base = 55.0 if horse_sex in ['牝','牝馬','F','f'] else 57.0
                else:
                    base = 55.0
            else:
                base = 58.0
            lines.append(f"  現在{current_weight:.1f}kg - 基準{base:.1f}kg = 差分{current_weight-base:+.1f}kg → {wp:+.1f}点")

        # ─── 長期休養ペナルティ ────────────────────────────────────────
        lp = result['layoff_penalty']
        lines.append(f"\n▼ 長期休養ペナルティ: {lp:.1f}点")
        if history_data:
            last_race = history_data[0]
            date_str = last_race.get('race_date', '')
            if date_str:
                try:
                    race_date = datetime.strptime(date_str, '%Y/%m/%d')
                    days = (datetime.now() - race_date).days
                    months = days / 30.44
                    lines.append(f"  前走日: {date_str} → {days}日前 ({months:.1f}ヶ月)")
                    if lp == 0:
                        lines.append(f"  → 4ヶ月以内のためペナルティなし")
                    else:
                        lines.append(f"  → {lp:.1f}点ペナルティ")
                except Exception:
                    lines.append("  （日付解析不可）")

        # ─── 重賞出走ボーナス ────────────────────────────────────────
        grb = result['grade_race_bonus']
        lines.append(f"\n▼ 重賞出走ボーナス: +{grb:.1f}点")
        if history_data:
            found = False
            for idx, race in enumerate(history_data[:5]):
                race_name = race.get('race_name', '')
                chakujun = race.get('chakujun', 99)
                grade, _ = self.detect_race_grade(race_name)
                if grade in ['G1','G2','G3']:
                    found = True
                    decay = 1.0 - (idx * 0.15)
                    if grade == 'G1':
                        rb = 10.0 if chakujun==1 else 8.0 if chakujun<=3 else 5.0
                    elif grade == 'G2':
                        rb = 7.0 if chakujun==1 else 5.0 if chakujun<=3 else 3.0
                    else:
                        rb = 5.0 if chakujun==1 else 3.0 if chakujun<=3 else 2.0
                    lines.append(f"  {idx+1}走前 {race_name}({grade}) {chakujun}着: {rb:.1f}×時間減衰{decay:.2f} = {rb*decay:.1f}点")
            if not found:
                lines.append("  （重賞出走なし）")

        # ─── 新馬戦2戦目ブースト ────────────────────────────────────────
        sb = result.get('shinba_boost', 0)
        lines.append(f"\n▼ 新馬戦2戦目ブースト: +{sb:.1f}点")
        if history_data and len(history_data) == 1 and '新馬' in history_data[0].get('race_name', ''):
            fr = history_data[0]
            chakujun = fr.get('chakujun', 99)
            base = 5.0 if chakujun==1 else 2.5 if chakujun==2 else 1.0 if chakujun==3 else 0.0
            lines.append(f"  新馬戦{chakujun}着 ベースボーナス: +{base:.1f}点")
            ahrs = fr.get('all_horses_results', [])
            my3f = fr.get('last_3f', 0.0)
            margin_disp = abs(fr.get('goal_time_diff', 0.0))
            if ahrs and my3f > 0:
                valid = [h['last_3f'] for h in ahrs if h.get('last_3f',0)>0]
                if valid:
                    avg = sum(valid)/len(valid)
                    diff = avg - my3f
                    if diff >= 0.5:
                        if margin_disp >= 1.1:
                            bonus_note = f"上がり上位だが着差{margin_disp:.2f}s大敗→ボーナス無効(+0点)"
                        elif margin_disp >= 0.7:
                            bonus_note = f"上がり上位だが着差{margin_disp:.2f}s→半減(+2.5点)"
                        else:
                            bonus_note = "上がり上位ボーナス+5点"
                    else:
                        bonus_note = "上がり上位非該当"
                    lines.append(f"  上がり: 自身{my3f:.2f}s vs レース平均{avg:.2f}s 差{diff:+.2f}s → {bonus_note}")
            p4c = fr.get('position_4c', 0)
            fs = fr.get('field_size', 0)
            if p4c > 0 and fs > 0:
                ratio = p4c / fs
                if ratio > 0.50:
                    if margin_disp >= 1.1:
                        kekka = f"後方だが着差{margin_disp:.2f}s大敗→後方差しボーナス無効"
                    else:
                        kekka = "後方差し加算対象"
                else:
                    kekka = "中団以前、後方差し非対象"
                lines.append(f"  4角位置: {p4c}/{fs}番手 ({ratio:.0%}) → {kekka}")
        elif sb == 0:
            lines.append("  （条件非該当：前走が新馬戦で通算1走のみの場合に適用）")

        # ─── 連続大敗ペナルティ（最詳細） ────────────────────────────────────────
        clp = result.get('consecutive_loss_penalty', 0)
        lines.append(f"\n▼ 連続大敗ペナルティ: {clp:.1f}点")
        if history_data:
            # 連続大敗を自前で再計算して詳細表示
            consec = self.count_consecutive_big_losses(history_data, threshold=THRESHOLD)
            if consec == 0:
                lines.append(f"  連続大敗なし（直近{min(len(history_data),3)}走で着差{THRESHOLD}s以上の大敗なし）")
            else:
                lines.append(f"  連続大敗{consec}回検出（着差{THRESHOLD}s以上）:")
                reduced_count = 0
                for i in range(consec):
                    race = history_data[i]
                    gtd = race.get('goal_time_diff', 0)
                    rname = race.get('race_name','')
                    dist = race.get('dist', 0)
                    date = race.get('race_date', '')
                    should_reduce = self._should_reduce_big_loss_penalty(history_data, i)
                    reduce_reason = ""
                    if should_reduce:
                        # 理由特定
                        if i + 1 < len(history_data):
                            prev = history_data[i+1]
                            prev_dist = prev.get('dist', 0)
                            if abs(dist - prev_dist) >= 600:
                                reduce_reason = f"距離大幅変更（{prev_dist}m→{dist}m）"
                            elif prev.get('chakujun',0) == 0 or prev.get('chakujun',0) >= 90:
                                reduce_reason = "前走中止/取消"
                            else:
                                # 休養明け
                                from datetime import datetime as _dt
                                try:
                                    d1 = _dt.strptime(date, '%Y/%m/%d')
                                    d2 = _dt.strptime(prev.get('race_date',''), '%Y/%m/%d')
                                    days = (d1 - d2).days
                                    reduce_reason = f"休養明け初戦（{days}日ぶり）"
                                except Exception:
                                    reduce_reason = "軽減条件該当"
                        mark = "✓軽減"
                    else:
                        mark = "✗適用"
                        reduced_count += 1
                    lines.append(f"    {i+1}走前 {rname} {dist}m 着差{gtd:.2f}s [{mark}] "
                                 f"{('→ ' + reduce_reason) if reduce_reason else ''}")

                lines.append(f"  実質カウント: {reduced_count}回 → "
                             f"{'ペナルティなし' if reduced_count==0 else f'-3点' if reduced_count==1 else f'-8点' if reduced_count==2 else f'-15点'}")

        # ─── 危険フラグ ────────────────────────────────────────
        dp = result['danger_penalty']
        if dp != 0:
            lines.append(f"\n▼ 危険フラグペナルティ: {dp:.1f}点")
            flags = result.get('danger_flags', {})
            if flags.get('local_to_jra'):
                lines.append("  → 地方競馬→JRA転入フラグ（直近5走中4走以上が地方、かつ前走も地方）")

        lines.append(f"\n{sep}")
        return "\n".join(lines)


if __name__ == "__main__":
    # テストケース
    print("=" * 80)
    print("enhanced_scorer_v6.py - 新馬戦2戦目ブースト機能テスト")
    print("=" * 80)
    print()
    
    scorer = RaceScorer(debug_mode=False)  # verboseで全情報表示するのでdebug_modeはFalでOK
    
    # 【新】新馬戦2戦目ブーストのテスト
    print("【新馬戦2戦目ブーストテスト】")
    print()
    
    # ケース1: 新馬戦で上がり上位 + 道中後方→直線伸び（最大ブースト）
    print("ケース1: 新馬戦で上がり上位 + 道中後方→直線伸び")
    test_history_shinba_best = [
        {
            'race_name': '新馬',
            'course': '東京',
            'dist': 1600,
            'chakujun': 3,
            'last_3f': 33.5,
            'weight': 54.0,
            'track_type': '芝',
            'position_4c': 12,  # 道中後方
            'field_size': 16,
            'all_horses_results': [
                {'last_3f': 33.2, 'goal_time_diff': -0.4},
                {'last_3f': 33.4, 'goal_time_diff': -0.2},
                {'last_3f': 33.5, 'goal_time_diff': 0.0},  # 自分
                {'last_3f': 34.2, 'goal_time_diff': 0.5},  # レース平均: 約34.0秒（0.5秒以上速い）
                {'last_3f': 34.5, 'goal_time_diff': 0.8},
            ]
        }
    ]
    
    result_shinba_best = scorer.calculate_total_score(
        current_weight=54.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_shinba_best,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.75},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(scorer.format_score_breakdown(result_shinba_best, 1600))
    print()
    
    # ケース2: 新馬戦で上がり上位のみ
    print("ケース2: 新馬戦で上がり上位（道中は中団）")
    test_history_shinba_good = [
        {
            'race_name': '新馬',
            'course': '中山',
            'dist': 1600,
            'chakujun': 2,
            'last_3f': 34.8,
            'weight': 54.0,
            'track_type': '芝',
            'position_4c': 7,  # 中団
            'field_size': 16,
            'all_horses_results': [
                {'last_3f': 34.5, 'goal_time_diff': -0.3},
                {'last_3f': 34.8, 'goal_time_diff': 0.0},  # 自分
                {'last_3f': 35.5, 'goal_time_diff': 0.5},  # レース平均: 約35.3秒（0.5秒速い）
                {'last_3f': 35.8, 'goal_time_diff': 0.7},
            ]
        }
    ]
    
    result_shinba_good = scorer.calculate_total_score(
        current_weight=54.0,
        target_course='中山',
        target_distance=1600,
        history_data=test_history_shinba_good,
        target_track_type='芝',
        running_style_info={'style': '先行', 'confidence': 0.80},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(scorer.format_score_breakdown(result_shinba_good, 1600))
    print()
    
    # ケース3: 新馬戦で平凡な内容（ベースボーナスのみ）
    print("ケース3: 新馬戦で平凡な内容（上がりは平均的）")
    test_history_shinba_normal = [
        {
            'race_name': '新馬',
            'course': '京都',
            'dist': 1800,
            'chakujun': 5,
            'last_3f': 35.2,
            'weight': 54.0,
            'track_type': '芝',
            'position_4c': 8,
            'field_size': 14,
            'all_horses_results': [
                {'last_3f': 34.8, 'goal_time_diff': -0.5},
                {'last_3f': 35.0, 'goal_time_diff': -0.2},
                {'last_3f': 35.2, 'goal_time_diff': 0.0},  # 自分
                {'last_3f': 35.3, 'goal_time_diff': 0.1},  # レース平均: 約35.1秒（ほぼ同じ）
                {'last_3f': 35.5, 'goal_time_diff': 0.3},
            ]
        }
    ]
    
    result_shinba_normal = scorer.calculate_total_score(
        current_weight=54.0,
        target_course='京都',
        target_distance=1800,
        history_data=test_history_shinba_normal,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.70},
        race_pace_prediction={'pace': 'スロー'}
    )
    
    print(scorer.format_score_breakdown(result_shinba_normal, 1800))
    print()
    
    # ケース4: 2戦目以降（ブースト対象外）
    print("ケース4: 2戦目以降の馬（ブースト対象外）")
    test_history_not_shinba = [
        {
            'race_name': '未勝利',
            'course': '東京',
            'dist': 1600,
            'chakujun': 3,
            'last_3f': 33.8,
            'weight': 54.0,
            'track_type': '芝'
        },
        {
            'race_name': '新馬',
            'course': '東京',
            'dist': 1600,
            'chakujun': 5,
            'last_3f': 34.5,
            'weight': 54.0,
            'track_type': '芝'
        }
    ]
    
    result_not_shinba = scorer.calculate_total_score(
        current_weight=54.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_not_shinba,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.75},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(scorer.format_score_breakdown(result_not_shinba, 1600))
    print()
    
    # ケース5: 新馬戦4着以下（ベースボーナスなし、上がり上位のみ）
    print("ケース5: 新馬戦5着（上がりは上位）")
    test_history_shinba_5th = [
        {
            'race_name': '新馬',
            'course': '東京',
            'dist': 1600,
            'chakujun': 5,
            'last_3f': 33.5,
            'weight': 54.0,
            'track_type': '芝',
            'position_4c': 10,
            'field_size': 16,
            'all_horses_results': [
                {'last_3f': 33.2, 'goal_time_diff': -0.5},
                {'last_3f': 33.4, 'goal_time_diff': -0.3},
                {'last_3f': 33.5, 'goal_time_diff': 0.0},
                {'last_3f': 34.3, 'goal_time_diff': 0.5},  # レース平均: 約34.0秒
                {'last_3f': 34.5, 'goal_time_diff': 0.8},
            ]
        }
    ]
    
    result_shinba_5th = scorer.calculate_total_score(
        current_weight=54.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_shinba_5th,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.75},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(scorer.format_score_breakdown(result_shinba_5th, 1600))
    print()
    
    # 連続大敗ペナルティのテスト
    print("=" * 80)
    print("【連続大敗ペナルティテスト】")
    print("=" * 80)
    print()
    
    # ケース1: 2連続大敗
    print("ケース1: 2連続大敗（-8点）")
    test_history_loss_2 = [
        {
            'race_name': '1勝クラス',
            'course': '東京',
            'dist': 1600,
            'chakujun': 12,
            'last_3f': 35.5,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 1.5,  # 大敗
            'race_date': '2026/01/15'
        },
        {
            'race_name': '1勝クラス',
            'course': '中山',
            'dist': 1600,
            'chakujun': 10,
            'last_3f': 36.0,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 1.3,  # 大敗
            'race_date': '2025/12/15'
        },
        {
            'race_name': '未勝利',
            'course': '東京',
            'dist': 1600,
            'chakujun': 3,
            'last_3f': 34.0,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 0.3,
            'race_date': '2025/11/10'
        }
    ]
    
    result_loss_2 = scorer.calculate_total_score(
        current_weight=54.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_loss_2,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.70},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(scorer.format_score_breakdown(result_loss_2, 1600))
    print()
    
    # ケース2: 3連続大敗
    print("ケース2: 3連続大敗（-15点、原則消し）")
    test_history_loss_3 = [
        {
            'race_name': '1勝クラス',
            'course': '東京',
            'dist': 1600,
            'chakujun': 14,
            'last_3f': 36.0,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 1.8,  # 大敗
            'race_date': '2026/01/20'
        },
        {
            'race_name': '1勝クラス',
            'course': '中山',
            'dist': 1600,
            'chakujun': 12,
            'last_3f': 35.8,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 1.5,  # 大敗
            'race_date': '2025/12/20'
        },
        {
            'race_name': '1勝クラス',
            'course': '京都',
            'dist': 1600,
            'chakujun': 11,
            'last_3f': 35.5,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 1.2,  # 大敗
            'race_date': '2025/11/20'
        }
    ]
    
    result_loss_3 = scorer.calculate_total_score(
        current_weight=54.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_loss_3,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.70},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(scorer.format_score_breakdown(result_loss_3, 1600))
    print()
    
    # ケース3: 最新の大敗が距離変更で軽減（ペナルティなし）
    print("ケース3: 2連続大敗だが、最新の大敗が距離大幅変更（軽減でペナルティなし）")
    test_history_loss_reduced = [
        {
            'race_name': '1勝クラス',
            'course': '東京',
            'dist': 1600,
            'chakujun': 10,
            'last_3f': 35.5,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 1.3,  # 大敗
            'race_date': '2026/01/15'
        },
        {
            'race_name': '1勝クラス',
            'course': '阪神',
            'dist': 2200,  # 600m以上の距離変更
            'chakujun': 12,
            'last_3f': 36.5,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 1.5,  # 大敗
            'race_date': '2025/12/15'
        },
        {
            'race_name': '未勝利',
            'course': '中京',
            'dist': 1600,
            'chakujun': 3,
            'last_3f': 34.5,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 0.2,
            'race_date': '2025/11/10'
        }
    ]
    
    result_loss_reduced = scorer.calculate_total_score(
        current_weight=54.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_loss_reduced,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.70},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(scorer.format_score_breakdown(result_loss_reduced, 1600))
    print()
    
    # ケース4: 休養明け初戦の大敗で軽減
    print("ケース4: 休養明け初戦の大敗（軽減対象）")
    test_history_layoff_loss = [
        {
            'race_name': '1勝クラス',
            'course': '東京',
            'dist': 1600,
            'chakujun': 13,
            'last_3f': 36.0,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 1.6,  # 大敗
            'race_date': '2026/01/15'
        },
        {
            'race_name': '未勝利',
            'course': '東京',
            'dist': 1600,
            'chakujun': 2,
            'last_3f': 33.8,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 0.1,
            'race_date': '2025/08/01'  # 5ヶ月以上前
        }
    ]
    
    result_layoff_loss = scorer.calculate_total_score(
        current_weight=54.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_layoff_loss,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.70},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(scorer.format_score_breakdown(result_layoff_loss, 1600))
    print()
    
    # ケース5: 1回の大敗 + 距離変更での大敗（実質1回）
    print("ケース5: 1走前は通常の大敗、2走前は距離変更での大敗（実質1回、-3点）")
    test_history_one_loss = [
        {
            'race_name': '1勝クラス',
            'course': '東京',
            'dist': 1600,
            'chakujun': 11,
            'last_3f': 35.8,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 1.4,  # 大敗
            'race_date': '2026/01/15'
        },
        {
            'race_name': '1勝クラス',
            'course': '中山',
            'dist': 1600,  # 同距離
            'chakujun': 13,
            'last_3f': 36.2,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 1.5,  # 大敗
            'race_date': '2025/12/15'
        },
        {
            'race_name': '未勝利',
            'course': '阪神',
            'dist': 2200,  # 3走前から距離変更
            'chakujun': 2,
            'last_3f': 34.8,
            'weight': 54.0,
            'track_type': '芝',
            'goal_time_diff': 0.1,
            'race_date': '2025/11/10'
        }
    ]
    
    result_one_loss = scorer.calculate_total_score(
        current_weight=54.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_one_loss,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.70},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(scorer.format_score_breakdown(result_one_loss, 1600))
    print()
    
    print("=" * 80)
    print("テスト完了")
    print("新馬戦着順別ベースボーナス:")
    print("  1着: +5点、2着: +2.5点、3着: +1点、4着以下: 0点")
    print("  + 上がり上位（0.5秒以上速い）: +5点")
    print("  + 道中後方→直線伸び: +3点")
    print()
    print("連続大敗ペナルティ:")
    print("  1回: -3点、2連続: -8点、3連続以上: -15点")
    print("  軽減条件: 距離大幅変更（600m）、中止→大敗、休養明け初戦")
    print("  ※軽減対象の大敗が見つかると、そこで連続がリセットされます")
    print("=" * 80)
