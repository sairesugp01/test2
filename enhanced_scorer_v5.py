"""
競馬予想AI - enhanced_scorer_v5_with_late4f.py（後半4F評価・長期休養ペナルティ・斤量×タイム評価・斤量増ペナルティ・重賞出走ボーナス追加版）
最終更新: 2026年2月12日

主な改善点:
1. コース特性を考慮したペース予測
   - 直線の長さでペース判定基準を変更
   - 東京・新潟（長い直線）: 差し・追込が届きやすい
   - 中山・小倉（短い直線）: 逃げ・先行有利
   
2. コース×距離別の脚質ボーナスウェイト
   - 東京1600m: 差し20%、追込15%（従来は全て20%）
   - 中山1600m: 逃げ15%、先行20%（前残り重視）
   
3. 重賞レースでの着順評価緩和
   - G3以上の2-3着: finish_bonus 2.0（従来1.0-2.0）
   - G3以上の4-5着: finish_bonus 1.5（新設）
   
4. 上がり3F評価の改善
   - 重賞レースでは「レース質」を考慮
   - 2.0秒圏内の平均との比較を維持

5. 【新】後半4F評価の追加（V4から移植）
   - 芝中長距離（1800m以上）で後半4F評価を導入
   - 上がり3Fから後半4Fを推定し、基準値（47.2-48.3秒）と比較
   - スコアウェイト: 上がり3F 30%、後半4F 20%、距離15%、コース10%、脚質15%、斤量10%

6. 【新】長期休養明けペナルティの追加
   - 最終走からの経過日数に応じてペナルティを課す
   - 3ヶ月まで: ペナルティなし
   - 4ヶ月: -4点、5ヶ月: -6点、6ヶ月: -8点、7ヶ月: -10点
   - 8ヶ月: -11点、9ヶ月: -12点、10ヶ月: -14点、11ヶ月: -16点
   - 1年以上: 一律-20点
   - アイルシャインのような1年9ヶ月ぶりの出走での過大評価を防止

7. 【新】短距離専用の斤量×タイム評価（V4から移植）
   - 1600m以下で「斤量×タイム評価」を導入
   - 高斤量（57-58kg）で速い上がり3Fを高評価
   - スコアウェイト: 上がり3F 30%、斤量×タイム 30%、距離15%、コース10%、脚質15%

8. 【新】斤量増ペナルティの全距離適用
   - 前走または過去3走平均より斤量が増えた場合にペナルティ
   - 短距離（1200m以下）: 1kgあたり-1.5点
   - マイル（1400-1600m）: 1kgあたり-2.0点
   - 中距離（1800m）: 1kgあたり-2.5点
   - 長距離（2000m以上）: 1kgあたり-3.0点
   - 例: 前走56kg → 今回58kg（+2kg）の場合、短距離で-3～-4点のペナルティ

9. 【新】重賞出走ボーナスの追加
   - G1出走: +10点（1着）、+8点（2-3着）、+5点（4着以下）
   - G2出走: +7点（1着）、+5点（2-3着）、+3点（4着以下）
   - G3出走: +5点（1着）、+3点（2-3着）、+2点（4着以下）
   - 過去5走まで評価、時間減衰あり
   - G1出走実績のある馬を適切に評価
"""

import logging
from typing import List, Dict, Optional, Tuple
from collections import Counter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CourseAnalyzer:
    """コース分析クラス"""
    
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
            1400: {'逃げ': 0.10, '先行': 0.15, '差し': 0.15, '追込': 0.10},
            1600: {'逃げ': 0.08, '先行': 0.12, '差し': 0.15, '追込': 0.10},
            1800: {'逃げ': 0.05, '先行': 0.10, '差し': 0.12, '追込': 0.08},
            2000: {'逃げ': 0.05, '先行': 0.10, '差し': 0.12, '追込': 0.08},
        },
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
    def predict_race_pace(horses_running_styles: List[Dict], field_size: int = None, 
                         course: str = None) -> Dict:
        """
        【改善版】レース展開を予測（コース特性を考慮）
        
        改善点:
        1. コース特性（直線の長さ）を考慮
        2. 逃げ馬の質（信頼度）を重視
        3. 前残り率でペースを判定
        
        Args:
            horses_running_styles: 各馬の脚質情報リスト
            field_size: 出走頭数
            course: コース名（例: '東京'）
        """
        if not horses_running_styles:
            return {
                'pace': 'ミドル', 
                'front_ratio': 0.0, 
                'confidence': 0.0,
                'front_runners': 0,
                'field_size': field_size or 0,
                'straight_length': 400,
                'distribution': {'逃げ': 0, '先行': 0, '差し': 0, '追込': 0}
            }
        
        if field_size is None:
            field_size = len(horses_running_styles)
        
        # 脚質ごとのカウント
        style_counts = Counter(h['style'] for h in horses_running_styles if h.get('style'))
        
        # 前走組（逃げ+先行）の数と割合
        front_runners = style_counts['逃げ'] + style_counts['先行']
        front_ratio = front_runners / field_size if field_size > 0 else 0.0
        
        # 逃げ馬の質を評価
        escapers = [h for h in horses_running_styles if h.get('style') == '逃げ']
        strong_escaper = any(h.get('confidence', 0) >= 0.8 for h in escapers)
        
        # 【新】コース特性を考慮
        straight_length = 400  # デフォルト
        if course and course in RunningStyleAnalyzer.COURSE_CHARACTERISTICS:
            straight_length = RunningStyleAnalyzer.COURSE_CHARACTERISTICS[course]['straight']
        
        # 【改善】直線の長さに応じて判定基準を変更
        if straight_length >= 500:  # 東京・新潟（長い直線）
            # 長い直線→差し・追込が届きやすい
            if field_size >= 16:
                if front_ratio < 0.30:
                    pace = 'ハイ'
                elif front_ratio < 0.50:
                    pace = 'ミドル'
                else:
                    pace = 'スロー'
            else:
                if front_ratio < 0.25:
                    pace = 'ハイ'
                elif front_ratio < 0.45:
                    pace = 'ミドル'
                else:
                    pace = 'スロー'
        
        elif straight_length <= 350:  # 中山・小倉・福島（短い直線）
            # 短い直線→前残りしやすい
            if field_size >= 16:
                if front_ratio < 0.20:
                    pace = 'ハイ'
                elif front_ratio < 0.40:
                    pace = 'ミドル'
                else:
                    pace = 'スロー'
            else:
                if front_ratio < 0.15:
                    pace = 'ハイ'
                elif front_ratio < 0.35:
                    pace = 'ミドル'
                else:
                    pace = 'スロー'
        
        else:  # 京都・阪神・中京（標準的な直線）
            # 従来の判定ロジック
            if field_size >= 16:
                if front_ratio < 0.25:
                    pace = 'ハイ'
                elif front_ratio < 0.45:
                    pace = 'ミドル'
                else:
                    pace = 'スロー'
            else:
                if front_ratio < 0.20:
                    pace = 'ハイ'
                elif front_ratio < 0.40:
                    pace = 'ミドル'
                else:
                    pace = 'スロー'
        
        # 逃げ馬の質による補正
        if strong_escaper and style_counts['逃げ'] == 1:
            if pace == 'スロー':
                pace = 'ミドル'
        
        confidence = min(len(horses_running_styles) / field_size, 1.0) if field_size > 0 else 0.0
        
        # 【追加】後方互換性のためdistributionを含める
        distribution = {
            '逃げ': style_counts.get('逃げ', 0),
            '先行': style_counts.get('先行', 0),
            '差し': style_counts.get('差し', 0),
            '追込': style_counts.get('追込', 0)
        }
        
        return {
            'pace': pace,
            'front_ratio': round(front_ratio, 2),
            'front_runners': front_runners,
            'field_size': field_size,
            'confidence': round(confidence, 2),
            'straight_length': straight_length,
            'distribution': distribution  # 追加
        }
    
    @staticmethod
    def calculate_style_match_bonus(style: str, pace: str, course: str, distance: int) -> float:
        """
        【改善版】脚質×展開×コース特性の適合度ボーナス
        
        改善点:
        - コース×距離別のウェイトを使用
        - 直線の長さを考慮
        """
        if not style or not pace:
            return 0.0
        
        bonus = 0.0
        
        # 基本ボーナス（ペース×脚質）
        if pace == 'ハイ':
            if style in ['差し', '追込']:
                bonus += 8.0
            elif style == '先行':
                bonus += 3.0
        elif pace == 'スロー':
            if style in ['逃げ', '先行']:
                bonus += 8.0
            elif style == '差し':
                bonus += 3.0
        elif pace == 'ミドル':
            if style in ['先行', '差し']:
                bonus += 5.0
            else:
                bonus += 2.0
        
        # 【新】直線の長さによる補正
        if course in RunningStyleAnalyzer.COURSE_CHARACTERISTICS:
            straight = RunningStyleAnalyzer.COURSE_CHARACTERISTICS[course]['straight']
            
            if straight >= 500:  # 長い直線
                if style in ['差し', '追込'] and pace in ['ハイ', 'ミドル']:
                    bonus += 5.0
                elif style in ['逃げ', '先行'] and pace == 'スロー':
                    bonus += 3.0
            
            elif straight <= 350:  # 短い直線
                if style in ['逃げ', '先行']:
                    bonus += 5.0
                elif style in ['差し', '追込'] and pace == 'ハイ':
                    bonus -= 2.0  # 届きにくい
        
        return bonus
    
    @staticmethod
    def get_style_weight(course: str, distance: int, style: str) -> float:
        """
        【新設】コース×距離別の脚質ボーナスウェイトを取得
        
        Returns:
            脚質ボーナスのウェイト（0.0～0.20）
        """
        # コース別ウェイトを取得
        course_weights = RunningStyleAnalyzer.COURSE_DISTANCE_STYLE_WEIGHTS.get(course, {})
        
        if not course_weights:
            # デフォルトは距離のみで判定（従来方式）
            if distance <= 1600:
                return 0.20
            elif distance <= 2200:
                return 0.10
            else:
                return 0.05
        
        # 距離に最も近いウェイトを取得
        distances = sorted(course_weights.keys())
        closest_distance = min(distances, key=lambda d: abs(d - distance))
        distance_weights = course_weights[closest_distance]
        
        return distance_weights.get(style, 0.10)


class EnhancedRaceScorer:
    """競馬レーススコアラー（東京新聞杯対応版）"""
    
    central_courses = ['東京', '中山', '京都', '阪神', '小倉', '新潟', '中京', '札幌', '函館', '福島']
    local_dirt_courses = ['大井', '川崎', '船橋', '浦和', '盛岡', '水沢', '門別', '帯広', '笠松', '金沢', '名古屋', '園田', '姫路', '高知', '佐賀']
    local_turf_courses = []
    
    def __init__(self, debug_mode: bool = False):
        self.debug_mode = debug_mode
        self.course_analyzer = CourseAnalyzer()
        self.style_analyzer = RunningStyleAnalyzer()
    
    def detect_race_grade(self, race_name: str) -> Tuple[str, float]:
        """レースグレードを判定"""
        if 'G1' in race_name or 'GⅠ' in race_name or 'GI' in race_name:
            return ('G1', 1.2)
        if 'G2' in race_name or 'GⅡ' in race_name or 'GII' in race_name:
            return ('G2', 1.1)
        if 'G3' in race_name or 'GⅢ' in race_name or 'GIII' in race_name:
            return ('G3', 1.0)
        if 'OP' in race_name or 'オープン' in race_name or 'リステッド' in race_name or 'L' == race_name.strip():
            return ('OP', 0.9)
        if '1600万' in race_name or '3勝' in race_name:
            return ('3勝', 0.85)
        if '1000万' in race_name or '2勝' in race_name:
            return ('2勝', 0.80)
        if '500万' in race_name or '1勝' in race_name:
            return ('1勝', 0.75)
        if '未勝利' in race_name or '新馬' in race_name:
            return ('未勝利', 0.70)
        return ('その他', 0.65)
    
    def _is_local_race(self, race_name: str, course: str) -> bool:
        """地方レースかどうかを判定（交流重賞は除外）"""
        # 交流重賞（JpnI/II/III）は地方競馬場開催でもJRA相当として扱う
        if self._is_kouryu_grade_race(race_name):
            return False
        
        if course in self.local_dirt_courses or course in self.local_turf_courses:
            return True
        if any(marker in race_name for marker in ['C1', 'C2', 'C3', 'B1', 'B2', 'B3', 'A1', 'A2']):
            return True
        return False
    
    def _is_kouryu_grade_race(self, race_name: str) -> bool:
        """交流重賞（JpnI/II/III）かどうかを判定"""
        # Jpnグレード表記がある場合
        if any(grade in race_name for grade in ['JpnI', 'JpnII', 'JpnIII', 'Jpn1', 'Jpn2', 'Jpn3']):
            return True
        
        # 主要交流重賞（レース名で判定）
        kouryu_races = [
            '帝王賞', '東京大賞典', 'かしわ記念', 'JBCクラシック', 'JBCスプリント', 'JBCレディスクラシック',
            'ジャパンダートダービー', 'エンプレス杯', 'マリーンC', 'スパーキングレディーC',
            'さきたま杯', 'ブリーダーズゴールドC', 'ダイオライト記念', '名古屋グランプリ',
            '黒船賞', 'マーキュリーC', 'ウィナーズカップ', 'ジャパンブリーダーズカップ',
            'TCK女王盃', 'クラスターC', '東京スプリント', '全日本2歳優駿', 'ローレル賞'
        ]
        
        return any(race in race_name for race in kouryu_races)
    
    def _get_track_type_by_distance(self, distance: int, race_name: str, course: str) -> str:
        """距離とレース名からトラックタイプを判定"""
        if 'ダ' in race_name or 'ダート' in race_name:
            return 'ダート'
        if '芝' in race_name:
            return '芝'
        
        if course in self.local_dirt_courses:
            return 'ダート'
        
        if course in self.local_turf_courses:
            return 'ダート' if distance <= 1400 else '芝'
        
        if course in self.central_courses:
            return '芝'
        
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
    
    def calculate_last_3f_relative_score(self, history_data: List[Dict], target_track_type: str = "芝", 
                                        target_course: str = None, target_distance: int = None, 
                                        target_baba: str = "良") -> float:
        """
        【改善版】上がり3F相対評価（重賞評価を緩和）
        
        改善点:
        - 重賞（G3以上）の2-3着: finish_bonus 2.0
        - 重賞（G3以上）の4-5着: finish_bonus 1.5（新設）
        """
        if not history_data:
            return 0.0
        
        score = 0.0
        THRESHOLD = 2.0
        
        for idx, race in enumerate(history_data[:3]):
            my_last_3f = race.get('last_3f', 0.0)
            if my_last_3f <= 0:
                continue
            
            race_name = race.get('race_name', '')
            course = race.get('course', '')
            distance = race.get('dist', 2000)
            baba = race.get('baba', '良')
            distance_text = race.get('distance_text', '')
            
            race_track_type = race.get('track_type', None)
            if not race_track_type or race_track_type == '不明':
                race_track_type = self._get_track_type_by_distance(distance, race_name, course)
            track_type_mismatch = (race_track_type != target_track_type)
            is_local = self._is_local_race(race_name, course)
            
            # 2.0秒圏内の馬の平均上がり3Fを計算
            all_horses_results = race.get('all_horses_results', [])
            
            if not all_horses_results:
                race_avg_3f = race.get('race_avg_last_3f', 0.0)
                
                if race_avg_3f <= 0:
                    if race_track_type == '芝' and course in self.central_courses:
                        race_avg_3f = self.course_analyzer.get_baseline_3f(course, distance, distance_text, baba)
                    else:
                        race_avg_3f = self._get_default_baseline_3f(distance, race_track_type)
                        if is_local:
                            race_avg_3f += 1.0 if race_track_type == 'ダート' else 0.5
                
                comparison_type = "レース基準値"
            else:
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
                    valid_3f = [h['last_3f'] for h in all_horses_results if h.get('last_3f', 0) > 0]
                    race_avg_3f = sum(valid_3f) / len(valid_3f) if valid_3f else self._get_default_baseline_3f(distance, race_track_type)
                    comparison_type = "レース全体平均（圏内なし）"
            
            chakujun = race.get('chakujun', 99)
            speed_diff = race_avg_3f - my_last_3f
            
            # 短距離は1.3秒以内の基準で評価（修正: 値を下げる）
            if distance <= 1400:
                if speed_diff >= 1.3:
                    base_points = 15.0  # 25.0 → 15.0
                elif speed_diff >= 0.8:
                    base_points = 12.0  # 20.0 → 12.0
                elif speed_diff >= 0.4:
                    base_points = 8.0   # 15.0 → 8.0
                elif speed_diff >= 0.0:
                    base_points = 5.0   # 10.0 → 5.0
                else:
                    base_points = -3.0  # -5.0 → -3.0
            else:
                if speed_diff >= 1.5:
                    base_points = 15.0  # 25.0 → 15.0
                elif speed_diff >= 1.0:
                    base_points = 12.0  # 20.0 → 12.0
                elif speed_diff >= 0.5:
                    base_points = 8.0   # 15.0 → 8.0
                elif speed_diff >= 0.0:
                    base_points = 5.0   # 10.0 → 5.0
                else:
                    base_points = -3.0  # -5.0 → -3.0
            
            # 【改善】重賞レースでの着順評価を緩和
            grade, base_reliability = self.detect_race_grade(race_name)
            
            if grade in ['G1', 'G2', 'G3']:
                # 重賞なら2-5着でも一定の評価
                if chakujun == 1:
                    finish_bonus = 3.0
                elif chakujun in [2, 3]:
                    finish_bonus = 2.0  # 従来1.0-2.0 → 2.0に統一
                elif chakujun in [4, 5]:
                    finish_bonus = 1.5  # 新設
                else:
                    finish_bonus = 0.0
            else:
                # 通常のレース
                if chakujun == 1:
                    finish_bonus = 3.0
                elif chakujun == 2:
                    finish_bonus = 2.0
                elif chakujun == 3:
                    finish_bonus = 1.0
                else:
                    finish_bonus = 0.0
            
            # 【修正】ポイント計算を適正化（乗算→加算）
            if speed_diff > 0 and finish_bonus > 0:
                # 上がりが速く、好着順の場合
                points = base_points + (finish_bonus * 5.0)  # 修正: 乗算→加算
            elif speed_diff > 0:
                # 上がりは速いが着順が悪い
                points = base_points * 0.5  # 修正: 係数を下げる
            elif speed_diff <= 0 and finish_bonus > 0:
                # 上がりは遅いが着順は良い
                points = finish_bonus * 3.0  # 修正: 係数を下げる
            else:
                # 上がりも遅く着順も悪い
                points = -3.0  # 修正: ペナルティを緩和
            
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
                            target_baba: str = "良") -> Dict:
        """総合スコアを計算"""
        
        # 1. 上がり3F相対評価
        last_3f_score = self.calculate_last_3f_relative_score(
            history_data, target_track_type, target_course, target_distance, target_baba
        )
        
        # 2. 距離適性スコア
        distance_score = self._calculate_distance_score(history_data, target_distance)
        
        # 3. コース適性スコア
        course_score = self._calculate_course_score(history_data, target_course)
        
        # 4. 斤量評価（全距離で適用）
        # 短距離: weight_time_score（高斤量×速いタイムを評価）
        # 全距離: weight_penalty（斤量増加のペナルティ）
        if target_distance <= 1600:
            weight_time_score = self._calculate_weight_time_score(current_weight, history_data, target_distance, target_track_type)
        else:
            weight_time_score = 0.0
        
        # 斤量増ペナルティは全距離で適用
        weight_penalty = self._calculate_weight_penalty(history_data, current_weight, target_distance)
        
        # 5. 【新】後半4F評価（芝中長距離のみ）
        late_4f_score = self._calculate_late_4f_score(history_data, target_distance, target_track_type)
        
        # 6. 【新】長期休養ペナルティ
        layoff_penalty = self._calculate_layoff_penalty(history_data)
        
        # 7. 【新】重賞出走ボーナス
        grade_race_bonus = self._calculate_grade_race_bonus(history_data)
        
        # 8. 【改善】脚質ボーナス（コース×距離別ウェイト使用）
        style_bonus = 0.0
        if running_style_info and race_pace_prediction:
            style = running_style_info.get('style', '')
            confidence = running_style_info.get('confidence', 0.0)
            pace = race_pace_prediction.get('pace', 'ミドル')
            
            raw_bonus = self.style_analyzer.calculate_style_match_bonus(style, pace, target_course, target_distance)
            
            # 【新】コース×距離別のウェイトを取得
            style_weight = self.style_analyzer.get_style_weight(target_course, target_distance, style)
            
            style_bonus = raw_bonus * confidence * style_weight
            
            if self.debug_mode:
                logger.debug(f"  脚質ボーナス: {style}×{pace}×{target_course}{target_distance}m")
                logger.debug(f"    生ボーナス{raw_bonus:+.1f} × 信頼度{confidence:.2f} × ウェイト{style_weight:.2f} = {style_bonus:+.2f}")
        
        # 8. 危険フラグ
        danger_flags = self._check_danger_flags(history_data, target_course, target_track_type)
        
        # 9. 危険フラグペナルティ
        danger_penalty = 0.0
        if danger_flags['local_to_jra']:
            danger_penalty -= 15.0
        
        # 【修正】各スコアを正規化してウェイトを適用
        # 距離別にウェイトを調整
        is_long_distance = target_distance >= 1800 and target_track_type == "芝"
        
        # 上がり3Fスコアを0-100点に正規化（最大150点想定）
        normalized_3f = min(last_3f_score / 150.0 * 100, 100)
        
        # 距離適性を0-100点に正規化（最大15点想定）
        normalized_distance = min(distance_score / 15.0 * 100, 100)
        
        # コース適性を0-100点に正規化（最大15点想定）
        normalized_course = min(course_score / 15.0 * 100, 100)
        
        # 脚質ボーナスを0-100点に正規化（最大20点想定）
        normalized_style = min(style_bonus / 20.0 * 100, 100)
        
        # 後半4Fスコアを0-100点に正規化（最大50点想定）
        normalized_late_4f = min(late_4f_score / 50.0 * 100, 100) if late_4f_score != 0 else 0
        
        # 【新】weight_time_scoreを0-100点に正規化（最大30点想定）
        normalized_weight_time = min(weight_time_score / 30.0 * 100, 100) if weight_time_score != 0 else 0
        
        # ウェイトを適用して総合スコア計算
        if is_long_distance:
            # 芝中長距離（1800m以上）: 後半4F評価を重視
            weight_3f = 0.30
            weight_late_4f = 0.20
            weight_distance = 0.15
            weight_course = 0.10
            weight_style = 0.15
            weight_penalty_factor = 0.10
            
            total = (
                normalized_3f * weight_3f +
                normalized_late_4f * weight_late_4f +
                normalized_distance * weight_distance +
                normalized_course * weight_course +
                normalized_style * weight_style +
                weight_penalty +  # 斤量ペナルティは直接減算
                layoff_penalty +  # 長期休養ペナルティも直接減算
                grade_race_bonus +  # 【新】重賞出走ボーナス
                danger_penalty    # 危険フラグペナルティ
            )
        else:
            # 短距離（1600m以下）: weight_time_scoreを使用
            weight_3f = 0.30
            weight_late_4f = 0.0
            weight_weight_time = 0.30  # 【新】斤量×タイム評価
            weight_distance = 0.15
            weight_course = 0.10
            weight_style = 0.15
            weight_penalty_factor = 0.0
            
            total = (
                normalized_3f * weight_3f +
                normalized_weight_time * weight_weight_time +  # 【新】
                normalized_distance * weight_distance +
                normalized_course * weight_course +
                normalized_style * weight_style +
                weight_penalty +  # 【新】斤量増ペナルティ（全距離適用）
                layoff_penalty +  # 長期休養ペナルティは直接減算
                grade_race_bonus +  # 【新】重賞出走ボーナス
                danger_penalty    # 危険フラグペナルティ
            )
        
        breakdown = {
            'last_3f_score': last_3f_score,
            'last_3f_normalized': round(normalized_3f, 1),
            'late_4f_score': late_4f_score,
            'late_4f_normalized': round(normalized_late_4f, 1),
            'weight_time_score': weight_time_score,  # 【新】短距離用
            'weight_time_normalized': round(normalized_weight_time, 1),
            'distance_score': distance_score,
            'distance_normalized': round(normalized_distance, 1),
            'course_score': course_score,
            'course_normalized': round(normalized_course, 1),
            'weight_penalty': weight_penalty,  # 中長距離用
            'layoff_penalty': layoff_penalty,
            'grade_race_bonus': grade_race_bonus,  # 【新】重賞出走ボーナス
            'danger_penalty': danger_penalty,  # 危険フラグペナルティ
            'style_bonus': style_bonus,
            'style_normalized': round(normalized_style, 1),
            'weights': {
                'last_3f': weight_3f,
                'late_4f': weight_late_4f,
                'weight_time': weight_weight_time if target_distance <= 1600 else 0.0,
                'distance': weight_distance,
                'course': weight_course,
                'running_style': weight_style,
                'weight_penalty': weight_penalty_factor
            }
        }
        
        return {
            'total_score': round(total, 1),
            'is_dangerous': danger_flags['is_dangerous'],
            'danger_flags': danger_flags,
            'breakdown': breakdown
        }
    
    def _calculate_distance_score(self, history_data: List[Dict], target_distance: int) -> float:
        """距離適性スコア"""
        score = 0.0
        for race in history_data[:3]:
            dist_diff = abs(race.get('dist', 0) - target_distance)
            chakujun = race.get('chakujun', 99)
            
            if dist_diff <= 200:
                bonus = 5.0 if chakujun <= 3 else 2.0
            elif dist_diff <= 400:
                bonus = 3.0 if chakujun <= 3 else 1.0
            else:
                bonus = 0.0
            
            score += bonus
        
        return round(score, 1)
    
    def _calculate_course_score(self, history_data: List[Dict], target_course: str) -> float:
        """コース適性スコア"""
        score = 0.0
        for race in history_data[:3]:
            course = race.get('course', '')
            chakujun = race.get('chakujun', 99)
            
            if course == target_course:
                bonus = 5.0 if chakujun <= 3 else 2.0
                score += bonus
        
        return round(score, 1)
    
    def _calculate_layoff_penalty(self, history_data: List[Dict]) -> float:
        """長期休養明けペナルティ
        
        最終走からの経過日数に応じてペナルティを課す
        - 4ヶ月未満: ペナルティなし
        - 4-7ヶ月: 段階的にペナルティ増加
        - 1年以上: 一律-20点
        """
        if not history_data:
            return 0.0
        
        latest_race = history_data[0]
        race_date = latest_race.get('race_date', '') or latest_race.get('date', '')
        
        # race_dateがない場合はペナルティなし
        if not race_date:
            return 0.0
        
        # 日付の解析（複数フォーマット対応）
        from datetime import datetime, timedelta
        try:
            if isinstance(race_date, str):
                # "2024/01/15", "2024-01-15", "2024.01.15" 形式に対応
                race_date = race_date.replace('.', '/').replace('-', '/')
                race_datetime = datetime.strptime(race_date, '%Y/%m/%d')
            else:
                return 0.0
            
            # 現在日時（2026年2月11日を基準）
            current_date = datetime(2026, 2, 11)
            days_since_last_race = (current_date - race_datetime).days
            
            # 日数に応じたペナルティ（細分化版）
            if days_since_last_race >= 365:  # 1年以上は一律
                penalty = -20.0
            elif days_since_last_race >= 330:  # 11ヶ月以上（330日）
                penalty = -16.0
            elif days_since_last_race >= 300:  # 10ヶ月以上（300日）
                penalty = -14.0
            elif days_since_last_race >= 270:  # 9ヶ月以上（270日）
                penalty = -12.0
            elif days_since_last_race >= 240:  # 8ヶ月以上（240日）
                penalty = -11.0
            elif days_since_last_race >= 210:  # 7ヶ月以上（210日）
                penalty = -10.0
            elif days_since_last_race >= 180:  # 6ヶ月以上（180日）
                penalty = -8.0
            elif days_since_last_race >= 150:  # 5ヶ月以上（150日）
                penalty = -6.0
            elif days_since_last_race >= 120:  # 4ヶ月以上（120日）
                penalty = -4.0
            else:  # 4ヶ月未満（3ヶ月以下）はペナルティなし
                penalty = 0.0
            
            return penalty
            
        except (ValueError, AttributeError):
            return 0.0
    
    def _calculate_grade_race_bonus(self, history_data: List[Dict]) -> float:
        """重賞出走ボーナス
        
        過去のレース出走歴に基づいてボーナスを付与
        - G1出走: +10点（1着）、+8点（2-3着）、+5点（4着以下）
        - G2出走: +7点（1着）、+5点（2-3着）、+3点（4着以下）
        - G3出走: +5点（1着）、+3点（2-3着）、+2点（4着以下）
        - 最大5走まで評価（時間減衰あり）
        """
        if not history_data:
            return 0.0
        
        bonus = 0.0
        
        for idx, race in enumerate(history_data[:5]):  # 過去5走まで見る
            race_name = race.get('race_name', '')
            chakujun = race.get('chakujun', 99)
            grade, _ = self.detect_race_grade(race_name)
            
            # 重賞のみ評価
            if grade not in ['G1', 'G2', 'G3']:
                continue
            
            # グレード別のボーナス
            if grade == 'G1':
                if chakujun == 1:
                    race_bonus = 10.0
                elif chakujun in [2, 3]:
                    race_bonus = 8.0
                elif chakujun in [4, 5]:
                    race_bonus = 6.0
                else:
                    race_bonus = 5.0  # 出走しただけでも評価
            elif grade == 'G2':
                if chakujun == 1:
                    race_bonus = 7.0
                elif chakujun in [2, 3]:
                    race_bonus = 5.0
                elif chakujun in [4, 5]:
                    race_bonus = 4.0
                else:
                    race_bonus = 3.0
            else:  # G3
                if chakujun == 1:
                    race_bonus = 5.0
                elif chakujun in [2, 3]:
                    race_bonus = 3.0
                elif chakujun in [4, 5]:
                    race_bonus = 2.5
                else:
                    race_bonus = 2.0
            
            # 時間減衰（新しい方が重視）
            time_decay = 1.0 - (idx * 0.10)
            bonus += race_bonus * time_decay
            
            if self.debug_mode:
                logger.debug(f"  重賞出走ボーナス: {race_name} {grade} {chakujun}着 → +{race_bonus * time_decay:.1f}点")
        
        return round(bonus, 1)
    
    def _calculate_late_4f_score(self, history_data: List[Dict], target_distance: int, 
                                  target_track_type: str) -> float:
        """後半4F評価（芝中長距離専用）"""
        if target_track_type != "芝" or target_distance < 1800:
            return 0.0
        
        # 距離別の後半4F基準値
        BASELINE_4F = 47.2 if target_distance <= 2000 else 47.8 if target_distance <= 2400 else 48.3
        score = 0.0
        
        for idx, race in enumerate(history_data[:3]):
            distance = race.get('dist', 0)
            race_name = race.get('race_name', '')
            course = race.get('course', '')
            
            # トラックタイプを取得
            race_track_type = race.get('track_type', None)
            if not race_track_type or race_track_type == '不明':
                race_track_type = self._get_track_type_by_distance(distance, race_name, course)
            
            # 芝中長距離レースのみ評価
            if race_track_type != '芝' or distance < 1800:
                continue
            
            # 地方競馬は評価対象外
            if self._is_local_race(race_name, course):
                continue
            
            last_3f = race.get('last_3f', 0.0)
            if last_3f <= 0:
                continue
            
            # 後半4Fを推定（上がり3Fから換算）
            estimated_late_4f = last_3f * (4.0 / 3.0) + 0.4
            diff_from_baseline = BASELINE_4F - estimated_late_4f
            
            # 差分に応じた倍率適用
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
            
            # レース格による信頼度
            grade, _ = self.detect_race_grade(race_name)
            reliability_map = {
                'G1': 1.0, 'G2': 0.95, 'G3': 0.9,
                'JpnI': 1.0, 'JpnII': 0.95, 'JpnIII': 0.9,
                'OP': 0.85
            }
            reliability = reliability_map.get(grade, 0.7)
            
            # 時間減衰
            time_decay = 1.0 - (idx * 0.15)
            
            # 着順ボーナス
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
    
    def _calculate_weight_time_score(self, current_weight: float, history_data: List[Dict], 
                                     target_distance: int, target_track_type: str = "芝") -> float:
        """斤量-タイム評価（短距離1600m以下専用）"""
        if not history_data:
            return 0.0
        
        score = 0.0
        
        for idx, race in enumerate(history_data[:3]):
            distance = race.get('dist', 0)
            weight = race.get('weight', 0.0)
            last_3f = race.get('last_3f', 0.0)
            race_name = race.get('race_name', '')
            course = race.get('course', '')
            
            # 短距離（1000-1600m）のみ評価
            if distance > 1600 or distance < 1000:
                continue
            if weight <= 0 or last_3f <= 0:
                continue
            
            # トラックタイプを取得
            race_track_type = race.get('track_type', None)
            if not race_track_type or race_track_type == '不明':
                race_track_type = self._get_track_type_by_distance(distance, race_name, course)
            
            is_local = self._is_local_race(race_name, course)
            time_decay = 1.0 - (idx * 0.25)
            
            # 距離別の基準値設定
            if distance <= 1200:
                BASE_3F, WEIGHT_THRESHOLD = 34.0, 56.0
            elif distance <= 1400:
                BASE_3F, WEIGHT_THRESHOLD = 34.3, 55.5
            else:  # 1401～1600m
                BASE_3F, WEIGHT_THRESHOLD = 34.5, 55.0
            
            # 地方戦の基準値調整
            if is_local:
                BASE_3F += 1.2 if race_track_type == 'ダート' else 0.5
            
            # 斤量ボーナス
            if weight >= 58.0:
                weight_bonus = 8.0
            elif weight >= 57.0:
                weight_bonus = 5.0
            elif weight >= WEIGHT_THRESHOLD:
                weight_bonus = 2.0
            elif weight <= 52.0:
                weight_bonus = -3.0
            else:
                weight_bonus = 0.0
            
            if is_local:
                weight_bonus *= 0.5
            
            # スピードボーナス
            adjusted_base = BASE_3F + (0.3 if weight >= 57.0 else 0.1 if weight >= 55.0 else 0)
            diff = adjusted_base - last_3f
            
            if diff >= 1.0:
                speed_bonus = 6.0
            elif diff >= 0.5:
                speed_bonus = 4.0
            elif diff >= 0.0:
                speed_bonus = 2.0
            elif diff >= -0.5:
                speed_bonus = 0.0
            else:
                speed_bonus = -3.0
            
            if is_local:
                speed_bonus *= 0.6
            
            # コンボボーナス
            combo_bonus = 0.0
            if weight >= 57.0 and last_3f < BASE_3F - 0.5:
                combo_bonus = 8.0 if last_3f < BASE_3F - 1.0 else 5.0
            elif weight >= 55.0 and last_3f < BASE_3F:
                combo_bonus = 2.0
            elif weight <= 54.0 and last_3f > BASE_3F + 0.5:
                combo_bonus = -5.0
            
            if is_local:
                combo_bonus *= 0.5
            
            # レーススコア計算
            race_score = (weight_bonus + speed_bonus + combo_bonus) * time_decay
            
            # 着順による調整
            chakujun = race.get('chakujun', 99)
            if chakujun == 1:
                race_score *= 1.2
            elif chakujun <= 3:
                race_score *= 1.0
            elif chakujun <= 5:
                race_score *= 0.85
            elif chakujun <= 10:
                race_score *= 0.6
            else:
                race_score *= 0.3
            
            # トラックタイプ不一致ペナルティ
            if race_track_type != target_track_type:
                race_score *= 0.2
            
            score += race_score
        
        return round(score, 1)
    
    def _calculate_weight_penalty(self, history_data: List[Dict], current_weight: float, 
                                  target_distance: int) -> float:
        """斤量ペナルティ（全距離適用）
        
        前走または過去3走平均より斤量が増えた場合にペナルティ
        """
        if not history_data:
            return 0.0
        
        # 前走の斤量を取得
        prev_weight = history_data[0].get('weight', current_weight)
        
        # 過去3走の平均斤量も計算
        recent_weights = [r.get('weight', current_weight) for r in history_data[:3]]
        avg_weight = sum(recent_weights) / len(recent_weights)
        
        # 前走との差分と平均との差分の大きい方を使用
        prev_diff = current_weight - prev_weight
        avg_diff = current_weight - avg_weight
        weight_diff = max(prev_diff, avg_diff)
        
        # 斤量が増えた場合のみペナルティ
        if weight_diff <= 0:
            return 0.0
        
        # 距離別のペナルティレート
        if target_distance >= 2000:
            penalty_rate = -3.0  # 長距離は厳しく
        elif target_distance >= 1800:
            penalty_rate = -2.5
        elif target_distance >= 1400:
            penalty_rate = -2.0
        else:
            penalty_rate = -1.5  # 短距離は軽め
        
        return round(weight_diff * penalty_rate, 1)
    
    def _check_danger_flags(self, history_data: List[Dict], target_course: str, 
                           target_track_type: str) -> Dict:
        """危険フラグをチェック"""
        flags = {
            'is_dangerous': False,
            'local_to_jra': False,
            'track_switch_dart_to_turf': False,
            'reasons': []
        }
        
        if not history_data:
            return flags
        
        # 地方→JRA転換
        recent_local = sum(1 for r in history_data[:3] 
                          if self._is_local_race(r.get('race_name', ''), r.get('course', '')))
        if recent_local >= 2 and target_course in self.central_courses:
            flags['local_to_jra'] = True
            flags['reasons'].append('地方馬のJRA復帰')
        
        # ダート→芝転換
        recent_track = history_data[0].get('track_type', '')
        if not recent_track:
            recent_track = self._get_track_type_by_distance(
                history_data[0].get('dist', 0),
                history_data[0].get('race_name', ''),
                history_data[0].get('course', '')
            )
        
        if recent_track == 'ダート' and target_track_type == '芝':
            flags['track_switch_dart_to_turf'] = True
            flags['reasons'].append('ダート→芝転換')
        
        flags['is_dangerous'] = flags['local_to_jra'] or flags['track_switch_dart_to_turf']
        
        return flags
    
    def format_score_breakdown(self, result: Dict, target_distance: int) -> str:
        """スコア内訳をフォーマット"""
        breakdown = result['breakdown']
        weights = breakdown['weights']
        
        output = []
        output.append(f"総合スコア: {result['total_score']:.1f}点")
        output.append(f"  上がり3F評価: {breakdown['last_3f_score']:+.1f} (ウェイト{weights['last_3f']:.0%})")
        
        # 後半4F評価（芝中長距離のみ）
        if breakdown.get('late_4f_score', 0) != 0:
            output.append(f"  後半4F評価: {breakdown['late_4f_score']:+.1f} (ウェイト{weights['late_4f']:.0%})")
        
        # 斤量×タイム評価（短距離のみ）
        if target_distance <= 1600 and breakdown.get('weight_time_score', 0) != 0:
            output.append(f"  斤量×タイム評価: {breakdown['weight_time_score']:+.1f} (ウェイト{weights['weight_time']:.0%})")
        
        output.append(f"  距離適性: {breakdown['distance_score']:+.1f} (ウェイト{weights['distance']:.0%})")
        output.append(f"  コース適性: {breakdown['course_score']:+.1f} (ウェイト{weights['course']:.0%})")
        
        # 斤量増ペナルティ（全距離で表示）
        if breakdown.get('weight_penalty', 0) != 0:
            output.append(f"  斤量増ペナルティ: {breakdown['weight_penalty']:+.1f}")
        
        # 長期休養ペナルティ
        if breakdown.get('layoff_penalty', 0) != 0:
            output.append(f"  ⚠️ 長期休養ペナルティ: {breakdown['layoff_penalty']:+.1f}")
        
        # 危険フラグペナルティ
        if breakdown.get('danger_penalty', 0) != 0:
            output.append(f"  ⚠️ 危険フラグペナルティ: {breakdown['danger_penalty']:+.1f}")
        
        if breakdown.get('style_bonus', 0) != 0:
            output.append(f"  脚質ボーナス: {breakdown['style_bonus']:+.1f} (ウェイト{weights['running_style']:.0%})")
        
        if result['is_dangerous']:
            output.append(f"  ⚠️ 危険フラグ: {', '.join(result['danger_flags']['reasons'])}")
        
        return "\n".join(output)


if __name__ == "__main__":
    print("✅ EnhancedRaceScorer v5（後半4F評価・長期休養ペナルティ追加版）loaded")
    print("主な改善点:")
    print("  1. コース特性を考慮したペース予測（直線の長さで判定基準変更）")
    print("  2. コース×距離別の脚質ボーナスウェイト")
    print("  3. 重賞レースでの着順評価緩和（G3以上の2-5着）")
    print("  4. 直線の長さを考慮した展開予測")
    print("  5. 【新】後半4F評価の追加（芝中長距離1800m以上）")
    print("  6. 【新】長期休養明けペナルティ（6ヶ月以上でペナルティ）")
    print()
    
    scorer = EnhancedRaceScorer(debug_mode=True)
    
    # 東京1600mのペース予測テスト
    print("【東京1600m ペース予測テスト】")
    test_horses = [
        {'style': '逃げ', 'confidence': 0.75},
        {'style': '先行', 'confidence': 0.70},
        {'style': '先行', 'confidence': 0.65},
        {'style': '差し', 'confidence': 0.80},
        {'style': '差し', 'confidence': 0.75},
        {'style': '差し', 'confidence': 0.70},
        {'style': '追込', 'confidence': 0.75},
    ]
    
    pace_tokyo = scorer.style_analyzer.predict_race_pace(test_horses, field_size=16, course='東京')
    print(f"  16頭立て、逃げ1頭・先行2頭: {pace_tokyo['pace']}ペース")
    print(f"  前残り率: {pace_tokyo['front_ratio']}")
    print(f"  直線長: {pace_tokyo['straight_length']}m")
    print()
    
    # 中山1600mとの比較
    print("【中山1600m ペース予測テスト（同じメンバー）】")
    pace_nakayama = scorer.style_analyzer.predict_race_pace(test_horses, field_size=16, course='中山')
    print(f"  16頭立て、逃げ1頭・先行2頭: {pace_nakayama['pace']}ペース")
    print(f"  前残り率: {pace_nakayama['front_ratio']}")
    print(f"  直線長: {pace_nakayama['straight_length']}m")
    print()
    
    # 脚質ウェイトの比較
    print("【脚質ボーナスウェイトの比較】")
    print("  東京1600m:")
    for style in ['逃げ', '先行', '差し', '追込']:
        weight = scorer.style_analyzer.get_style_weight('東京', 1600, style)
        print(f"    {style}: {weight:.0%}")
    
    print("\n  中山1600m:")
    for style in ['逃げ', '先行', '差し', '追込']:
        weight = scorer.style_analyzer.get_style_weight('中山', 1600, style)
        print(f"    {style}: {weight:.0%}")
    print()
    
    # 重賞着順評価のテスト
    print("【重賞着順評価テスト（短距離）】")
    test_history_g3 = [
        {
            'race_name': '京都金杯(G3)',
            'course': '京都',
            'dist': 1600,
            'chakujun': 4,
            'last_3f': 33.5,
            'weight': 58.0,
            'all_horses_results': [
                {'last_3f': 33.2, 'goal_time_diff': -0.4},
                {'last_3f': 33.3, 'goal_time_diff': -0.2},
                {'last_3f': 33.4, 'goal_time_diff': -0.1},
                {'last_3f': 33.5, 'goal_time_diff': 0.0},
                {'last_3f': 33.6, 'goal_time_diff': 0.1},
            ]
        }
    ]
    
    result_g3 = scorer.calculate_total_score(
        current_weight=58.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_g3,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.80},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(scorer.format_score_breakdown(result_g3, 1600))
    print(f"\n  ※ G3・4着でもfinish_bonus=1.5が適用され評価される")
    print()
    
    # 【新】後半4F評価のテスト（芝中長距離）
    print("【後半4F評価テスト（芝2000m）】")
    test_history_long = [
        {
            'race_name': '日経新春杯(G2)',
            'course': '京都',
            'dist': 2400,
            'chakujun': 2,
            'last_3f': 34.5,
            'weight': 57.0,
            'track_type': '芝'
        },
        {
            'race_name': 'OP',
            'course': '阪神',
            'dist': 2000,
            'chakujun': 1,
            'last_3f': 35.0,
            'weight': 56.0,
            'track_type': '芝'
        }
    ]
    
    result_long = scorer.calculate_total_score(
        current_weight=57.0,
        target_course='東京',
        target_distance=2000,
        history_data=test_history_long,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.75},
        race_pace_prediction={'pace': 'スロー'}
    )
    
    print(scorer.format_score_breakdown(result_long, 2000))
    print(f"\n  ※ 後半4F評価が追加され、中長距離での評価精度が向上")
    print()
    
    # 【新】長期休養ペナルティのテスト
    print("【長期休養ペナルティテスト】")
    
    # ケース1: 1年9ヶ月ぶりの出走（アイルシャインのケース）
    test_history_layoff_long = [
        {
            'race_name': 'OP',
            'course': '東京',
            'dist': 1600,
            'chakujun': 1,
            'last_3f': 33.2,
            'weight': 56.0,
            'race_date': '2024/05/12',  # 約1年9ヶ月前
            'track_type': '芝'
        }
    ]
    
    result_layoff_long = scorer.calculate_total_score(
        current_weight=56.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_layoff_long,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.70},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(f"  ケース1: 1年9ヶ月ぶりの出走（2024/05/12 → 2026/02/11）")
    print(scorer.format_score_breakdown(result_layoff_long, 1600))
    
    # ケース2: 6ヶ月ぶりの出走
    test_history_layoff_6m = [
        {
            'race_name': 'OP',
            'course': '東京',
            'dist': 1600,
            'chakujun': 1,
            'last_3f': 33.2,
            'weight': 56.0,
            'race_date': '2025/08/11',  # 6ヶ月前
            'track_type': '芝'
        }
    ]
    
    result_layoff_6m = scorer.calculate_total_score(
        current_weight=56.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_layoff_6m,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.70},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(f"\n  ケース2: 6ヶ月ぶりの出走（2025/08/11 → 2026/02/11）")
    print(scorer.format_score_breakdown(result_layoff_6m, 1600))
    
    # ケース3: 4ヶ月ぶりの出走
    test_history_layoff_4m = [
        {
            'race_name': 'OP',
            'course': '東京',
            'dist': 1600,
            'chakujun': 1,
            'last_3f': 33.2,
            'weight': 56.0,
            'race_date': '2025/10/11',  # 4ヶ月前
            'track_type': '芝'
        }
    ]
    
    result_layoff_4m = scorer.calculate_total_score(
        current_weight=56.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_layoff_4m,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.70},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(f"\n  ケース3: 4ヶ月ぶりの出走（2025/10/11 → 2026/02/11）")
    print(scorer.format_score_breakdown(result_layoff_4m, 1600))
    
    # ケース4: 5ヶ月ぶりの出走
    test_history_layoff_5m = [
        {
            'race_name': 'OP',
            'course': '東京',
            'dist': 1600,
            'chakujun': 1,
            'last_3f': 33.2,
            'weight': 56.0,
            'race_date': '2025/09/11',  # 5ヶ月前
            'track_type': '芝'
        }
    ]
    
    result_layoff_5m = scorer.calculate_total_score(
        current_weight=56.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_layoff_5m,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.70},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(f"\n  ケース4: 5ヶ月ぶりの出走（2025/09/11 → 2026/02/11）")
    print(scorer.format_score_breakdown(result_layoff_5m, 1600))
    
    # ケース5: 7ヶ月ぶりの出走
    test_history_layoff_7m = [
        {
            'race_name': 'OP',
            'course': '東京',
            'dist': 1600,
            'chakujun': 1,
            'last_3f': 33.2,
            'weight': 56.0,
            'race_date': '2025/07/11',  # 7ヶ月前
            'track_type': '芝'
        }
    ]
    
    result_layoff_7m = scorer.calculate_total_score(
        current_weight=56.0,
        target_course='東京',
        target_distance=1600,
        history_data=test_history_layoff_7m,
        target_track_type='芝',
        running_style_info={'style': '差し', 'confidence': 0.70},
        race_pace_prediction={'pace': 'ミドル'}
    )
    
    print(f"\n  ケース5: 7ヶ月ぶりの出走（2025/07/11 → 2026/02/11）")
    print(scorer.format_score_breakdown(result_layoff_7m, 1600))
    
    print(f"\n  ※ ペナルティ基準:")
    print(f"     3ヶ月まで0点、4ヶ月-4点、5ヶ月-6点、6ヶ月-8点、7ヶ月-10点")
    print(f"     8ヶ月-11点、9ヶ月-12点、10ヶ月-14点、11ヶ月-16点、1年以上-20点")
