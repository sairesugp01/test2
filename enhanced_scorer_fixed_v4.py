"""
競馬予想AI - enhanced_scorer_v6.py（新馬戦2戦目ブースト追加版）
最終更新: 2026年2月16日

V5からの改善点:
10. 【新】新馬戦2戦目ブーストの追加
   - 新馬戦を走った馬（total_races == 1 and first_race_type == "新馬"）を識別
   - 新馬 → 未勝利初戦は期待値加点:
     * 1着: +5点
     * 2着: +2.5点
     * 3着: +1点
     * 4着以下: 0点
   - 特別加点条件:
     * 新馬戦で上がり上位（レース平均より0.5秒以上速い）: +5点
     * 道中後方（4角位置が後ろ50%以内） → 直線伸び（上がり上位）: +3点
   - 最大合計: +13点のブースト（1着の場合）
   - 経験不足による未知数をカバーし、適切に評価

11. 【新】連続大敗ペナルティの追加
   - 着差1.1秒以上の大敗を連続でカウント
   - ペナルティ基準:
     * 1回: -3点（軽微）
     * 2連続: -8点（危険）
     * 3連続以上: -15点（原則消し）
   - ペナルティ軽減条件:
     * 距離の大幅変更（600m以上）
     * 中止 → 大敗（実質1走扱い）
     * 休養明け初戦（4ヶ月以上）の大敗

既存の主な機能:
1. コース特性を考慮したペース予測
2. コース×距離別の脚質ボーナスウェイト
3. 重賞レースでの着順評価緩和
4. 上がり3F評価の改善
5. 後半4F評価の追加（芝中長距離）
6. 長期休養明けペナルティ
7. 短距離専用の斤量×タイム評価
8. 斤量増ペナルティの全距離適用
9. 重賞出走ボーナス
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
    
    def detect_race_grade(self, race_name: str) -> Tuple[str, float]:
        """レース格を判定"""
        grade_map = {
            'G1': 1.0, 'G2': 0.95, 'G3': 0.9,
            'JpnI': 1.0, 'JpnII': 0.95, 'JpnIII': 0.9
        }
        
        for grade, reliability in grade_map.items():
            if grade in race_name or grade.replace('Jpn', '') in race_name:
                return grade, reliability
        
        if 'OP' in race_name or 'オープン' in race_name or '特別' in race_name:
            return 'OP', 0.85
        elif '1勝クラス' in race_name or '500万下' in race_name:
            return '1勝', 0.75
        elif '未勝利' in race_name:
            return '未勝利', 0.70
        elif '新馬' in race_name:
            return '新馬', 0.65
        
        return '不明', 0.60
    
    def _is_local_race(self, race_name: str, course: str) -> bool:
        """地方競馬かどうか判定"""
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
        """距離適性スコア"""
        if not history_data:
            return 0.0
        
        score = 0.0
        for race in history_data[:3]:
            dist = race.get('dist', 0)
            if dist <= 0:
                continue
            
            diff = abs(target_distance - dist)
            
            if diff <= 200:
                points = 15.0
            elif diff <= 400:
                points = 10.0
            elif diff <= 600:
                points = 5.0
            else:
                points = 0.0
            
            score += points
        
        return round(score / min(len(history_data[:3]), 3), 1)
    
    def _calculate_course_score(self, history_data: List[Dict], target_course: str) -> float:
        """コース適性スコア"""
        if not history_data:
            return 0.0
        
        score = 0.0
        for race in history_data[:3]:
            course = race.get('course', '')
            if course == target_course:
                score += 15.0
            elif course:
                score += 5.0
        
        return round(score / min(len(history_data[:3]), 3), 1)
    
    def _calculate_weight_penalty(self, history_data: List[Dict], current_weight: float, 
                                  target_distance: int) -> float:
        """斤量増ペナルティ（全距離適用）"""
        if not history_data or current_weight <= 0:
            return 0.0
        
        recent_weights = [r.get('weight', 0) for r in history_data[:3] if r.get('weight', 0) > 0]
        if not recent_weights:
            return 0.0
        
        # 前走の斤量
        prev_weight = history_data[0].get('weight', 0) if history_data else 0
        
        # 過去3走の平均斤量
        avg_weight = sum(recent_weights) / len(recent_weights)
        
        # 前走または平均との差（大きい方）
        weight_diff = max(current_weight - prev_weight, current_weight - avg_weight)
        
        if weight_diff <= 0:
            return 0.0
        
        # 距離別のペナルティ係数
        if target_distance <= 1200:
            penalty_per_kg = -1.5
        elif target_distance <= 1600:
            penalty_per_kg = -2.0
        elif target_distance <= 1800:
            penalty_per_kg = -2.5
        else:
            penalty_per_kg = -3.0
        
        penalty = weight_diff * penalty_per_kg
        
        if self.debug_mode:
            logger.debug(f"  斤量増ペナルティ: {weight_diff:+.1f}kg × {penalty_per_kg:.1f} = {penalty:.1f}点")
        
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
            current_date = datetime(2026, 2, 16)
            days_since = (current_date - race_date).days
            months_since = days_since / 30.44
            
            if months_since <= 3:
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
        
        # 2. 中止 → 大敗
        if loss_index + 1 < len(history_data):
            prev_race = history_data[loss_index + 1]
            prev_chakujun = prev_race.get('chakujun', 0)
            
            # 着順0または99は中止・取消の可能性
            if prev_chakujun == 0 or prev_chakujun >= 90:
                if self.debug_mode:
                    logger.debug(f"    大敗ペナルティ軽減: 前走中止/取消")
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
    
    def _calculate_shinba_second_race_boost(self, history_data: List[Dict]) -> float:
        """
        【新】新馬戦2戦目ブーストの計算
        
        条件:
        - 総走数が1回（新馬戦のみ）
        - 最初のレースが「新馬」
        
        ブースト内容:
        - ベースボーナス（着順別）:
          * 1着: +5点
          * 2着: +2.5点
          * 3着: +1点
          * 4着以下: 0点
        - 上がり上位（レース平均より0.5秒以上速い）: +5点
        - 道中後方（4角位置が後ろ50%以内） → 直線伸び: +3点
        - 最大合計: +13点（1着の場合）
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
        
        # ベースボーナス（着順別）
        chakujun = first_race.get('chakujun', 99)
        if chakujun == 1:
            boost = 5.0
            if self.debug_mode:
                logger.debug("    新馬1着ベースボーナス: +5点")
        elif chakujun == 2:
            boost = 2.5
            if self.debug_mode:
                logger.debug("    新馬2着ベースボーナス: +2.5点")
        elif chakujun == 3:
            boost = 1.0
            if self.debug_mode:
                logger.debug("    新馬3着ベースボーナス: +1点")
        else:
            boost = 0.0
            if self.debug_mode:
                logger.debug("    新馬4着以下: ベースボーナスなし")
        
        # 上がり3F評価
        my_last_3f = first_race.get('last_3f', 0.0)
        all_horses_results = first_race.get('all_horses_results', [])
        
        if my_last_3f > 0 and all_horses_results:
            # レース平均上がり3Fを計算
            valid_3f = [h.get('last_3f', 0) for h in all_horses_results if h.get('last_3f', 0) > 0]
            if valid_3f:
                race_avg_3f = sum(valid_3f) / len(valid_3f)
                speed_diff = race_avg_3f - my_last_3f
                
                # 0.5秒以上速い場合は上がり上位と判定
                if speed_diff >= 0.5:
                    boost += 5.0
                    if self.debug_mode:
                        logger.debug(f"    新馬で上がり上位（平均{race_avg_3f:.2f}s vs 自身{my_last_3f:.2f}s、差{speed_diff:+.2f}s）: +5点")
        
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
                            boost += 3.0
                            if self.debug_mode:
                                logger.debug(f"    道中後方（{position_4c}/{field_size}番手）→直線伸び: +3点")
        
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
        
        # 4. 斤量評価
        if target_distance <= 1600:
            weight_time_score = self._calculate_weight_time_score(current_weight, history_data, target_distance, target_track_type)
        else:
            weight_time_score = 0.0
        
        weight_penalty = self._calculate_weight_penalty(history_data, current_weight, target_distance)
        
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
        
        # 10. 脚質ボーナス
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
        
        # 11. 危険フラグ
        danger_flags = self._check_danger_flags(history_data, target_course, target_track_type)
        danger_penalty = -15.0 if danger_flags['local_to_jra'] else 0.0
        
        # スコア正規化
        is_long_distance = target_distance >= 1800 and target_track_type == "芝"
        
        normalized_3f = min(last_3f_score / 150.0 * 100, 100)
        normalized_distance = min(distance_score / 15.0 * 100, 100)
        normalized_course = min(course_score / 15.0 * 100, 100)
        normalized_style = min(style_bonus / 20.0 * 100, 100)
        normalized_late_4f = min(late_4f_score / 50.0 * 100, 100) if late_4f_score != 0 else 0
        normalized_weight_time = min(weight_time_score / 30.0 * 100, 100) if weight_time_score != 0 else 0
        
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
                shinba_boost +  # 【新】新馬戦2戦目ブースト
                consecutive_loss_penalty +  # 【新】連続大敗ペナルティ
                danger_penalty
            )
        else:
            weight_3f = 0.30
            weight_wt = 0.30
            weight_distance = 0.15
            weight_course = 0.10
            weight_style = 0.15
            
            total = (
                normalized_3f * weight_3f +
                normalized_weight_time * weight_wt +
                normalized_distance * weight_distance +
                normalized_course * weight_course +
                normalized_style * weight_style +
                weight_penalty +
                layoff_penalty +
                grade_race_bonus +
                shinba_boost +  # 【新】新馬戦2戦目ブースト
                consecutive_loss_penalty +  # 【新】連続大敗ペナルティ
                danger_penalty
            )
        
        return {
            'total_score': round(total, 1),
            'last_3f_score': last_3f_score,
            'distance_score': distance_score,
            'course_score': course_score,
            'style_bonus': round(style_bonus, 1),
            'weight_time_score': weight_time_score,
            'late_4f_score': late_4f_score,
            'weight_penalty': weight_penalty,
            'layoff_penalty': layoff_penalty,
            'grade_race_bonus': grade_race_bonus,
            'shinba_boost': shinba_boost,  # 【新】
            'consecutive_loss_penalty': consecutive_loss_penalty,  # 【新】
            'danger_penalty': danger_penalty,
            'danger_flags': danger_flags
        }
    
    def _calculate_late_4f_score(self, history_data: List[Dict], target_distance: int, 
                                  target_track_type: str) -> float:
        """後半4F評価（芝中長距離専用）"""
        if target_track_type != "芝" or target_distance < 1800:
            return 0.0
        
        BASELINE_4F = 47.2 if target_distance <= 2000 else 47.8 if target_distance <= 2400 else 48.3
        score = 0.0
        
        for idx, race in enumerate(history_data[:3]):
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
            
            last_3f = race.get('last_3f', 0.0)
            if last_3f <= 0:
                continue
            
            estimated_late_4f = last_3f * (4.0 / 3.0) + 0.4
            diff_from_baseline = BASELINE_4F - estimated_late_4f
            
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
            
            if distance > 1600 or distance < 1000:
                continue
            if weight <= 0 or last_3f <= 0:
                continue
            
            race_track_type = race.get('track_type', None)
            if not race_track_type or race_track_type == '不明':
                race_track_type = self._get_track_type_by_distance(distance, race_name, course)
            
            is_local = self._is_local_race(race_name, course)
            time_decay = 1.0 - (idx * 0.25)
            
            if distance <= 1200:
                BASE_3F, WEIGHT_THRESHOLD = 34.0, 56.0
            elif distance <= 1400:
                BASE_3F, WEIGHT_THRESHOLD = 34.3, 55.5
            else:
                BASE_3F, WEIGHT_THRESHOLD = 34.5, 55.0
            
            if is_local:
                BASE_3F += 1.2 if race_track_type == 'ダート' else 0.5
            
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
            
            adjusted_base = BASE_3F + (0.3 if weight >= 57.0 else 0.1 if weight >= 55.0 else 0)
            diff = adjusted_base - last_3f
            
            if diff >= 1.0:
                speed_bonus = 12.0
            elif diff >= 0.5:
                speed_bonus = 8.0
            elif diff >= 0.0:
                speed_bonus = 4.0
            elif diff >= -0.5:
                speed_bonus = 0.0
            else:
                speed_bonus = -4.0
            
            if is_local:
                speed_bonus *= 0.6
            
            chakujun = race.get('chakujun', 99)
            if chakujun == 1:
                finish_multiplier = 1.5
            elif chakujun <= 3:
                finish_multiplier = 1.2
            elif chakujun <= 5:
                finish_multiplier = 1.0
            else:
                finish_multiplier = 0.7
            
            points = (weight_bonus + speed_bonus) * finish_multiplier * time_decay
            score += points
        
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
        recent_local_count = sum(1 for race in history_data[:3] 
                                if self._is_local_race(race.get('race_name', ''), race.get('course', '')))
        
        if recent_local_count >= 2:
            flags['local_to_jra'] = True
        
        return flags
    
    def format_score_breakdown(self, result: Dict, target_distance: int) -> str:
        """スコア内訳をフォーマット"""
        lines = []
        lines.append(f"【総合スコア: {result['total_score']:.1f}点】")
        lines.append(f"  上がり3F評価: {result['last_3f_score']:.1f}点")
        
        if target_distance <= 1600 and result['weight_time_score'] != 0:
            lines.append(f"  斤量×タイム評価: {result['weight_time_score']:.1f}点")
        
        if target_distance >= 1800 and result['late_4f_score'] != 0:
            lines.append(f"  後半4F評価: {result['late_4f_score']:.1f}点")
        
        lines.append(f"  距離適性: {result['distance_score']:.1f}点")
        lines.append(f"  コース適性: {result['course_score']:.1f}点")
        lines.append(f"  脚質ボーナス: {result['style_bonus']:.1f}点")
        
        if result['weight_penalty'] != 0:
            lines.append(f"  斤量増ペナルティ: {result['weight_penalty']:.1f}点")
        
        if result['layoff_penalty'] != 0:
            lines.append(f"  長期休養ペナルティ: {result['layoff_penalty']:.1f}点")
        
        if result['grade_race_bonus'] != 0:
            lines.append(f"  重賞出走ボーナス: +{result['grade_race_bonus']:.1f}点")
        
        if result.get('shinba_boost', 0) != 0:
            lines.append(f"  ★新馬戦2戦目ブースト: +{result['shinba_boost']:.1f}点")
        
        if result.get('consecutive_loss_penalty', 0) != 0:
            lines.append(f"  連続大敗ペナルティ: {result['consecutive_loss_penalty']:.1f}点")
        
        if result['danger_penalty'] != 0:
            lines.append(f"  危険フラグペナルティ: {result['danger_penalty']:.1f}点")
        
        return "\n".join(lines)


if __name__ == "__main__":
    # テストケース
    print("=" * 80)
    print("enhanced_scorer_v6.py - 新馬戦2戦目ブースト機能テスト")
    print("=" * 80)
    print()
    
    scorer = RaceScorer(debug_mode=True)
    
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
