# app_v3_fixed.py の先頭(シンプル版)
import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
import unicodedata
import re
from datetime import datetime
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, KeepTogether
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.lib.enums import TA_CENTER, TA_LEFT

# インポートのデバッグ情報
import sys
import os

# sys.pathに現在のディレクトリを追加(デバッグ表示なし)
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
except Exception as e:
    pass


# scraperのインポート
try:
    from scraper_v3_fixed import NetkeibaRaceScraper
except ImportError as e:
    st.error(f"❌ **Import Error**: {e}")
    st.error("""
    **解決方法**:
    1. `scraper_v3_fixed.py` と `enhanced_scorer_v5.py` が同じディレクトリにあることを確認
    2. `scraper_v3_fixed.py`の28行目を以下のように修正:
       ```python
       from enhanced_scorer_v5 import EnhancedRaceScorer
       ```
    3. Streamlit Cloudの場合、GitHubリポジトリのルートに全ファイルを配置
    4. ファイル名が正確に一致しているか確認(大文字小文字も含む)
    """)
    st.stop()

st.set_page_config(page_title="競馬予想AI v7.1", page_icon="🏇", layout="wide")

# --- 日本語フォント設定 ---
@st.cache_resource
def setup_japanese_font():
    """日本語フォントの設定"""
    try:
        # CIDフォント(ReportLab組み込み)を使用
        from reportlab.pdfbase.cidfonts import UnicodeCIDFont
        pdfmetrics.registerFont(UnicodeCIDFont('HeiseiMin-W3'))
        return 'HeiseiMin-W3'
    except Exception as e:
        st.error(f"⚠️ 日本語フォント読み込みエラー: {e}")
        return 'Helvetica'

JAPANESE_FONT = setup_japanese_font()

# --- 共通ユーティリティ ---
def normalize_uma(s):
    """馬番の正規化"""
    if s is None: 
        return ""
    return unicodedata.normalize('NFKC', str(s)).strip().lstrip('0')

def fetch_results_simple(race_id):
    """レース結果を取得"""
    results = {}
    try:
        scraper = NetkeibaRaceScraper()
        res = scraper.session.get(f"https://race.netkeiba.com/race/result.html?race_id={race_id}", timeout=10)
        res.encoding = 'EUC-JP'
        soup = BeautifulSoup(res.content, "html.parser")
        table = soup.find("table", id="All_Result_Table")
        if table:
            for row in table.find_all("tr")[1:]:
                tds = row.find_all("td")
                if len(tds) >= 3:
                    rank = tds[0].get_text(strip=True)
                    u_no = normalize_uma(tds[2].get_text(strip=True))
                    if u_no: 
                        results[u_no] = rank
    except Exception as e:
        st.warning(f"結果取得エラー: {e}")
    return results

def prepare_display_df(raw_df, results):
    """表示用データフレームを準備(着順ソート対応)"""
    # 空チェック
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()
    
    # 防御的修正:列名を'指数'に統一
    if '総合指数' in raw_df.columns:
        raw_df = raw_df.rename(columns={'総合指数': '指数'})
    
    if '指数' not in raw_df.columns:
        raw_df['指数'] = 0.0
    
    data = []
    for _, r in raw_df.iterrows():
        u_no_str = normalize_uma(r.get('馬番',''))
        rank_str = results.get(u_no_str, "-")
        
        # 着順・馬番を数値に変換(ソートを正常化)
        try:
            u_no_val = int(u_no_str) if u_no_str.isdigit() else 99
        except: 
            u_no_val = 99
            
        try:
            # 「1」は1に、「中止」は999にする
            rank_val = int(re.sub(r'\D', '', rank_str)) if re.search(r'\d', rank_str) else 999
        except: 
            rank_val = 999

        data.append({
            "印": r.get('印',''),
            "馬番": u_no_val,
            "馬名": r.get('馬名',''),
            "指数": float(r.get('指数', 0)),
            "着順": rank_val if rank_str != "-" else "-",
            "_sort_rank": rank_val  # ソート用の内部フィールド
        })
    
    df = pd.DataFrame(data)
    
    # 結果照合時(着順データがある場合)は着順でソート
    if not df.empty and results:
        df = df.sort_values('_sort_rank').reset_index(drop=True)
        df = df.drop(columns=['_sort_rank'])  # ソート用フィールドを削除
    else:
        # 分析時は指数の高い順でソート(印と一致させる)
        df = df.sort_values('指数', ascending=False).reset_index(drop=True)
        df = df.drop(columns=['_sort_rank'], errors='ignore')
    
    # 表示用の着順列を「数値」として扱うことで 1.2.10 の順になる
    if not df.empty:
        df["着順"] = pd.to_numeric(df["着順"], errors='coerce')
    
    return df

def create_pdf_report(batch_data, venue, date, mode="analysis"):
    """PDF予想レポートを生成"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=15*mm,
        leftMargin=15*mm,
        topMargin=20*mm,
        bottomMargin=20*mm
    )
    
    # スタイル設定
    styles = getSampleStyleSheet()
    
    # 日本語対応スタイル
    title_style = ParagraphStyle(
        'JapaneseTitle',
        parent=styles['Title'],
        fontName=JAPANESE_FONT,
        fontSize=18,
        alignment=TA_CENTER,
        spaceAfter=20
    )
    
    heading_style = ParagraphStyle(
        'JapaneseHeading',
        parent=styles['Heading2'],
        fontName=JAPANESE_FONT,
        fontSize=14,
        spaceAfter=10,
        spaceBefore=15
    )
    
    normal_style = ParagraphStyle(
        'JapaneseNormal',
        parent=styles['Normal'],
        fontName=JAPANESE_FONT,
        fontSize=9,
        alignment=TA_LEFT
    )
    
    # コンテンツ作成
    story = []
    
    # タイトル
    report_type = "予想レポート" if mode == "analysis" else "結果照合レポート"
    title = Paragraph(f"{venue} {report_type}", title_style)
    story.append(title)
    
    # 開催情報
    date_formatted = f"{date[:4]}年{date[4:6]}月{date[6:8]}日"
    info = Paragraph(f"開催日: {date_formatted}", normal_style)
    story.append(info)
    story.append(Spacer(1, 10*mm))
    
    # 各レースのテーブル
    for race in batch_data:
        race_elements = []  # 1レース分の要素をまとめる
        
        # レース名
        race_title = Paragraph(f"{race['no']}R  {race['name']}", heading_style)
        race_elements.append(race_title)
        
        # レース条件(距離・コース種別)
        if 'info' in race:
            condition_text = f"条件: {race['info'].get('track_type', '')} {race['info'].get('distance', '')}m"
            condition = Paragraph(condition_text, normal_style)
            race_elements.append(condition)
            race_elements.append(Spacer(1, 3*mm))
        
        # データフレームをテーブルに変換
        df = race['df']
        
        if not df.empty:
            # ヘッダー
            table_data = [['印', '馬番', '馬名', '指数', '着順']]
            
            # データ行
            for _, row in df.iterrows():
                table_data.append([
                    str(row.get('印', '')),
                    str(row.get('馬番', '')),
                    str(row.get('馬名', ''))[:15],  # 長い馬名は切り詰め
                    f"{row.get('指数', 0):.1f}",
                    str(row.get('着順', '-')) if row.get('着順', '-') != '-' else '-'
                ])
            
            # テーブル作成
            table = Table(table_data, colWidths=[15*mm, 20*mm, 60*mm, 25*mm, 20*mm])
            
            # テーブルスタイル
            table.setStyle(TableStyle([
                # ヘッダー
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), JAPANESE_FONT),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                # データ行のフォント設定（文字化け防止）
                ('FONTNAME', (0, 1), (-1, -1), JAPANESE_FONT),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                # ボーダー
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('BOX', (0, 0), (-1, -1), 1, colors.black),
                # データ行
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F0F0F0')]),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            
            race_elements.append(table)
        else:
            race_elements.append(Paragraph("データなし", normal_style))
        
        race_elements.append(Spacer(1, 8*mm))
        
        # 1レース分をKeepTogetherでまとめる
        story.append(KeepTogether(race_elements))
        
        # 3レースごとに改ページ(ページに収まる量を調整)
        if race['no'] % 3 == 0 and race['no'] < 12:
            story.append(PageBreak())
    
    # PDF生成
    doc.build(story)
    buffer.seek(0)
    return buffer

# --- セッション初期化 ---
if 'batch_data' not in st.session_state: 
    st.session_state['batch_data'] = []
if 'race_info' not in st.session_state: 
    st.session_state['race_info'] = None
if 'res_map' not in st.session_state: 
    st.session_state['res_map'] = {}
if 'current_mode' not in st.session_state:
    st.session_state['current_mode'] = 'analysis'

# --- 定数定義 ---
VENUES = {"東京": "05", "京都": "08", "小倉": "10"}

SCHEDULE = {
    "20260214": {"東京": (1, 5), "京都": (2, 5), "小倉": (1, 7)},
    "20260215": {"東京": (1, 6), "京都": (2, 6), "小倉": (1, 8)}
}

# --- サイドバー ---
with st.sidebar:
    st.header("⚙️ 設定")
    mode = st.selectbox("📌 モード", ["個別レース", "一括レース"])
    date_sel = st.selectbox("開催日", list(SCHEDULE.keys()))
    venue_sel = st.selectbox("競馬場", list(SCHEDULE[date_sel].keys()))
    
    if mode == "個別レース":
        race_no = st.selectbox("レース番号", range(1, 13), index=10)
        analyze_clicked = st.button("🚀 指数分析", type="primary", use_container_width=True)
        result_clicked = st.button("🏆 結果照合", use_container_width=True)
    else:
        batch_analyze_clicked = st.button("🚀 全レース一括解析", type="primary", use_container_width=True)
        batch_result_clicked = st.button("🏆 全レース結果照合", use_container_width=True)

# --- メインコンテンツ ---
st.title("🏇 競馬予想AI v7.1")

# --- 1. 個別解析ロジック ---
if mode == "個別レース":
    rid = f"{date_sel[:4]}{VENUES[venue_sel]}{SCHEDULE[date_sel][venue_sel][0]:02d}{SCHEDULE[date_sel][venue_sel][1]:02d}{race_no:02d}"
    
    if analyze_clicked:
        progress_placeholder = st.empty()
        status_placeholder = st.empty()
        
        with st.spinner(f"{race_no}R 分析中..."):
            # 進捗表示用のコールバック関数
            def progress_callback(horse_name, current, total):
                percent = int((current / total) * 100)
                progress_placeholder.progress(percent / 100)
                status_placeholder.text(f"🔍 {horse_name} を分析中... ({current}/{total})")
            
            scraper = NetkeibaRaceScraper()
            scraper.progress_callback = progress_callback
            st.session_state['race_info'] = scraper.get_race_data(rid)
            st.session_state['res_map'] = {}  # 照合はリセット
            st.session_state['current_mode'] = 'analysis'
        
        progress_placeholder.empty()
        status_placeholder.empty()

    if result_clicked:
        with st.spinner("結果取得中..."):
            st.session_state['res_map'] = fetch_results_simple(rid)
            st.session_state['current_mode'] = 'result'

    if st.session_state['race_info']:
        info = st.session_state['race_info']
        
        # 取りやめレース・新馬戦の表示
        if info.get('is_cancelled'):
            st.warning(f"⚠️ このレースは取りやめになりました")
            st.info(f"理由: {info.get('skip_reason', 'レース取りやめ')}")
            st.stop()
        elif info.get('is_new_horse_race'):
            st.info(f"ℹ️ このレースは新馬戦のためスキップされました")
            st.info(f"理由: {info.get('skip_reason', '新馬戦')}")
            st.stop()
        
        # レース名と条件を表示
        race_title = f"📋 {race_no}R {info.get('race_name', '')}"
        race_condition = f"({info.get('track_type', '')} {info.get('distance', '')}m)"
        
        st.subheader(f"{race_title} {race_condition}")
        
        df = prepare_display_df(info['df'], st.session_state['res_map'])
        st.dataframe(df, hide_index=True, use_container_width=True)
        
        # PDF出力ボタン
        if not df.empty:
            st.markdown("---")
            pdf_data = [{
                'no': race_no, 
                'name': info.get('race_name', ''),
                'info': info,
                'df': df
            }]
            
            pdf_buffer = create_pdf_report(
                pdf_data, 
                venue_sel, 
                date_sel,
                mode=st.session_state['current_mode']
            )
            
            report_type = "予想レポート" if st.session_state['current_mode'] == 'analysis' else "結果照合レポート"
            filename = f"{venue_sel}_{race_no}R_{report_type}_{datetime.now().strftime('%Y%m%d')}.pdf"
            
            st.download_button(
                label=f"📥 {report_type}をダウンロード (PDF)",
                data=pdf_buffer,
                file_name=filename,
                mime="application/pdf",
                use_container_width=True
            )

# --- 2. 一括解析ロジック ---
elif mode == "一括レース":
    if batch_analyze_clicked or batch_result_clicked:
        st.session_state['batch_data'] = []
        st.session_state['current_mode'] = 'result' if batch_result_clicked else 'analysis'
        
        # 進捗表示用のプレースホルダー
        race_progress_bar = st.progress(0)
        race_status = st.empty()
        horse_progress_bar = st.progress(0)
        horse_status = st.empty()
        
        scraper = NetkeibaRaceScraper()
        
        # 馬の進捗表示用コールバック
        def progress_callback(horse_name, current, total):
            percent = int((current / total) * 100)
            horse_progress_bar.progress(percent / 100)
            horse_status.text(f"   🐴 {horse_name} を分析中... ({current}/{total}頭)")
        
        scraper.progress_callback = progress_callback
        
        for i in range(1, 13):
            rid = f"{date_sel[:4]}{VENUES[venue_sel]}{SCHEDULE[date_sel][venue_sel][0]:02d}{SCHEDULE[date_sel][venue_sel][1]:02d}{i:02d}"
            
            # レース進捗を表示
            race_percent = int(((i - 1) / 12) * 100)
            race_progress_bar.progress(race_percent / 100)
            race_status.markdown(f"### 📊 {i}R を解析中... ({i}/12レース)")
            
            try:
                res = scraper.get_race_data(rid)
                
                # 取りやめレース・新馬戦をスキップ
                if res and res.get('is_cancelled'):
                    st.warning(f"⚠️ {i}R: {res.get('skip_reason', 'レース取りやめ')} - スキップします")
                    continue
                elif res and res.get('is_new_horse_race'):
                    st.info(f"ℹ️ {i}R: {res.get('skip_reason', '新馬戦')} - スキップします")
                    continue
                
                if res and not res['df'].empty:
                    rmap = fetch_results_simple(rid) if batch_result_clicked else {}
                    df_res = prepare_display_df(res['df'], rmap)
                    st.session_state['batch_data'].append({
                        'no': i, 
                        'name': res.get('race_name', ''),
                        'info': res,
                        'df': df_res
                    })
            except Exception as e:
                st.error(f"❌ {i}R: エラーが発生しました - {str(e)[:100]} - スキップします")
                continue
            
            # レース完了時にプログレスバーを100%に
            race_progress_bar.progress(i / 12)
            horse_progress_bar.progress(0)
            horse_status.empty()
        
        # 完了後にプレースホルダーをクリア
        race_progress_bar.empty()
        race_status.empty()
        horse_progress_bar.empty()
        horse_status.empty()
        
        st.success("✅ 全レースの解析が完了しました!")

    if st.session_state['batch_data']:
        # レース一覧表示
        for race in st.session_state['batch_data']:
            race_condition = f"({race['info'].get('track_type', '')} {race['info'].get('distance', '')}m)"
            st.markdown(f"#### {race['no']}R {race['name']} {race_condition}")
            st.dataframe(race['df'], hide_index=True, use_container_width=True)
        
        # PDF出力ボタン(全レース)
        st.markdown("---")
        
        pdf_buffer = create_pdf_report(
            st.session_state['batch_data'], 
            venue_sel, 
            date_sel,
            mode=st.session_state['current_mode']
        )
        
        report_type = "全レース予想" if st.session_state['current_mode'] == 'analysis' else "全レース結果照合"
        filename = f"{venue_sel}_{report_type}_{datetime.now().strftime('%Y%m%d')}.pdf"
        
        st.download_button(
            label=f"📥 {report_type}レポートをダウンロード (PDF)",
            data=pdf_buffer,
            file_name=filename,
            mime="application/pdf",
            type="primary",
            use_container_width=True
        )
