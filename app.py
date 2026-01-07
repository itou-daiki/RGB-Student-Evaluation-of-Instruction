"""
授業アンケート分析アプリケーション

教員ごとに実施された授業アンケートの生データ（回答ログ）をCSVとしてアップロードし、
自動的に集計、可視化、および報告用データのダウンロードができるStreamlitアプリ
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
from datetime import datetime
from data_processor import (
    load_and_process_csv,
    calculate_statistics,
    get_overall_average,
    extract_free_comments,
    create_download_data,
    detect_subject_column,
    write_to_template,
    create_integrated_raw_data_excel,
)
from config import SUBJECT_CATEGORY_MAPPING


def get_current_reiwa_year():
    """現在の令和年を計算"""
    return str(datetime.now().year - 2018)

def get_survey_number():
    """現在の月から実施回数を計算"""
    month = datetime.now().month
    if 4 <= month <= 7: return "1"
    if 9 <= month <= 11: return "2"
    return "3"

def get_current_month():
    """現在の月を取得"""
    return str(datetime.now().month)

def show_welcome_message():
    """ファイルがアップロードされていない場合に表示するウェルカムメッセージ"""
    st.info("☝ 上記のエリアからCSV/Excelファイルをアップロードしてください")
    st.markdown("""
    ### 使い方
    1. **ファイルのアップロード**: 授業アンケートのCSV/Excelファイルをアップロードします
    2. **科目の選択**: 分析したい科目を選択します（複数ファイル可）
    3. **ダッシュボードの確認**: 回答者数、平均点、質問ごとの統計が表示されます
    4. **データのダウンロード**: 集計結果を様々な形式でExcelファイルとしてダウンロードできます
    """)

def render_statistics_tab(stats_df, overall_avg):
    """「集計結果」タブのコンテンツを表示する"""
    st.subheader("質問項目ごとの集計")
    st.dataframe(stats_df.style.format({'平均値': '{:.2f}'}), use_container_width=True, height=400)

    st.markdown("---")
    st.subheader("質問ごとの平均点（棒グラフ）")
    
    stats_df_plot = stats_df.copy()
    stats_df_plot['質問番号'] = [f"Q{i+1}" for i in range(len(stats_df_plot))]

    fig_bar = px.bar(
        stats_df_plot, x='質問番号', y='平均値', hover_data=['質問項目', '平均値', '有効回答数'],
        title="質問項目ごとの平均点", color='平均値', color_continuous_scale='RdYlGn', range_color=[1, 4]
    )
    fig_bar.update_layout(yaxis_range=[0, 4.5], height=500)
    fig_bar.add_hline(y=overall_avg, line_dash="dash", annotation_text=f"総合平均: {overall_avg:.2f}")
    st.plotly_chart(fig_bar, use_container_width=True)

def render_comments_tab(df, metadata):
    """「自由記述」タブのコンテンツを表示する"""
    st.subheader("💬 学生の意見・感想")
    free_text_col = metadata.get('free_text_column')
    if not free_text_col:
        st.warning("⚠️ 自由記述カラムが検出されませんでした")
        return

    exclude_empty = st.checkbox("空白や「特になし」を除外する", value=True)
    comments = extract_free_comments(df, free_text_col, exclude_empty=exclude_empty)
    
    if comments:
        st.info(f"📝 {len(comments)}件の意見・感想があります")
        for i, comment in enumerate(comments, 1):
            st.markdown(f"**{i}.** {comment}")
    else:
        st.warning("意見・感想が見つかりませんでした")

def render_download_tab(combined_df, filtered_df, stats_df, overall_avg, selected_subject, metadata):
    """「ダウンロード」タブのコンテンツを表示する"""
    st.subheader("📥 集計結果のダウンロード")
    download_format = st.radio(
        "ダウンロード形式を選択してください",
        ["テンプレートを使用", "統合形式（全データ）", "科目別形式"],
        help="テンプレート：既存テンプレートに書き込み / 統合形式：全データ・全教科を1ファイルに / 科目別：現在選択中の科目のみ"
    )

    if download_format == "テンプレートを使用":
        render_template_download_option(combined_df, metadata['question_columns'])
    elif download_format == "統合形式（全データ）":
        render_integrated_download_option(combined_df)
    else: # 科目別形式
        render_subject_download_option(filtered_df, stats_df, overall_avg, selected_subject, metadata)

def render_template_download_option(df, question_cols):
    """テンプレート形式でのダウンロードオプションを表示する"""
    st.markdown("### 📝 テンプレート情報の入力")
    col1, col2, col3 = st.columns(3)
    year = col1.text_input("📅 年度", value=get_current_reiwa_year())
    survey_number = col2.text_input("🔢 実施回数", value=get_survey_number())
    month = col3.text_input("📆 実施月", value=get_current_month())

    st.markdown("---")
    st.markdown("### 📚 教科と科目のマッピング設定")
    
    subject_col = detect_subject_column(df)
    if not subject_col or subject_col not in df.columns:
        st.warning("⚠️ 科目名カラムが検出されませんでした。")
        return

    available_subjects = sorted([str(s) for s in df[subject_col].unique() if pd.notna(s)])
    template_subjects = list(SUBJECT_CATEGORY_MAPPING.keys())
    
    subject_mapping = {}
    cols = st.columns(3)
    for idx, template_subject in enumerate(template_subjects):
        with cols[idx % 3]:
            # 他の教科で選択済みの科目は選択肢から除外する
            already_selected = {item for key, val in st.session_state.items() if key.startswith('map_') and key != f'map_{template_subject}' for item in val}
            options = [s for s in available_subjects if s not in already_selected]
            
            # デフォルト選択を計算
            default_selection = [s for s in options if any(keyword in s for keyword in SUBJECT_CATEGORY_MAPPING.get(template_subject, []))]
            
            subject_mapping[template_subject] = st.multiselect(
                f"**{template_subject}**", options=options, default=default_selection, key=f"map_{template_subject}"
            )

    if st.button("📥 テンプレート形式でダウンロード", type="primary"):
        placeholders = {'Y': year, 'n': survey_number, 'MM': month}
        try:
            output, match_info = write_to_template(df, question_cols, subject_mapping, placeholders)
            st.download_button(
                "💾 ファイルを保存", output, "survey_analysis_template.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.success("✅ ダウンロードの準備ができました。")
        except Exception as e:
            st.error(f"❌ テンプレート処理中にエラーが発生しました: {e}")

def render_integrated_download_option(df):
    """統合形式でのダウンロードオプションを表示する"""
    st.info("📊 全体シートと各教科シートを含む統合Excelファイル（生データ）をダウンロードします")
    if st.button("📥 統合形式でダウンロード", type="primary"):
        try:
            output = create_integrated_raw_data_excel(df)
            st.download_button(
                "💾 ファイルを保存", output, "survey_raw_data_integrated.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.success("✅ ダウンロードの準備ができました。")
        except Exception as e:
            st.error(f"❌ 統合Excelファイルの生成中にエラーが発生しました: {e}")

def render_subject_download_option(df, stats_df, overall_avg, subject, metadata):
    """科目別形式でのダウンロードオプションを表示する"""
    st.info("📊 選択された科目の集計結果をダウンロードします")
    download_df = create_download_data(stats_df, overall_avg, subject)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        download_df.to_excel(writer, sheet_name='集計結果', index=False)
        if metadata.get('free_text_column'):
            comments = extract_free_comments(df, metadata['free_text_column'], exclude_empty=True)
            if comments:
                pd.DataFrame({'意見・感想': comments}).to_excel(writer, sheet_name='自由記述', index=False)
    output.seek(0)
    st.download_button(
        "📥 標準形式でダウンロード", output, f"survey_analysis_{subject}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

def main():
    """メインアプリケーション"""
    st.set_page_config(page_title="授業アンケート分析システム", page_icon="📊", layout="wide")
    st.title("📊 授業アンケート分析システム")
    st.header("📁 データアップロード")
    
    uploaded_files = st.file_uploader(
        "CSV/Excelファイルをアップロードしてください",
        type=['csv', 'xlsx', 'xls'], accept_multiple_files=True
    )

    if not uploaded_files:
        show_welcome_message()
        return

    try:
        all_data = [load_and_process_csv(f)[0] for f in uploaded_files]
        all_metadata = [load_and_process_csv(f)[1] for f in uploaded_files]
        combined_df = pd.concat(all_data, ignore_index=True)
        
        subject_col = detect_subject_column(combined_df)
        st.markdown("---")
        st.header("🔍 フィルタ設定")

        if subject_col and subject_col in combined_df.columns:
            subjects = sorted([str(s) for s in combined_df[subject_col].unique() if pd.notna(s)])
            selected_subject = st.selectbox("分析する科目を選択", ["全体"] + subjects)
            filtered_df = combined_df if selected_subject == "全体" else combined_df[combined_df[subject_col] == selected_subject]
        else:
            st.warning("⚠️ 科目名カラムが検出されませんでした")
            filtered_df = combined_df
            selected_subject = "全体"

        if filtered_df.empty:
            st.error("選択された科目にデータがありません")
            return

        metadata = all_metadata[0]
        question_cols = metadata['question_columns']
        stats_df = calculate_statistics(filtered_df, question_cols)
        overall_avg = get_overall_average(filtered_df, question_cols)

        st.header(f"📈 分析結果: {selected_subject}")
        col1, col2, col3 = st.columns(3)
        col1.metric("📝 回答者数", f"{len(filtered_df)}人")
        col2.metric("⭐ 総合平均点", f"{overall_avg:.2f}")
        col3.metric("📋 質問項目数", f"{len(question_cols)}項目")
        st.markdown("---")

        tab1, tab2, tab3 = st.tabs(["📊 集計結果", "💬 自由記述", "📥 ダウンロード"])
        with tab1:
            render_statistics_tab(stats_df, overall_avg)
        with tab2:
            render_comments_tab(filtered_df, metadata)
        with tab3:
            render_download_tab(combined_df, filtered_df, stats_df, overall_avg, selected_subject, metadata)

    except Exception as e:
        st.error(f"❌ エラーが発生しました: {e}")
        st.exception(e)

if __name__ == "__main__":
    main()