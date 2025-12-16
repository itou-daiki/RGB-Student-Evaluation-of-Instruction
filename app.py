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
from data_processor import (
    load_and_process_csv,
    calculate_statistics,
    get_overall_average,
    extract_free_comments,
    create_download_data,
    detect_subject_column,
)


# ページ設定
st.set_page_config(
    page_title="授業アンケート分析システム",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    """メインアプリケーション"""

    # タイトル
    st.title("📊 授業アンケート分析システム")
    st.markdown("---")

    # サイドバー
    st.sidebar.header("📁 データアップロード")
    st.sidebar.markdown("CSVファイルをアップロードしてください")

    # ファイルアップローダー（複数ファイル対応）
    uploaded_files = st.sidebar.file_uploader(
        "アンケートCSVファイル",
        type=['csv'],
        accept_multiple_files=True,
        help="Google FormsやMicrosoft Forms等から出力されたCSVファイルをアップロードしてください"
    )

    if not uploaded_files:
        # ファイルがアップロードされていない場合の説明
        st.info("👈 サイドバーからCSVファイルをアップロードしてください")

        st.markdown("""
        ### 使い方

        1. **CSVファイルのアップロード**: サイドバーから授業アンケートのCSVファイルをアップロードします
        2. **科目の選択**: 複数科目のデータが含まれる場合、分析したい科目を選択できます
        3. **ダッシュボードの確認**: 回答者数、平均点、質問ごとの統計が表示されます
        4. **グラフの確認**: 質問ごとの平均点を視覚的に確認できます
        5. **自由記述の確認**: 学生からの意見・感想を一覧で確認できます
        6. **データのダウンロード**: 集計結果をExcelファイルとしてダウンロードできます

        ### 対応フォーマット

        - Google Forms、Microsoft Formsなどから出力されたCSV
        - 質問項目の回答は4件法（「とてもそう思う」「そう思う」「あまりそう思わない」「思わない」）
        - 自動的に質問項目を検出し、スコアリングします

        ### サンプルデータの生成

        サンプルデータを生成するには、以下のコマンドを実行してください：

        ```bash
        python generate_sample_data.py
        ```
        """)
        return

    # データ読み込み
    try:
        all_data = []
        all_metadata = []

        for uploaded_file in uploaded_files:
            df, metadata = load_and_process_csv(uploaded_file)
            all_data.append(df)
            all_metadata.append(metadata)

        # データを結合
        combined_df = pd.concat(all_data, ignore_index=True)

        # 科目カラムを検出
        subject_col = detect_subject_column(combined_df)

        # サイドバー: 科目選択フィルタ
        st.sidebar.markdown("---")
        st.sidebar.header("🔍 フィルタ設定")

        if subject_col and subject_col in combined_df.columns:
            subjects = combined_df[subject_col].unique().tolist()
            subjects_sorted = sorted([str(s) for s in subjects if pd.notna(s)])

            selected_subject = st.sidebar.selectbox(
                "分析する科目を選択",
                ["全体"] + subjects_sorted,
                help="特定の科目のみを分析する場合は選択してください"
            )

            # フィルタリング
            if selected_subject == "全体":
                filtered_df = combined_df
            else:
                filtered_df = combined_df[combined_df[subject_col] == selected_subject]
        else:
            st.sidebar.warning("⚠️ 科目名カラムが検出されませんでした")
            filtered_df = combined_df
            selected_subject = "全体"

        # メタデータを取得（最初のファイルから）
        metadata = all_metadata[0]
        question_cols = metadata['question_columns']

        # データが空の場合
        if len(filtered_df) == 0:
            st.error("選択された科目にデータがありません")
            return

        # 統計情報を計算
        stats_df = calculate_statistics(filtered_df, question_cols)
        overall_avg = get_overall_average(filtered_df, question_cols)

        # ========================================
        # メインエリア: ダッシュボード
        # ========================================

        st.header(f"📈 分析結果: {selected_subject}")

        # KPI表示
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                label="📝 回答者数",
                value=f"{len(filtered_df)}人"
            )

        with col2:
            st.metric(
                label="⭐ 総合平均点",
                value=f"{overall_avg:.2f}",
                help="全質問項目の平均スコア（4点満点）"
            )

        with col3:
            st.metric(
                label="📋 質問項目数",
                value=f"{len(question_cols)}項目"
            )

        st.markdown("---")

        # タブで機能を分ける
        tab1, tab2, tab3 = st.tabs(["📊 集計結果", "💬 自由記述", "📥 ダウンロード"])

        # ========================================
        # タブ1: 集計結果
        # ========================================
        with tab1:
            st.subheader("質問項目ごとの集計")

            # 集計テーブル表示
            st.dataframe(
                stats_df.style.format({
                    '平均値': '{:.2f}',
                }),
                use_container_width=True,
                height=400
            )

            st.markdown("---")

            # グラフ表示: 質問ごとの平均点
            st.subheader("質問ごとの平均点（棒グラフ）")

            # 質問番号を振る（表示用）
            stats_df_plot = stats_df.copy()
            stats_df_plot['質問番号'] = [f"Q{i+1}" for i in range(len(stats_df_plot))]

            # 棒グラフ
            fig_bar = px.bar(
                stats_df_plot,
                x='質問番号',
                y='平均値',
                hover_data=['質問項目', '平均値', '有効回答数'],
                title="質問項目ごとの平均点",
                labels={'質問番号': '質問', '平均値': '平均点'},
                color='平均値',
                color_continuous_scale='RdYlGn',
                range_color=[1, 4],
            )

            fig_bar.update_layout(
                xaxis_title="質問項目",
                yaxis_title="平均点（4点満点）",
                yaxis_range=[0, 4.5],
                height=500,
                font=dict(size=12),
                hoverlabel=dict(font_size=14),
            )

            fig_bar.add_hline(
                y=overall_avg,
                line_dash="dash",
                line_color="blue",
                annotation_text=f"総合平均: {overall_avg:.2f}",
                annotation_position="right"
            )

            st.plotly_chart(fig_bar, use_container_width=True)

            # レーダーチャート（上位10項目と下位10項目）
            st.subheader("平均点の分布（レーダーチャート）")

            # 上位10項目を抽出
            top_10 = stats_df_plot.nlargest(10, '平均値')

            # レーダーチャート
            fig_radar = go.Figure()

            fig_radar.add_trace(go.Scatterpolar(
                r=top_10['平均値'].tolist(),
                theta=top_10['質問番号'].tolist(),
                fill='toself',
                name='上位10項目',
                hovertext=top_10['質問項目'].tolist(),
            ))

            fig_radar.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 4]
                    )
                ),
                showlegend=True,
                title="平均点上位10項目",
                height=500,
            )

            st.plotly_chart(fig_radar, use_container_width=True)

        # ========================================
        # タブ2: 自由記述
        # ========================================
        with tab2:
            st.subheader("💬 学生の意見・感想")

            free_text_col = metadata['free_text_column']

            if free_text_col:
                # フィルタオプション
                exclude_empty = st.checkbox(
                    "空白や「特になし」を除外する",
                    value=True
                )

                comments = extract_free_comments(
                    filtered_df,
                    free_text_col,
                    exclude_empty=exclude_empty
                )

                if comments:
                    st.info(f"📝 {len(comments)}件の意見・感想があります")

                    # コメントを表示
                    for i, comment in enumerate(comments, 1):
                        st.markdown(f"**{i}.** {comment}")
                else:
                    st.warning("意見・感想が見つかりませんでした")
            else:
                st.warning("⚠️ 自由記述カラムが検出されませんでした")

        # ========================================
        # タブ3: ダウンロード
        # ========================================
        with tab3:
            st.subheader("📥 集計結果のダウンロード")

            st.markdown("""
            集計結果をExcelファイルとしてダウンロードできます。
            ダウンロードされるデータには、質問ごとの平均値、回答数、分布が含まれます。
            """)

            # ダウンロード用データを作成
            download_df = create_download_data(stats_df, overall_avg, selected_subject)

            # Excelファイルとして出力
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # 集計結果シート
                download_df.to_excel(writer, sheet_name='集計結果', index=False)

                # 自由記述シート（ある場合）
                free_text_col = metadata['free_text_column']
                if free_text_col:
                    comments = extract_free_comments(filtered_df, free_text_col, exclude_empty=True)
                    if comments:
                        comments_df = pd.DataFrame({'意見・感想': comments})
                        comments_df.to_excel(writer, sheet_name='自由記述', index=False)

                # サマリーシート
                summary_data = {
                    '項目': ['科目名', '回答者数', '質問項目数', '総合平均点'],
                    '値': [selected_subject, len(filtered_df), len(question_cols), f"{overall_avg:.2f}"]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='サマリー', index=False)

            output.seek(0)

            # ダウンロードボタン
            st.download_button(
                label="📥 Excelファイルをダウンロード",
                data=output,
                file_name=f"survey_analysis_{selected_subject}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            st.success("✅ ダウンロードボタンをクリックしてファイルを保存してください")

    except Exception as e:
        st.error(f"❌ エラーが発生しました: {str(e)}")
        st.exception(e)


if __name__ == "__main__":
    main()
