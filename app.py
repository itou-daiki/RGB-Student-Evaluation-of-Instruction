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
    write_to_template,
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

    # ファイルアップロード（メインエリア）
    st.header("📁 データアップロード")
    uploaded_files = st.file_uploader(
        "CSV/Excelファイルをアップロードしてください",
        type=['csv', 'xlsx', 'xls'],
        accept_multiple_files=True,
        help="Google FormsやMicrosoft Forms等から出力されたCSV/Excelファイルをアップロードしてください"
    )

    if not uploaded_files:
        # ファイルがアップロードされていない場合の説明
        st.info("☝ 上記のエリアからCSV/Excelファイルをアップロードしてください")

        st.markdown("""
        ### 使い方

        1. **ファイルのアップロード**: サイドバーから授業アンケートのCSV/Excelファイルをアップロードします
        2. **科目の選択**: 複数科目のデータが含まれる場合、分析したい科目を選択できます
        3. **ダッシュボードの確認**: 回答者数、平均点、質問ごとの統計が表示されます
        4. **グラフの確認**: 質問ごとの平均点を視覚的に確認できます
        5. **自由記述の確認**: 学生からの意見・感想を一覧で確認できます
        6. **データのダウンロード**: 集計結果をExcelファイルとしてダウンロードできます

        ### 対応フォーマット

        - Google Forms、Microsoft Formsなどから出力されたCSV/Excel (.csv, .xlsx, .xls)
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

        # 科目選択フィルタ
        st.markdown("---")
        st.header("🔍 フィルタ設定")

        if subject_col and subject_col in combined_df.columns:
            subjects = combined_df[subject_col].unique().tolist()
            subjects_sorted = sorted([str(s) for s in subjects if pd.notna(s)])

            selected_subject = st.selectbox(
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
            st.warning("⚠️ 科目名カラムが検出されませんでした")
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
            """)

            # ダウンロード形式の選択
            download_format = st.radio(
                "ダウンロード形式を選択してください",
                ["テンプレートを使用", "標準形式"],
                help="テンプレートを使用する場合、全教科のデータがテンプレートに書き込まれます"
            )

            if download_format == "テンプレートを使用":
                st.info("📄 テンプレート.xlsxに全教科のデータを書き込みます")

                # テンプレートのプレースホルダー入力
                st.markdown("### 📝 テンプレート情報の入力")
                col1, col2, col3 = st.columns(3)

                with col1:
                    year = st.text_input("年度 (令和{Y}年度)", value="6", help="令和の年号を入力してください（例：6）")
                with col2:
                    survey_number = st.text_input("回数 (第{n}回)", value="1", help="アンケートの実施回数を入力してください（例：1）")
                with col3:
                    month = st.text_input("月 ({MM}月)", value="12", help="実施月を入力してください（例：12）")

                st.markdown("---")

                # 科目と教科のマッピング設定
                st.markdown("### 📋 教科と科目のマッピング設定")
                st.markdown("各教科にどの科目を含めるか選択してください。")

                # データに含まれる科目名を取得
                subject_col = detect_subject_column(combined_df)
                if subject_col and subject_col in combined_df.columns:
                    available_subjects = sorted([str(s) for s in combined_df[subject_col].unique() if pd.notna(s)])

                    # 教科名のリスト
                    template_subjects = ['国語', '数学', '地歴公民', '理科', '外国語', '保健体育', '芸術', '家庭', '情報']

                    # 教科名ごとのデフォルトキーワード（初期選択のための推奨）
                    default_keywords = {
                        '国語': ['国語', 'こくご'],
                        '数学': ['数学', 'すうがく'],
                        '地歴公民': ['地理', '歴史', '公民', '地歴', '社会'],
                        '理科': ['理科', '物理', '化学', '生物', '地学'],
                        '外国語': ['英語', '外国語', 'English'],
                        '保健体育': ['保健', '体育', 'たいいく'],
                        '芸術': ['音楽', '美術', '書道', '芸術'],
                        '家庭': ['家庭', 'かてい'],
                        '情報': ['情報', 'じょうほう'],
                    }

                    # 教科ごとの科目選択（マルチセレクト）
                    subject_mapping = {}

                    # 2列レイアウト
                    col1, col2 = st.columns(2)

                    for idx, template_subject in enumerate(template_subjects):
                        # すでに他の教科で選択されている科目を収集
                        already_selected = set()
                        for other_subject in template_subjects[:idx]:
                            # session_stateから以前選択された値を取得
                            key = f"subject_mapping_{other_subject}"
                            if key in st.session_state:
                                already_selected.update(st.session_state[key])

                        # この教科で利用可能な科目（他で選択されていないもの）
                        available_for_this = [s for s in available_subjects if s not in already_selected]

                        # 初期選択の推奨値を計算
                        default_selected = []
                        keywords = default_keywords.get(template_subject, [])

                        for subject in available_for_this:
                            # 完全一致または部分一致をチェック
                            if subject == template_subject:
                                default_selected.append(subject)
                            else:
                                for keyword in keywords:
                                    if keyword in subject:
                                        default_selected.append(subject)
                                        break

                        # 2列に分けて表示
                        with col1 if idx % 2 == 0 else col2:
                            selected = st.multiselect(
                                f"**{template_subject}**",
                                options=available_for_this,
                                default=default_selected,
                                key=f"subject_mapping_{template_subject}",
                                help=f"選択可能な科目: {len(available_for_this)}個"
                            )
                            subject_mapping[template_subject] = selected

                    st.markdown("---")

                    # ダウンロードボタン
                    if st.button("📥 テンプレート形式でダウンロード", type="primary"):
                        try:
                            # プレースホルダーの値を辞書にまとめる
                            placeholders = {
                                'Y': year,
                                'n': survey_number,
                                'MM': month
                            }

                            # テンプレートを使用してExcelファイルを生成
                            output, match_info = write_to_template(
                                combined_df,
                                question_cols,
                                subject_mapping=subject_mapping,
                                placeholders=placeholders
                            )

                            # マッチング情報を表示
                            st.markdown("---")
                            st.subheader("📊 質問項目のマッピング結果")

                            # サマリー情報
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("テンプレート質問項目", f"{match_info['template_question_count']}個")
                            with col2:
                                st.metric("データ質問項目", f"{match_info['data_question_count']}個")
                            with col3:
                                st.metric("マッピング成功", f"{match_info['total_matches']}個")

                            # マッチングタイプの内訳
                            col1, col2 = st.columns(2)
                            with col1:
                                st.info(f"✅ **完全一致**: {match_info['exact_matches']}個")
                            with col2:
                                st.info(f"🔄 **部分一致**: {match_info['partial_matches']}個")

                            # 余分な質問項目の警告
                            if match_info['excess_questions']:
                                with st.expander(f"⚠️ 質問項目が30個を超えています（余分な{len(match_info['excess_questions'])}個は無視されます）"):
                                    for q in match_info['excess_questions']:
                                        st.write(f"- {q}")

                            # マッピングされなかった質問項目
                            if match_info['unmapped_questions']:
                                with st.expander(f"⚠️ マッピングされなかった質問項目 ({len(match_info['unmapped_questions'])}個)", expanded=True):
                                    st.warning("以下のテンプレート質問項目にデータが見つかりませんでした:")
                                    for col_idx, q in match_info['unmapped_questions'][:10]:
                                        st.write(f"**[列{col_idx}]** {q}")
                                    if len(match_info['unmapped_questions']) > 10:
                                        st.write(f"... 他{len(match_info['unmapped_questions']) - 10}個")

                            # 部分一致の詳細
                            if match_info['partial_match_details']:
                                with st.expander(f"📋 部分一致の詳細 ({len(match_info['partial_match_details'])}個)"):
                                    for detail in match_info['partial_match_details'][:5]:
                                        st.markdown(f"**[列{detail['column']}]** {detail['match_type']}")
                                        st.write(f"- テンプレート: `{detail['template_question']}`")
                                        st.write(f"- データ: `{detail['data_question']}`")
                                        st.markdown("---")
                                    if len(match_info['partial_match_details']) > 5:
                                        st.write(f"... 他{len(match_info['partial_match_details']) - 5}個")

                            st.markdown("---")

                            # ダウンロード用のリンクを生成
                            st.download_button(
                                label="💾 ファイルを保存",
                                data=output,
                                file_name="survey_analysis_template.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            )

                            st.success("✅ ダウンロードボタンをクリックしてファイルを保存してください")
                        except Exception as e:
                            st.error(f"❌ テンプレートの処理中にエラーが発生しました: {str(e)}")
                            st.exception(e)
                else:
                    st.warning("⚠️ 科目名カラムが検出されませんでした。テンプレート形式でのダウンロードができません。")

            else:
                st.info("📊 選択された科目の集計結果をダウンロードします")

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
                    label="📥 標準形式でダウンロード",
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
