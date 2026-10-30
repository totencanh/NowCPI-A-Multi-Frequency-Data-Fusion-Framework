# README — Cải tiến 2: phương pháp, feature store PIT và ablation

## Phạm vi

Thử các cấu hình từ baseline → thêm phương pháp → thêm nguồn; lưu tham số và kết quả từng fold; kiểm tra feature theo as-of cutoff; đo chất lượng, độ trễ và lỗi. Tất cả kết quả dưới đây là nghiên cứu chẩn đoán, không thay đổi mô hình production hiện tại.

## Thay đổi trong pipeline

- Thêm [`gold_nowcasting_features_pit`](../infra/dbt/models/gold/gold_nowcasting_features_pit.sql), projection dành riêng cho phân tích có kiểm soát thời gian. Feature chỉ được giữ nếu `available_at` của nguồn không sau cutoff và ngày/năm quan sát cũng không vượt cutoff. Giá trị không an toàn được để `NULL`; không điền lại bằng dữ liệu mới hơn.
- Bổ sung `cpi_latest_available_at` vào Gold feature store để kiểm soát CPI giống Brent, FX, fuel và IIP.
- Thêm kiểm tra dbt [`gold_nowcasting_features_pit_integrity`](../infra/dbt/tests/gold_nowcasting_features_pit_integrity.sql) cho timestamp, observation cutoff và giá trị bị null hóa khi không đủ điều kiện.
- Thêm ablation rolling-origin: naive CPI, AR-MIDAS Ridge hiện tại, MIDAS Elastic Net, full multi-source Ridge và full multi-source Elastic Net. Mô hình Elastic Net chọn alpha/l1 ratio qua expanding validation bên trong tập train; imputation và scaling chỉ fit trên train. Full model thêm Brent MTD, USD/VND, giá xăng RON95/E10, IIP và indicator thiếu nguồn.
- Thêm task `dbt_pit_feature_tests` và `feature_ablation_evaluation` sau dbt Gold vào DAG hằng ngày. Task benchmark có thể thất bại độc lập với task nowcast; các output giữ lại số fold lỗi để kiểm tra.

## Kiểm chứng trên dữ liệu hiện tại

Projection PIT build thành công **142 dòng**; dbt integrity test **PASS**. Audit Gold PIT có 1 dòng feature với nguồn hiện thời đủ điều kiện trong 142 dòng. Tuy nhiên, trong **140 tháng CPI lịch sử có nhãn**, không tháng nào có feature mở rộng từ Brent, USD/VND, fuel hoặc IIP vừa thỏa as-of cutoff lịch sử. Các batch nguồn được ghi nhận khi hệ thống đã thu thập chúng, sau các cutoff lịch sử; snapshot vintage trước đó không tồn tại.

Vì vậy kết quả ablation sử dụng đúng thông tin có thể chứng minh theo timestamp, và phần lớn historical macro feature bị loại. Đây là bằng chứng về **độ thiếu point-in-time vintage**, không phải bằng chứng rằng các nguồn bổ sung không có giá trị kinh tế.

### So sánh trên 36 mốc chung

| Cấu hình | MAE (điểm %) | RMSE (điểm %) | Đúng chiều |
|---|---:|---:|---:|
| Naive giữ CPI gần nhất | 0,3645 | 0,4705 | 77,78% |
| AR(1) | 0,2729 | 0,3643 | 86,11% |
| CPI-only Ridge | 0,2716 | 0,3627 | 86,11% |
| Brent-only U-MIDAS Ridge | **0,1998** | **0,2776** | 91,67% |
| AR-MIDAS Ridge | 0,2028 | 0,2871 | 91,67% |
| MIDAS Elastic Net | 0,2223 | 0,3372 | 91,67% |
| MIDAS Lasso | 0,2476 | 0,3503 | 86,11% |
| MIDAS Huber | 0,2047 | 0,2888 | 91,67% |
| Full multi-source Ridge | 0,2029 | 0,2873 | 91,67% |
| Full multi-source Elastic Net | 0,2222 | 0,3374 | 91,67% |

Brent-only U-MIDAS có MAE thấp hơn AR-MIDAS **1,48%** trên 36 mốc, nhưng CI chênh lệch 95% **[-0,01875; 0,01153]** chứa 0 và DM `p=0,736`. Trên 24 mốc, U-MIDAS kém hơn AR-MIDAS 3,05% về MAE (DM `p=0,560`). Vì vậy chưa có uplift bền vững để đổi mô hình chính; U-MIDAS được giữ như ứng viên benchmark.

Full multi-source Ridge so với AR-MIDAS có chênh lệch MAE **+0,000085 điểm %**, CI 95% **[-0,000161; 0,000316]**, DM `p=0,589`; không có bằng chứng cải thiện. Các feature mở rộng không khả dụng trong lịch sử này, nên hiệu năng gần như trùng AR-MIDAS.

Elastic Net có MAE cao hơn AR-MIDAS **9,59%** ở cửa sổ 36 mốc; Huber cao hơn **0,92%**. Các CI paired của hai cấu hình này chứa 0. Lasso có MAE cao hơn **22,06%**, bootstrap CI chênh lệch MAE **[0,0008; 0,0922] điểm %**, nhưng DM `p=0,075`; bằng chứng chưa hoàn toàn đồng thuận. Elastic Net và Lasso tốn thời gian hơn Ridge; chưa có lý do để thay AR-MIDAS Ridge.

### Latency, bộ nhớ, completeness và lỗi

- Ablation gồm 131 outer origins đủ điều kiện cho mô hình mở rộng; bảng metric so sánh chung có 130 origins khi yêu cầu tất cả cấu hình và baseline cùng có dự báo.
- Tổng wall time benchmark: **57,347 giây**; peak RSS tiến trình khoảng **135.212 KB**. Thời gian fit/predict cộng dồn: AR-MIDAS Ridge **0,427 giây**, Brent-only U-MIDAS **0,411 giây**, MIDAS Elastic Net **12,854 giây**, Lasso **6,269 giây**, Huber **12,689 giây**, full Ridge **0,942 giây**, full Elastic Net **19,564 giây**.
- Có **5 model-fold không đủ số quan sát train hoàn chỉnh** ở mốc khởi đầu cho các cấu hình MIDAS; được ghi trong audit, không phải lỗi kết nối hoặc lỗi pipeline.
- Ba trường hợp sai số lớn nhất của từng mô hình được lưu riêng. Hiệu năng các cấu hình vẫn là pseudo-vintage vì lịch sử source vintage đầy đủ chưa được lưu.
- Không quy đổi chi phí thành tiền: triển khai chạy local trong Docker và không có billing telemetry. Runtime/RSS là chỉ số vận hành hiện đo được.

## Quyết định mô hình

Giữ AR-MIDAS Ridge làm production. Brent-only U-MIDAS có MAE thấp hơn trên 36 mốc nhưng chưa có khác biệt có ý nghĩa và không giữ lợi thế trên 24 mốc. Elastic Net, Lasso và Huber cũng chưa chứng minh tốt hơn ổn định. Không bật thêm FX, fuel hoặc IIP vào dự báo production dựa trên benchmark hiện tại vì historical as-of coverage là 0/140 tháng; cần bắt đầu lưu snapshot/version có `available_at` và source release/version metadata trước, sau đó tích lũy đủ thời gian để làm ablation point-in-time có ý nghĩa. Không chọn LightGBM lúc này vì dữ liệu monthly hiện hữu quá ngắn cho rolling-origin mạnh và còn thiếu vintage; thêm độ phức tạp sẽ không giải quyết được thiếu thông tin theo thời gian.

## Artifact

- [Rolling-origin metrics](../forecasting/outputs/feature_ablation_metrics.csv)
- [Paired CI/DM comparisons](../forecasting/outputs/feature_ablation_paired_comparisons.csv)
- [Per-fold predictions và tham số Elastic Net](../forecasting/outputs/feature_ablation_predictions.csv)
- [As-of eligibility theo tháng](../forecasting/outputs/feature_ablation_asof_audit.csv)
- [Độ phủ nguồn theo lịch sử](../forecasting/outputs/feature_ablation_source_coverage.csv)
- [Runtime, RSS và số fold thành công/lỗi](../forecasting/outputs/feature_ablation_runtime.csv)
- [Lỗi thực thi theo fold](../forecasting/outputs/feature_ablation_failures.csv)
- [Ba trường hợp sai số lớn](../forecasting/outputs/feature_ablation_error_cases.csv)
- [Protocol, lưới tham số và chính sách as-of](../forecasting/outputs/feature_ablation_protocol.json)
- [Benchmark implementation](../forecasting/benchmark/evaluate_feature_ablation.py)
