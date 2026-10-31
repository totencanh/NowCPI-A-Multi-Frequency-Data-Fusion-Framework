# README — Cải tiến 2: phương pháp, feature store PIT và ablation

## Phạm vi

Mô hình production chính là **Full multi-source Ridge**; naive CPI, AR(1), CPI-only, AR-MIDAS và các estimator khác làm baseline/ablation. Benchmark giữ so sánh rolling-origin trên cùng tập mốc, kiểm as-of cutoff, lưu version/hyperparameters/prediction/metrics riêng từng model. Kết quả lịch sử hiện vẫn là pseudo-vintage vì thiếu source vintage tại quá khứ; việc chọn mô hình đa nguồn làm chính chưa đồng nghĩa đã chứng minh nó tốt hơn.

## Rà soát theo checklist seminar — 31/10/2026

| Ý cần kiểm tra | Trạng thái hiện tại | Bằng chứng / giới hạn |
|---|---|---|
| Rolling-origin tối thiểu 24–36 mốc | **Đã có** | Báo cáo có cửa sổ 36 và 24 mốc; full history có 131 target rows. Đây là pseudo-vintage, không phải 131 vintage thật. |
| Naive, AR(1), AR-MIDAS; MAE/RMSE và Diebold–Mariano | **Đã có** | Cùng các mốc paired; CI bootstrap và DM được lưu. DM không báo cáo cho 9 mốc vì dưới ngưỡng 24. |
| Feature/hyperparameter selection chỉ dùng validation | **Không dùng outer test để chọn** | AR-MIDAS lag/penalty và Elastic Net/Lasso/Huber được chọn trong expanding inner-validation. Full multi-source Ridge giữ danh sách predictor đã định trước (không chọn subset theo kết quả test); PIT cutoff quyết định feature nào được phép hiện diện. |
| Nowcast thắng naive về chiều trong 9 mốc | **Có trong snapshot mới, nhưng chỉ mô tả** | Full multi-source đúng chiều 9/9 (100%), naive 7/9 (77,78%); MAE 0,268709 so với 0,439846 điểm %. N=9 nhỏ, pseudo-vintage; không xem là bằng chứng chắc chắn. |
| Release timestamp thực và chặn future leakage | **Chặn cutoff trong code; chưa có timestamp/vintage thật đầy đủ** | dbt PIT integrity test chạy trong DAG; NSO có ngày đăng cho 134/140 tháng nhưng không có giờ. Audit source `release_ts` CPI/Brent hiện 0%; lịch ngày và observation-date+1 là proxy, không phải release timestamp chính thức. |
| Freshness từ nguồn vận hành thay vì benchmark cô lập | **Đã đo từ source-check audit thật** | Báo cáo mới nhất: 5/6 nguồn đạt SLO (83,33%). USD/VND bị Yahoo rate limit; IIP hiện là NSO monthly và đạt freshness. Đây là observation-age freshness, chưa phải publication-to-ingestion latency. |
| Horizon backtest đủ dài | **Đã có horizon 36/24; vintage vẫn là giới hạn** | Nhiều mốc hơn cửa sổ 9 tháng đã được báo cáo, nhưng thiếu snapshot/vintage lịch sử nên không khẳng định strict real-time. |
| Baseline → phương pháp → full; version và hyperparameters | **Đã có và vừa chạy benchmark** | 14 model configurations, registry/version/code hash/hyperparameters; 0 runtime failures, 399 fold không đủ điều kiện. Leave-one-source-out Brent (cả tuần và tháng) có 130 pseudo-vintage origins; USD/VND, fuel và IIP chưa đủ PIT tại 131/131 mốc. |
| Đo vận hành song song chất lượng | **Đã có một phần** | Benchmark: 54,21 giây, peak RSS 137.620 KB, 0 lỗi runtime. Pipeline report vừa làm mới: DAG 582,95 giây; Kafka→Bronze P95 đạt 4/6 topic; 36/36 Silver integrity checks đạt. Chi phí tiền tệ và steady-state throughput SLO chưa có. |
| Insightfulness / policy gain | **Chưa đo** | Chưa có hàm tổn thất hay quyết định chính sách do stakeholder xác định; không gán uplift/policy gain giả từ MAE. |
| Độ bất định của chính dự báo | **Chưa có prediction interval được calibration** | CI hiện tại chỉ đo độ bất định của chênh lệch metric giữa hai model; chưa phải khoảng dự báo CPI hay coverage của khoảng dự báo. |

Benchmark mới nhất lưu tại `feature_ablation_run_manifest.json` với run ID `20261030T201343Z`. Báo cáo freshness/latency/throughput/quality vận hành thật vừa được làm mới lúc `2026-10-30 20:14:42 UTC` tại `workspace/pipeline-audit/reports/pipeline_metrics_latest.md` và `.json`.

## Thay đổi trong pipeline

- Thêm [`gold_nowcasting_features_pit`](../infra/dbt/models/gold/gold_nowcasting_features_pit.sql), projection dành riêng cho phân tích có kiểm soát thời gian. Feature chỉ được giữ nếu `available_at` của nguồn không sau cutoff và ngày/năm quan sát cũng không vượt cutoff. Giá trị không an toàn được để `NULL`; không điền lại bằng dữ liệu mới hơn.
- Bổ sung `cpi_latest_available_at` vào Gold feature store để kiểm soát CPI giống Brent, FX, fuel và IIP.
- Thêm kiểm tra dbt [`gold_nowcasting_features_pit_integrity`](../infra/dbt/tests/gold_nowcasting_features_pit_integrity.sql) cho timestamp, observation cutoff và giá trị bị null hóa khi không đủ điều kiện.
- Chọn `full_multisource_ridge` làm mô hình production chính. Nowcast lấy CPI lag, Brent weekly lags, Brent MTD, USD/VND, giá xăng RON95/E10, NSO IIP và missingness indicators từ Gold point-in-time features; chỉ sử dụng nguồn eligible trước cutoff, impute bằng median của tập train. AR-MIDAS và CPI-only còn là benchmark/fallback.
- Benchmark có cấu hình leave-one-source-group-out: lần lượt bỏ Brent, USD/VND, nhóm giá xăng Việt Nam (RON95 và E10), và IIP khỏi Full multi-source Ridge; so sánh trên các fold paired chung. Các cấu hình này chỉ có thể kết luận khi đủ vintage PIT lịch sử.
- Thêm ablation rolling-origin: naive CPI, AR(1), CPI-only Ridge, U-MIDAS/AR-MIDAS Ridge, Elastic Net, Lasso, Huber và full multi-source Ridge/Elastic Net. Elastic Net chọn alpha/l1 ratio bằng expanding validation trong train; preprocessing chỉ fit trên train.
- Mỗi cấu hình được lưu riêng dưới `forecasting/outputs/models/<model>/<version>/` với `manifest.json`, `predictions.csv`, `metrics.csv`, `runtime.csv`, `failures.csv`, `ineligible_folds.csv`; registry chung ghi role, predictor, version, hyperparameters, thời gian và số fold. `feature_ablation_failures.csv` chỉ còn lỗi thực thi; fold thiếu train history được ghi riêng là `not_evaluated` trong `feature_ablation_ineligible_folds.csv`.
- Thêm task `dbt_pit_feature_tests` và `feature_ablation_evaluation` sau dbt Gold vào DAG hằng ngày. Task benchmark có thể thất bại độc lập với task nowcast; các output giữ lại số fold lỗi để kiểm tra.
- Hai DAG chia sẻ Airflow pool `nowcpi_spark_pool` có một slot cho các task Spark wait/Silver, tránh chạy Silver hằng ngày và full-history đồng thời trên worker đơn.

## Kiểm chứng trên dữ liệu hiện tại

Projection PIT hiện có **142 dòng**; dbt integrity test **PASS**. Trong **140 tháng CPI lịch sử có nhãn**, không tháng nào có feature mở rộng từ Brent, USD/VND, fuel hoặc IIP đủ point-in-time theo cutoff. Kết quả dưới đây chạy trên collector IIP NSO và code mới của model đa nguồn.

Vì vậy kết quả ablation sử dụng đúng thông tin có thể chứng minh theo timestamp, và phần lớn historical macro feature bị loại. Đây là bằng chứng về **độ thiếu point-in-time vintage**, không phải bằng chứng rằng các nguồn bổ sung không có giá trị kinh tế.

### Ablation riêng từng nguồn

Đã bổ sung kiểm tra riêng Brent monthly, USD/VND, giá xăng (RON95/E10) và IIP. Một fold chỉ được chấm khi giá trị nguồn thật sự có sẵn theo `available_at` và as-of cutoff ở test origin; cờ “missing” không được tính nhầm là dữ liệu nguồn.

Kết quả lịch sử trước đây vẫn là **0/140 tháng đủ vintage xác thực cho từng nhóm nguồn**; rebuild không thể đảo ngược việc thiếu phiên bản tại các cutoff cũ. Full-history đã tạo `silver.observation_versions` (**9.333 phiên bản**), `gold_nowcasting_features_pit` (**142 hàng**) và snapshots; các timestamp snapshot hiện dựa trên `source_ingested_at`, chưa phải timestamp phát hành chính thức.

Benchmark được chạy lại sau khi bổ sung leave-one-source-group-out: **131 target rows**, **14 model configurations**, **0 runtime failures**, **399 fold không đủ điều kiện**. Cấu hình bỏ toàn bộ Brent tuần/tháng có 130 fold đánh giá được theo pseudo-vintage; bỏ USD/VND, fuel hoặc IIP thì mỗi cấu hình thiếu nguồn PIT tại 131/131 mốc. Năm fold đầu của một số baseline/model MIDAS khác cũng thiếu số hàng train tối thiểu. Các trường hợp được ghi trong `feature_ablation_ineligible_folds.csv`, không tính thành lỗi runtime. `feature_ablation_failures.csv` rỗng. Vintage phát hành Brent vẫn không được lưu, nên kết quả LOFO của Brent là chẩn đoán pseudo-vintage.

So sánh trên các mốc chung (pseudo-vintage, không phải bằng chứng real-time):

| Cửa sổ | Model | MAE (điểm %) | RMSE (điểm %) | Đúng chiều |
| --- | --- | ---: | ---: | ---: |
| 36 mốc | AR-MIDAS Ridge | 0,202210 | 0,288788 | 88,89% |
| 36 mốc | Full multi-source Ridge | 0,202314 | 0,288978 | 88,89% |
| 24 mốc | AR-MIDAS Ridge | 0,177036 | 0,252984 | 91,67% |
| 24 mốc | Full multi-source Ridge | 0,177154 | 0,253123 | 91,67% |

Full multi-source hiện kém AR-MIDAS rất nhẹ ở MAE/RMSE trên cả hai cửa sổ, cùng directional accuracy. Chênh lệch nhỏ này không chứng minh nguồn mới làm xấu mô hình; do 0/140 mốc có vintage nên các feature nguồn bổ sung chưa có lịch sử train point-in-time để học tác động. Mô hình đa nguồn đã là cấu hình chính theo yêu cầu, còn kết luận “đa nguồn cải thiện chất lượng” cần chờ snapshot tích lũy đủ forecast origins hợp lệ.

Nowcast chạy sau khi chuyển production selector trả `0,605245%` CPI MoM cho target `2026-10` (CPI thực tế mới nhất `2026-09`), chọn `full_multisource_ridge`. Tại cutoff hiện tại Gold có 5/5 nguồn, nhưng **0 training origins** có macro source PIT; trạng thái và artifact ghi rõ hiệu ứng lịch sử chưa định danh được.

Vì vậy MAE/RMSE uplift, CI và DM theo từng nguồn vẫn chưa thể kết luận từ 140 mốc cũ. Snapshot mới xác nhận nguồn có mặt tại một lần chạy, không đủ để đánh giá uplift; cần tích lũy ít nhất 24 paired forecast origins với timestamp/vintage hợp lệ hoặc bổ sung archive vintage có thể xác minh.

### So sánh rolling-origin trên các mốc chung

| Cấu hình | MAE (điểm %) | RMSE (điểm %) | Đúng chiều |
|---|---:|---:|---:|
| Naive giữ CPI gần nhất | 0,364489 | 0,470546 | 77,78% |
| AR(1) | 0,272864 | 0,364326 | 86,11% |
| CPI-only Ridge | 0,271621 | 0,362721 | 86,11% |
| Brent-only U-MIDAS Ridge | **0,198363** | **0,280191** | 91,67% |
| AR-MIDAS Ridge | 0,202210 | 0,288788 | 88,89% |
| MIDAS Elastic Net | 0,223000 | 0,338256 | 88,89% |
| MIDAS Lasso | 0,249662 | 0,350316 | 86,11% |
| MIDAS Huber | 0,203138 | 0,289168 | 88,89% |
| Full multi-source Ridge | 0,202314 | 0,288978 | 88,89% |
| Full multi-source Elastic Net | 0,222991 | 0,338415 | 88,89% |

Brent-only U-MIDAS có MAE thấp nhất trong 36 mốc nhưng vẫn là pseudo-vintage diagnostic; không được chọn làm production vì mục tiêu dự án là mô hình đa nguồn và chưa có source vintages để chứng minh khác biệt theo nguồn.

Full multi-source Ridge so với AR-MIDAS: trên 36 mốc, MAE chênh **+0,000104 điểm %**, CI 95% **[-0,000146; 0,000333]**, DM `p=0,509`; trên 24 mốc, MAE chênh **+0,000118 điểm %**, CI 95% **[-0,000087; 0,000300]**, DM `p=0,437`. Hai chênh lệch đều nhỏ, CI chứa 0, directional accuracy không đổi. Kết quả chưa cho thấy mô hình đa nguồn hơn AR-MIDAS; macro features không có training origins point-in-time nên tác động của chúng chưa được học/đo.

Trong 9 mốc mới nhất, Full multi-source đúng chiều **100%** so với naive **77,78%**, nhưng chỉ có 9 quan sát và DM không được báo cáo dưới 24 mốc. Kết quả pseudo-vintage này cập nhật nhận định directional trước đó; nó chưa đủ để kết luận tổng quát hay chứng minh strict real-time.

### Leave-one-source-out Brent

Lần đầu đã tắt đầy đủ khối Brent (12 weekly returns và Brent monthly) rồi so với Full multi-source Ridge trên cùng fold. Bỏ Brent làm MAE tăng **0,069307 điểm %** trên 36 mốc (95% CI **[0,032022; 0,108330]**, DM `p=0,000422`) và tăng **0,099988 điểm %** trên 24 mốc (95% CI **[0,059235; 0,139002]**, DM `p=0,000002`). Trên 9 mốc, MAE tăng **0,151503 điểm %**, nhưng đây chỉ là cửa sổ mô tả. Đây là bằng chứng pseudo-vintage về phần Brent weekly trong backtest, không phải vintage-accurate proof; monthly Brent, USD/VND, fuel và IIP riêng lẻ vẫn không có paired PIT origins để kết luận.

Elastic Net có MAE cao hơn AR-MIDAS khoảng **10,28%**, Huber khoảng **0,46%**, và Lasso khoảng **23,47%** ở cửa sổ này. Elastic Net/Lasso/Huber tốn thời gian hơn Ridge đáng kể; tiếp tục giữ chúng làm ablation, không làm mô hình chính.

### Latency, bộ nhớ, completeness và lỗi

- Ablation gồm 131 target rows; bảng so sánh common yêu cầu mọi model và baseline có dự báo nên có 130 origins. Lần benchmark lưu model artifacts cuối chạy **54,21 giây**, peak RSS **137.620 KB**. Fit/predict cộng dồn: AR-MIDAS Ridge **0,397 giây**, Brent-only U-MIDAS **0,383 giây**, Elastic Net **11,875 giây**, Lasso **5,707 giây**, Huber **11,588 giây**, full Ridge **0,852 giây**, full Elastic Net **18,215 giây**.
- Có **5 model-fold không đủ số quan sát train hoàn chỉnh** ở mốc khởi đầu; ghi là ineligible/not evaluated, không tính là lỗi runtime.
- Ba trường hợp sai số lớn nhất của từng mô hình được lưu riêng. Hiệu năng các cấu hình vẫn là pseudo-vintage vì lịch sử source vintage đầy đủ chưa được lưu.
- Không quy đổi chi phí thành tiền: triển khai chạy local trong Docker và không có billing telemetry. Runtime/RSS là chỉ số vận hành hiện đo được.

## Quyết định mô hình

Chọn **Full multi-source Ridge** làm production model theo định hướng nghiên cứu. Đây là lựa chọn kiến trúc/model chính, chưa phải kết luận chất lượng đã thắng AR-MIDAS. Khi chưa có source vintage lịch sử, output giữ flag nêu rõ các hiệu ứng macro chưa định danh; tiếp tục snapshot theo từng origin để đánh giá sau. Brent-only U-MIDAS, Elastic Net, Lasso và Huber tiếp tục là ablation/benchmarks. Không chọn LightGBM lúc này vì dữ liệu monthly còn ngắn và thiếu vintage; độ phức tạp thêm không bù được thiếu point-in-time history.

Đánh giá nguồn thay thế theo phạm vi Việt Nam nằm trong [đánh giá nguồn point-in-time](POINT_IN_TIME_SOURCE_REVIEW.md). Các biến CPI, FX, giá xăng và IIP phải là dữ liệu Việt Nam (`country=VNM`); Brent là chuẩn dầu quốc tế áp dụng làm biến đầu vào cho mô hình Việt Nam, không phải chuỗi của một quốc gia khác.

## Artifact

- [Rolling-origin metrics](../forecasting/outputs/feature_ablation_metrics.csv)
- [Paired CI/DM comparisons](../forecasting/outputs/feature_ablation_paired_comparisons.csv)
- [Per-fold predictions và tham số Elastic Net](../forecasting/outputs/feature_ablation_predictions.csv)
- [As-of eligibility theo tháng](../forecasting/outputs/feature_ablation_asof_audit.csv)
- [Độ phủ nguồn theo lịch sử](../forecasting/outputs/feature_ablation_source_coverage.csv)
- [Runtime, RSS và số fold thành công/lỗi](../forecasting/outputs/feature_ablation_runtime.csv)
- [Lỗi thực thi thực sự theo fold](../forecasting/outputs/feature_ablation_failures.csv)
- [Fold không đủ điều kiện đánh giá](../forecasting/outputs/feature_ablation_ineligible_folds.csv)
- [Model registry và version/hyperparameters](../forecasting/outputs/feature_ablation_model_registry.csv)
- [Run manifest](../forecasting/outputs/feature_ablation_run_manifest.json)
- [Artifact từng model](../forecasting/outputs/models/)
- [Ba trường hợp sai số lớn](../forecasting/outputs/feature_ablation_error_cases.csv)
- [Protocol, lưới tham số và chính sách as-of](../forecasting/outputs/feature_ablation_protocol.json)
- [Benchmark implementation](../forecasting/benchmark/evaluate_feature_ablation.py)
- [Ablation riêng từng nguồn](../forecasting/outputs/feature_ablation_sourcewise.csv)
- [Dự báo paired theo từng nguồn (hiện rỗng vì chưa đủ vintage)](../forecasting/outputs/feature_ablation_sourcewise_predictions.csv)
- [Freshness nguồn theo audit thực tế, tính đến 31/10/2026](../forecasting/outputs/source_freshness_asof_20261031.csv)
- [Báo cáo metric vận hành thật — Markdown](../workspace/pipeline-audit/reports/pipeline_metrics_latest.md)
- [Báo cáo metric vận hành thật — JSON](../workspace/pipeline-audit/reports/pipeline_metrics_latest.json)

### Freshness nguồn thực tế

Audit source-check mới nhất sau khi thay IIP sang NSO cho thấy **5/6 nguồn đạt freshness SLO (83,33%)**. NSO IIP tháng 09/2026 đã được collector ghi nhận; nguồn bị đánh dấu degraded là USD/VND do Yahoo Finance rate limit, dù observation gần nhất vẫn là 30/10/2026. IIP World Bank cũ được loại khỏi phép tính nguồn hiện hành.

Đây là observation-age freshness và trạng thái collector, **không phải độ trễ từ thời điểm nguồn phát hành đến lúc ingestion**. Báo cáo pipeline đo thêm Kafka→Bronze latency và DAG/task duration thật; 4/6 topic đạt P95 ≤60 giây, DAG gần nhất hoàn tất trong 582,95 giây (SLO ≤600 giây). USD/VND và fuel có P95 lần lượt 72,3 và 71,1 giây. Timestamp phát hành thực vẫn thiếu ở nhiều nguồn, nên chưa thể tính release-to-ingestion latency đáng tin cậy.
