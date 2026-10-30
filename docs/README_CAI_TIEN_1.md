# README — Cải tiến 1: đánh giá mô hình và dữ liệu theo thời điểm công bố

## Mục tiêu

Tài liệu này ghi lại cải tiến cho mục **2.1 — Tối ưu metric và chất lượng kết quả**: đánh giá rolling-origin, so sánh baseline theo cùng mốc dự báo, xử lý thời điểm dữ liệu có thể được biết, và kiểm tra khả năng lưu lịch sử phiên bản.

## Đã triển khai

- Rolling-origin evaluation so sánh naive giữ CPI gần nhất, AR(1), CPI-only Ridge, Brent-only U-MIDAS, AR-MIDAS cố định và AR-MIDAS chọn/blend bằng validation bên trong fold.
- Báo cáo MAE, RMSE, directional accuracy, sMAPE và MAPE. MAPE loại các tháng CPI MoM có trị tuyệt đối dưới 0,05 điểm phần trăm; số tháng tính/loại được ghi trong artifact.
- Paired comparison có chênh lệch tuyệt đối/tương đối, bootstrap CI theo block và Diebold–Mariano (DM). Cửa sổ 9 mốc chỉ mô tả, không dùng để tuyên bố ý nghĩa thống kê.
- Thêm collector lịch ngày đăng CPI chính thức của NSO và đưa vào DAG trước task nowcast. Lịch lưu snapshot bất biến, định danh theo nội dung.
- Backtest dùng ngày đăng NSO làm mốc ngày khi có; vì chỉ biết ngày, availability proxy bảo thủ là 00:00 ngày kế tiếp theo giờ Việt Nam. Thiếu ngày đăng thì dùng day-15 proxy. Brent thiếu release time dùng observation date + 1 ngày UTC.
- Audit phân biệt `release_ts` do nguồn công bố với `available_at` do pipeline ghi nhận. Audit raw kiểm kê batch bất biến và các event ID có thay đổi nội dung; không sửa collector trong `ingestion/`.

## Kiểm chứng DAG ngày 30/10/2026

DAG `nowcpi_daily_pipeline`, run `manual__nso_release_calendar_20261030`, hoàn tất thành công: collectors, lịch CPI NSO, Kafka, bronze catch-up, hai task Silver, dbt staging, dbt Gold và MIDAS.

### So sánh trên 36 mốc gần nhất

| Mô hình | MAE (điểm %) | RMSE (điểm %) | Đúng chiều |
|---|---:|---:|---:|
| Naive giữ CPI gần nhất | 0,3645 | 0,4705 | 77,78% |
| AR(1) | 0,2729 | 0,3643 | 86,11% |
| Brent-only U-MIDAS | 0,1952 | 0,2765 | 91,67% |
| AR-MIDAS cố định — production | 0,2028 | 0,2871 | 91,67% |
| AR-MIDAS validation-selected | 0,2155 | 0,2942 | 91,67% |
| AR-MIDAS validation blend | 0,2105 | 0,2952 | 91,67% |

AR-MIDAS cố định so với naive giảm MAE **44,35%** và RMSE **38,98%**; paired MAE CI 95% là **[-0,2110; -0,1131] điểm %**, DM `p=0,00000044`. So với AR(1), giảm MAE **25,66%**, RMSE **21,19%**; CI MAE **[-0,1083; -0,0329]**, DM `p=0,000337`.

### Độ bền trên 24 mốc

AR-MIDAS giảm MAE **48,09%** và RMSE **40,78%** so với naive (DM MAE `p=0,000072`); so với AR(1), giảm MAE **36,66%**, RMSE **31,40%** (DM MAE `p=0,000003`). Đúng chiều trên cửa sổ 24 mốc là 95,83%, so với naive 83,33%.

AR-MIDAS validation blend **không được chứng minh tốt hơn** AR-MIDAS cố định: trên 36 mốc blend có MAE cao hơn 0,0077 điểm %, khoảng tin cậy chứa 0, DM `p=0,0715`; trên 24 mốc blend cũng kém hơn nhẹ. Vì vậy production vẫn giữ AR-MIDAS cố định. Brent-only có MAE thấp nhất ở các cửa sổ gần nhất, nhưng đây là kết quả chẩn đoán trên mẫu pseudo-vintage; chưa đủ căn cứ để thay mô hình CPI autoregressive chính.

### Nowcast trong lần chạy

- CPI thực tế mới nhất: **09/2026**; mục tiêu một bước kế tiếp: **10/2026**.
- AR-MIDAS: **0,6415% MoM**.
- Đây là nowcast tháng tiếp theo sau CPI thực tế mới nhất, không phải CPI tháng đã được công bố. Kết quả là một điểm ước lượng, không phải xác nhận giá trị thực.
- Thời gian xử lý model/backtest trong lần chạy: **36,292 giây** (backtest 35,202 giây).

## Timestamp phát hành và vintage: kiểm tra nguồn

### Lịch CPI của NSO

Collector [`collect_nso_cpi_release_calendar.py`](../forecasting/source_history/collect_nso_cpi_release_calendar.py) đọc archive CPI của NSO, lưu kỳ tham chiếu và `Date of issue`, và tìm bổ sung bài CPI trên website NSO cho các kỳ thiếu trong archive tag. Snapshot hiện tại phủ **2015-01 đến 2026-09 (141 kỳ lịch)**; audit khớp được **140/140 tháng CPI có trong chuỗi hiện hành (100%)**. Lịch này chỉ có độ chính xác **ngày**, không có giờ-phút. Ví dụ trang NSO tháng 9/2026 ghi kỳ tham chiếu tháng 9 và ngày đăng 03/10/2026.

Ngày đăng không cung cấp giờ công bố chính xác, và CPI hiện hành từ IMF không phải vintage CPI đã biết tại từng ngày lịch sử. Do đó ngày NSO cải thiện mốc availability so với day-15 giả định, nhưng **chưa tạo strict real-time/vintage backtest**.

### Timestamp từ nguồn và thời gian pipeline

Audit của run ghi nhận:

| Nguồn | Bản ghi | Có source `release_ts` | Độ phủ |
|---|---:|---:|---:|
| CPI | 141 | 0 | 0% |
| Brent | 2.976 | 0 | 0% |

Các event trên có `available_at` từ `kafka_timestamp`; đây là lúc pipeline nhận/đưa event vào Kafka, không phải ngày/giờ nguồn phát hành. Vì vậy không dùng timestamp pipeline để giả làm timestamp nguồn.

### Lịch sử phiên bản dữ liệu đang lưu

Raw batch append-only hiện giữ các lần thu thập kể từ khi dự án bắt đầu lưu batch; audit mới ghi nhận:

| Dataset | Batch | Event ID duy nhất | Event ID có revision nội dung |
|---|---:|---:|---:|
| IMF CPI Việt Nam | 2 | 141 | 0 |
| IMF CPI components | 2 | 1.692 | 0 |
| Brent | 15 | 2.976 | 5 |
| USD/VND | 9 | 3.083 | 5 |
| Xăng E10 RON95 | 16 | 1.418 | 27 |
| World Bank IIP | 1 | 66 | 0 |

Đây là lịch sử phiên bản **kể từ batch đầu tiên được lưu**, không thể khôi phục snapshot hoặc giá trị đã công bố trước thời điểm đó. IMF SDMX, Yahoo Finance và API giá xăng hiện dùng không cung cấp vintage/release timestamp cho từng observation trong payload mà pipeline đang lấy. World Bank cũng lưu ý chuỗi lịch sử WDI có thể được sửa trong các lần cập nhật cơ sở dữ liệu.

**Kết luận về tính thời gian thực:** đã cải thiện backtest bằng lịch ngày đăng CPI NSO và cutoff bảo thủ; chưa thể gọi kết quả là strict real-time vì thiếu giờ công bố chính xác, vintage lịch sử CPI và release/version history hoàn chỉnh của các nguồn. Các bảng backtest gắn `pseudo_vintage_diagnostic`, không cho phép tuyên bố hiệu năng real-time.

## Artifact và mã liên quan

- [Metrics rolling-origin](../forecasting/outputs/midas_backtest_metrics.csv)
- [Paired comparisons, CI và DM](../forecasting/outputs/midas_paired_comparisons.csv)
- [Dự báo từng fold](../forecasting/outputs/midas_backtest_predictions.csv)
- [Nowcast và thời gian xử lý](../forecasting/outputs/midas_current_nowcast.json)
- [Audit lịch CPI NSO](../forecasting/outputs/nso_release_calendar_audit.csv)
- [Audit source release timestamp](../forecasting/outputs/release_timestamp_audit.csv)
- [Audit batch/event revisions](../forecasting/outputs/raw_event_version_audit.csv)
- [Protocol và giới hạn backtest](../forecasting/outputs/backtest_protocol.json)
- [Mã nowcast](../forecasting/midas_nowcast.py)
- [DAG](../infra/airflow/dags/nowcpi_daily_pipeline_dag.py)

## Nguồn chính thức

- [NSO — archive CPI](https://www.nso.gov.vn/en/cpi/)
- [NSO — CPI tháng 9/2026, kỳ tham chiếu và ngày đăng](https://www.nso.gov.vn/en/data-and-statistics/2026/10/consumer-price-index-gold-price-index-and-us-dollar-price-index-in-september-2026-q3-2026-and-the-nine-months-of-2026/)
- [IMF Data API — SDMX 2.1 và 3.0](https://data.imf.org/en/Resource-Pages/IMF-API)
- [World Bank — quản lý revisions của WDI](https://datahelpdesk.worldbank.org/knowledgebase/articles/114939-how-are-revisions-managed)
- [ICE — thông số Brent Futures](https://www.ice.com/products/219/Brent-Crude-Futures/specs)
