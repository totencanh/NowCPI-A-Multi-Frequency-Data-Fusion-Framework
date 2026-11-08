# Đánh giá nguồn dữ liệu point-in-time cho NowCPI

Ngày rà soát: 31/10/2026

## Phạm vi địa lý

Các biến đặc thù quốc gia chỉ dùng dữ liệu Việt Nam và gắn `country=VNM`: USD/VND, giá xăng Việt Nam, IIP Việt Nam và CPI Việt Nam. Không dùng tỷ giá hay IIP của nước khác làm proxy cho Việt Nam.

Brent là chuẩn giá dầu quốc tế, không phải chuỗi riêng của một quốc gia. Đây là biến thị trường toàn cầu đưa vào bài toán Việt Nam; giữ nguồn/series là Brent toàn cầu và gắn `country=VNM` chỉ ở lớp feature/target-economy để nói rõ mô hình áp dụng cho Việt Nam. Không lọc Brent theo mã quốc gia VNM như thể nơi niêm yết Brent ở Việt Nam.

## Kết luận

Không cần thay cả bốn nguồn cùng lúc. Vấn đề chính là mỗi nguồn hiện chỉ giữ chuỗi hiện tại hoặc thiếu lịch sử phiên bản, nên pipeline không chứng minh được giá trị nào đã có mặt tại cutoff quá khứ. Có thể cải thiện theo hướng dùng archive gốc khi có, và bắt đầu append snapshot point-in-time từ nay trở đi cho nguồn không có vintage.

| Nguồn hiện tại | Hạn chế đã thấy | Nguồn/biện pháp phù hợp | Quyết định |
|---|---|---|---|
| Brent — Yahoo Finance `BZ=F` | Ticker futures; event chưa có `release_ts`; dữ liệu lịch sử tải lại là chuỗi hiện tại, không giữ vintage theo cutoff. | EIA Brent spot Europe qua FRED `DCOILBRENTEU`, lấy ngày vintage từ ALFRED/FRED API; yêu cầu FRED API key. Đây là Brent quốc tế, vẫn dùng cho mô hình Việt Nam; không phải dữ liệu dầu của nước khác. | Dùng EIA/FRED làm ứng viên thay Brent futures trong benchmark, gắn target economy `VNM`, giữ ticker hiện tại để đối chiếu. Không đổi production trước khi so sánh cùng kỳ và kiểm tra đơn vị/coverage. |
| USD/VND — Yahoo Finance `VND=X` | Market quote không có publication timestamp hay archive vintage trong collector. | SBV công bố tỷ giá trung tâm USD/VND hằng ngày trước 09:00 ngày hiệu lực; đây là tỷ giá chính thức của Việt Nam, khác với tỷ giá giao dịch thị trường. Lưu thành series `usd_vnd_sbv_central` với `country=VNM`, không ghi đè chuỗi market `usd_vnd`. Với lịch sử market, tiếp tục Yahoo và tích lũy snapshot; archive bên thứ ba cần kiểm tra license/độ tin cậy trước khi dùng. | Không tìm được nguồn công khai, miễn phí, có vintage lịch sử cho đúng chuỗi market USD/VND để thay trực tiếp. Không dùng chuỗi tỷ giá nước khác. Có thể bổ sung SBV central rate như một feature Việt Nam riêng; market FX hiện chỉ có vintage đáng tin từ lúc bắt đầu lưu snapshot hoặc qua vendor có điều khoản phù hợp. |
| Giá xăng Việt Nam — GiaXangHomNay collector | Bản ghi lịch sử theo ngày không tự chứng minh thời điểm công bố/phiên bản mà mô hình biết giá. | Thông cáo báo chí Petrolimex có archive; trang công bố ngày đăng và thông cáo nêu giờ hiệu lực (ví dụ điều chỉnh 15:00). Lấy từng lần điều chỉnh làm price vintage, giữ sản phẩm và vùng áp dụng của Việt Nam. | Ưu tiên Petrolimex archive cho giá công bố tại Việt Nam. Không giả định chuỗi giá mỗi ngày là mỗi ngày có một release; forward-fill giá chỉ trong khoảng hiệu lực được văn bản xác nhận. |
| IIP Việt Nam — World Bank API annual | Collector cũ lấy chỉ tiêu annual, không phải IIP và không phù hợp tần suất nowcasting. | Đã chuyển sang NSDP của NSO: `IIPVNM.xml` và bộ dữ liệu Excel `IIP_VNM.xlsx`, chọn chuỗi aggregate Industry theo mã quốc gia Việt Nam. | Collector hiện phát `iip_total_index`, frequency monthly, `country=VNM`. Gold tính IIP YoY từ index tháng hiện tại so với cùng tháng năm trước. World Bank collector đã bị gỡ khỏi DAG/Kafka publisher; legacy events bị Silver loại khỏi curated IIP. |

## Định nghĩa vintage cần lưu

Mỗi phiên bản quan sát phải giữ tối thiểu:

```text
source, series_id, observation_period, value, unit,
source_release_ts, source_release_date, available_at,
ingested_at, vintage_id, source_record_id, raw_payload_hash
```

- `source_release_ts`: thời điểm công bố do nguồn xác nhận. Nếu chỉ biết ngày thì lưu ngày và đánh dấu độ chính xác `date`; không tự đặt thành giờ chính xác.
- `available_at`: thời điểm bản ghi thực sự được pipeline nhìn thấy. Đây là mốc cần dùng cho khả năng tái lập hệ thống; nếu có timestamp công bố đáng tin cậy, PIT cutoff yêu cầu cả hai mốc không muộn hơn cutoff.
- `vintage_id`: định danh phiên bản/batch bất biến. Khi một quan sát sửa đổi, append phiên bản mới, không ghi đè phiên bản cũ.
- Với nguồn chỉ có ngày hiệu lực (giá xăng) hoặc ngày phát hành của bản tin, cutoff theo ngày phải dùng quy tắc bảo thủ thống nhất; không tuyên bố chính xác theo giờ khi không có giờ đăng. Collector NSDP XML hiện chưa gắn ngày phát hành cho từng quan sát IIP.

Điều kiện đưa vào fold:

```text
source_release_ts <= forecast_as_of
available_at <= forecast_as_of
observation_period <= forecast_as_of
```

Với archive có vintage date (ALFRED), chọn đúng bản vintage mới nhất nhưng không vượt `forecast_as_of`, sau đó vẫn lưu `available_at` của pipeline. Với archive bài viết NSO/Petrolimex, lấy chính nội dung bài có ngày công bố trước cutoff và bảo toàn payload/hash.

## Tác động lên backtest hiện tại

### Hai luồng đánh giá sau khi bổ sung proxy

- **Strict PIT** vẫn yêu cầu vintage được lưu trước cutoff. Kết quả 0/140 trước đây không bị thay đổi hoặc “sửa” bằng cách bỏ điều kiện.
- **Pseudo-vintage proxy** được chạy riêng ở `feature_ablation_sourcewise_proxy.csv`, predictions và `feature_ablation_proxy_asof_audit.csv`. Brent, USD/VND và xăng dùng `source_release_ts` nếu có; nếu thiếu thì giả định observation date + 1 ngày UTC. IIP dùng timestamp nguồn nếu có; nếu thiếu thì giả định cuối tháng quan sát + 15 ngày theo giờ Việt Nam. Các mốc cũ được dựng từ chuỗi quan sát hiện có nên có thể chứa giá trị đã sửa đổi về sau; đây là phân tích độ nhạy, không phải tái tạo lịch sử phát hành.
- Báo cáo pipeline đọc bảng proxy riêng nhưng ghi rõ không phải strict PIT. Chỉ số proxy có thể giúp xem kết luận thay đổi thế nào dưới giả định khả dụng; không được dùng để tuyên bố đã chứng minh giá trị đa nguồn tại thời điểm thực.
- DAG hàng ngày tiếp tục append snapshot từ lúc pipeline nhìn thấy dữ liệu. Các snapshot tích lũy về sau tạo được origin quan sát thực của pipeline, nhưng không tự tạo vintage lịch sử và không chứng minh ngày công bố của nhà phát hành.

Để chuyển từ proxy sang bằng chứng lịch sử, cần nhập archive gốc có ngày vintage/phát hành đã xác minh cho từng nguồn. Nếu không có archive cho một nguồn, giữ strict PIT là “chưa đủ dữ liệu” và chỉ báo proxy như một phân tích phụ.

Rerun `nowcpi_daily_pipeline` ngày 31/10/2026 thành công. Sau run, Trino có **9.338 phiên bản quan sát**, **142 hàng** trong PIT feature view, và **5 snapshot Gold** cho target `2026-10`. Cả 5 snapshot đều có Brent, USD/VND, xăng và IIP eligible (cùng snapshot CPI thì coverage 5/5); timestamp availability hiện chủ yếu dựa trên `source_ingested_at`. Benchmark vẫn có **0/140 mốc lịch sử** với macro source PIT: các snapshot mới được ghi từ cuối tháng 10/2026, sau cutoff phát hành của phần lớn CPI target lịch sử. Việc backtest gắn snapshot mới vào cutoff cũ sẽ là leakage.

Primary nowcast và ablation hiện cùng chọn snapshot append-only theo `target_month`, `asof_cutoff_ts`, `snapshot_recorded_at` và `available_at`. Snapshot October 2026 chỉ có thể thành một origin huấn luyện sau khi CPI October được công bố; lần phát hành CPI tiếp theo sẽ là cơ hội đầu tiên kiểm chứng tăng coverage. Để tăng coverage nhanh hơn mà vẫn đúng PIT, cần nhập archive có vintage/release date kiểm chứng được cho lịch sử (ví dụ ALFRED/FRED cho Brent; archive NSO/Petrolimex cho IIP/xăng; nguồn FX có lịch sử phiên bản). Không tự gán lại `available_at` của dữ liệu backfill về ngày quan sát.

Task ablation chạy lại sau khi đổi sang snapshot đạt **131 target rows, 0 runtime failures**, thời gian **65,81 giây**, peak RSS **153.488 KB**. Có **399 model-fold không đủ điều kiện dữ liệu**; đây là thiếu PIT source history chứ không phải lỗi thực thi. Nowcast production hiện dùng 139 training rows, nhưng vẫn **0 training origin có macro source**; snapshot hiện hành có 4/4 macro sources. Chưa có bằng chứng để kết luận mô hình đa nguồn cải thiện MAE/RMSE cho tới khi có các origin PIT đã được gắn nhãn CPI.

Việc khởi tạo version history giúp giữ các bản khác nhau đã thực sự có trong Bronze và từ nay các lần ingest/snapshot tiếp theo sẽ được lưu lại. Nó **không tái tạo vintage lịch sử bị thiếu**: chuỗi Brent, USD/VND, xăng và NSO IIP tải hiện tại vẫn chỉ chứng minh pipeline đã quan sát chúng tại thời điểm ingest. Vì thế 140 cutoff/backtest lịch sử vẫn không tự động trở thành vintage thật; ablation point-in-time theo nguồn cần chờ đủ mốc snapshot tương lai hoặc nhập archive có ngày phát hành/vintage xác minh được. 100% coverage của snapshot hiện tại chỉ cho biết 5 nhóm nguồn có mặt ở lần build đó, không phải độ phủ point-in-time tại quá khứ hay độ chính xác dự báo.

Thứ tự `available_at` hiện là source release timestamp → collector `source_ingested_at` → Kafka timestamp → Bronze ingestion timestamp, và Gold giữ thêm `available_at_basis` để chỉ rõ timestamp nào được dùng. Các source release timestamp chưa có ở phần lớn nguồn nên chưa thể báo release-to-ingestion latency thực. Tên trường `source_ingested_at` biểu thị lúc collector lấy/nhìn thấy observation; không được diễn giải như ngày công bố chính thức.

Thứ tự triển khai nên là:

1. Đã chuyển IIP sang NSO monthly; full-history Silver/Gold đã build thành công.
2. Đã thêm version history trong Silver, availability basis và append-only Gold snapshot; tiếp tục chạy collector/DAG định kỳ để tích lũy các snapshot tương lai.
3. Cấu hình FRED API key qua secret/env cho Brent EIA/ALFRED; lấy vintage dates và lưu archive bất biến nếu muốn mở rộng backtest lịch sử có chứng cứ.
4. Với FX, lưu song song `usd_vnd_market` (snapshot từ ngày thu thập) và `usd_vnd_sbv_central` (official rate) như hai feature Việt Nam riêng; chỉ so sánh/đưa vào mô hình sau khi đánh giá.
5. Chạy lại sourcewise ablation khi có ít nhất 24 paired origins point-in-time; đến lúc đó phải ghi rõ trạng thái chưa đủ bằng chứng, không suy ra nguồn vô ích.

## Nguồn tham khảo

- [NSO — archive IIP tháng](https://www.nso.gov.vn/en/iip/) (bài có kỳ tham chiếu và ngày phát hành).
- [NSDP Vietnam — IIP Excel and SDMX downloads](https://nsdp.nso.gov.vn/) (IIP Vietnam dataset; direct SDMX resource: `https://nsdp.nso.gov.vn/GSO-chung/SDMXFiles/GSO/IIPVNM.xml`).
- [NSO — IIP tháng 12/2025](https://www.nso.gov.vn/en/data-and-statistics/2026/01/industrial-production-index-in-december-2025/) (Date of issue: 06/01/2026).
- [Petrolimex — archive thông cáo báo chí](https://pdas.petrolimex.com.vn/ndi/thong-cao-bao-chi/18.html) (các thông cáo ghi ngày đăng và giờ hiệu lực điều chỉnh).
- [FRED/EIA — daily Brent spot Europe](https://fred.stlouisfed.org/series/DCOILBRENTEU).
- [FRED API — observations và output/vintage options](https://fred.stlouisfed.org/docs/api/fred/series_observations.html) (API yêu cầu key đăng ký; có vintage dates/output types).
- [FRED API — vintage dates](https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html).
- [SBV — lịch phổ biến thông tin thống kê ngành ngân hàng](https://www.sbv.gov.vn/documents/d/sbv_portal/527697) (tỷ giá trung tâm được công bố hằng ngày trước 09:00 ngày hiệu lực; lịch tham khảo 2023).
- [SBV — quy định công bố tỷ giá trung tâm hằng ngày](https://www.sbv.gov.vn/documents/20117/0/CV%2B7593Ngan%2Bhang%2Bdau%2Btu.pdf/1779d37d-fe6d-2117-deeb-d620841e24ad?t=1756977359192).
- [FRED — Vietnam exchange rate series metadata](https://fred.stlouisfed.org/data/FXRATEVNA618NUPN): chuỗi này chỉ annual 1970–2010, nên không dùng thay USD/VND daily.
