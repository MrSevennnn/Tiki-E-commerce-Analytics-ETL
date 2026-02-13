-- ============================================================================
-- BigQuery Schema Definition - Tiki E-commerce Analytics Platform
-- 1. Bảng Dimension: Danh mục ngành hàng
-- Lưu cấu trúc phân cấp ngành hàng
CREATE TABLE IF NOT EXISTS `<YOUR_PROJECT_ID>.TikiWarehouse.dim_categories` (
    category_id INT64 OPTIONS(description="ID danh mục lấy từ URL hoặc API"),
    category_name STRING OPTIONS(description="Tên hiển thị của danh mục"),
    category_level INT64 OPTIONS(description="Cấp độ danh mục: 1=Root, 2=Sub, 3=Deep (Leaf)"),
    full_path STRING OPTIONS(description="Đường dẫn đầy đủ breadcrumb để dễ debug"),
    url_key STRING OPTIONS(description="Slug URL (vd: dien-thoai-may-tinh-bang)"),
    parent_id INT64 OPTIONS(description="ID danh mục cha để dựng cây phân cấp"),
    standard_category STRING OPTIONS(description="Tên danh mục chuẩn hóa dùng cho báo cáo (Mapping thủ công)")
);

-- 2. Bảng Dimension: Sản phẩm
-- Lưu thông tin tĩnh/ít thay đổi của sản phẩm
CREATE TABLE IF NOT EXISTS `<YOUR_PROJECT_ID>.TikiWarehouse.dim_products` (
    product_id INT64 OPTIONS(description="Master ID của sản phẩm trên Tiki (Khóa chính)"),
    sku STRING OPTIONS(description="Mã SKU quản lý kho"),
    name STRING OPTIONS(description="Tên đầy đủ của sản phẩm"),
    brand_name STRING OPTIONS(description="Thương hiệu (Apple, Samsung...)"),
    image_url STRING OPTIONS(description="Link ảnh thumbnail"),
    product_url STRING OPTIONS(description="Link chi tiết sản phẩm"),
    seller_id INT64 OPTIONS(description="ID nhà bán hàng"),
    seller_name STRING OPTIONS(description="Tên nhà bán hàng (VD: Tiki Trading)"),
    category_id INT64 OPTIONS(description="FK trỏ về bảng dim_categories"),
    created_at TIMESTAMP OPTIONS(description="Thời điểm lần đầu tiên hệ thống quét thấy sản phẩm này"),
    updated_at TIMESTAMP OPTIONS(description="Thời điểm cập nhật thông tin mô tả gần nhất"),
    root_category_id INT64 OPTIONS(description="ID danh mục cấp 1 (Gốc) để báo cáo tổng quan (VD: Điện thoại & Tablet)")
)
CLUSTER BY product_id, brand_name; 
-- CLUSTER BY giúp bạn query "tìm tất cả sản phẩm của Apple" cực nhanh

-- 3. Bảng Fact: Dữ liệu biến động hàng ngày (QUAN TRỌNG NHẤT)
-- Lưu lịch sử giá, tồn kho, review theo ngày
CREATE TABLE IF NOT EXISTS `<YOUR_PROJECT_ID>.TikiWarehouse.fact_daily_snapshot` (
    snapshot_date DATE OPTIONS(description="Ngày thu thập dữ liệu (Partition Key)"),
    product_id INT64 OPTIONS(description="FK trỏ về dim_products"),
    
    -- Nhóm về Giá
    current_price INT64 OPTIONS(description="Giá bán thực tế tại thời điểm crawl (VND)"),
    original_price INT64 OPTIONS(description="Giá niêm yết/gạch ngang (VND)"),
    discount_rate INT64 OPTIONS(description="Phần trăm giảm giá (VD: 25 tương ứng 25%)"),
    
    -- Nhóm về Sức bán & Review
    sales_volume_acc INT64 OPTIONS(description="Tổng số lượng đã bán lũy kế (Parse từ text 'Đã bán...')"),
    review_count INT64 OPTIONS(description="Tổng số lượng đánh giá"),
    rating_average FLOAT64 OPTIONS(description="Điểm đánh giá trung bình (0.0 - 5.0)"),
    
    -- Nhóm trạng thái
    inventory_status BOOL OPTIONS(description="TRUE: Còn hàng, FALSE: Hết hàng/Ngừng kinh doanh"),
    tiki_now BOOL OPTIONS(description="Có hỗ trợ giao nhanh TikiNow không"),
    
    extracted_at TIMESTAMP OPTIONS(description="Thời gian chính xác crawler chạy xong")
)
PARTITION BY snapshot_date
CLUSTER BY product_id;

CREATE TABLE IF NOT EXISTS `<YOUR_PROJECT_ID>.TikiWarehouse.dim_keyword_mapping`(
  mapping_id INT64 OPTIONS(description="ID định danh duy nhất cho cặp mapping"),
  tiki_category_id INT64 OPTIONS(description="ID danh mục hoặc sản phẩm trên Tiki (VD: 1789)"),
  tiki_category_name STRING OPTIONS(description="Tên danh mục trên Tiki để dễ đọc"),
  trend_keyword STRING OPTIONS(description="Từ khóa thực tế sẽ gửi lên Google Trends"),
  is_active BOOLEAN DEFAULT TRUE OPTIONS(description="Trạng thái sử dụng (True/False)"),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `<YOUR_PROJECT_ID>.TikiWarehouse.fact_google_trends`
(
    date DATE OPTIONS(description="Ngày ghi nhận dữ liệu trend"),
    keyword STRING OPTIONS(description="Từ khóa (Khóa ngoại nối với dim_keyword_mapping)"),
    score INT64 OPTIONS(description="Điểm số quan tâm (0-100)"),
    is_partial BOOLEAN OPTIONS(description="True nếu Google đánh dấu dữ liệu chưa hoàn chỉnh (thường là ngày hiện tại)"),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Thời điểm crawler ghi dữ liệu vào bảng")
)
PARTITION BY date 
CLUSTER BY keyword;

CREATE TABLE IF NOT EXISTS `<YOUR_PROJECT_ID>.TikiWarehouse.dim_exchange_rate`
(
    date DATE OPTIONS(description="Ngày ghi nhận tỷ giá"),
    from_currency STRING DEFAULT 'USD' OPTIONS(description="Đồng tiền gốc"),
    to_currency STRING DEFAULT 'VND' OPTIONS(description="Đồng tiền quy đổi"),
    rate FLOAT64 OPTIONS(description="Tỷ giá (VD: 25300.5)"),
    source STRING OPTIONS(description="Nguồn dữ liệu (VD: open.er-api.com)"),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY date;

CREATE TABLE IF NOT EXISTS `<YOUR_PROJECT_ID>.TikiWarehouse.analytics_product_market_daily`
(
    date DATE,
    product_id STRING,
    product_name STRING,
    category_name STRING,
    category_level INT64,
    price_vnd_real FLOAT64,
    price_vnd_list FLOAT64,
    discount_percentage FLOAT64,
    price_usd_real FLOAT64,
    trend_keyword STRING,
    google_trend_score INT64,
    trend_signal_status STRING, -- 'Full Data', 'No Trend', 'Unmapped'
    fx_rate FLOAT64,
    inserted_at TIMESTAMP
)
PARTITION BY date
CLUSTER BY product_id, trend_keyword
OPTIONS(
    description="Bảng phân tích tổng hợp Daily: Product + Trends + FX. 1 dòng = 1 sản phẩm/ngày."
);