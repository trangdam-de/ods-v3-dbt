set search_path = doisoatvnpost;
set search_path = datamart_dvbc;

DO $$
DECLARE
    updated_day INT;
    old_day INT;
BEGIN
    -- Lấy giá trị status_date gần nhấn trên bảng source
    SELECT MAX(status_date)
    INTO updated_day
    FROM pns.item_delivery_detail;

    -- Lấy giá trị ngày mới nhất trên bảng đích
    SELECT MAX(delivery_date)
    INTO old_day
    FROM datamart_dvbc.item_delivery_general_pos_day;

    IF old_day IS NULL OR updated_day > old_day THEN
        -- Gọi procedure sp_item_general_pos_day với giá trị nguyên bản của @to_day
        CALL sp_item_general_pos_day(updated_day, updated_day);

        -- Cắt bỏ 2 chữ số cuối và gọi procedure sp_item_general_pos_month
        updated_day := updated_day / 100; -- Chia 100 để lấy phần trước (VD: 20240919 -> 202409)
        CALL sp_item_general_pos_month(updated_day);
    END IF;

END $$;
