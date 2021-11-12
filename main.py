from datetime import datetime

from google.cloud import bigquery

from mysql import get

BQ_CLIENT = bigquery.Client()
DATASET = "WooCommerce"
TABLE = "OrderLines"

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

SCHEMA = [
    {"name": "order_id", "type": "INTEGER"},
    {"name": "post_date", "type": "TIMESTAMP"},
    {"name": "_billing_email", "type": "STRING"},
    {"name": "_billing_first_name", "type": "STRING"},
    {"name": "_billing_last_name", "type": "STRING"},
    {"name": "_billing_address_1", "type": "STRING"},
    {"name": "_billing_address_2", "type": "STRING"},
    {"name": "_billing_city", "type": "STRING"},
    {"name": "_billing_state", "type": "STRING"},
    {"name": "_billing_postcode", "type": "STRING"},
    {"name": "_billing_country", "type": "STRING"},
    {"name": "_shipping_first_name", "type": "STRING"},
    {"name": "_shipping_last_name", "type": "STRING"},
    {"name": "_shipping_address_1", "type": "STRING"},
    {"name": "_shipping_address_2", "type": "STRING"},
    {"name": "_shipping_city", "type": "STRING"},
    {"name": "_shipping_state", "type": "STRING"},
    {"name": "_shipping_postcode", "type": "STRING"},
    {"name": "_shipping_country", "type": "STRING"},
    {"name": "order_shipping", "type": "NUMERIC"},
    {"name": "order_total", "type": "NUMERIC"},
    {"name": "order_tax", "type": "NUMERIC"},
    {"name": "paid_date", "type": "TIMESTAMP"},
    {"name": "purchase_status", "type": "STRING"},
    {"name": "payment_method", "type": "STRING"},
    {"name": "_afl_wc_utm_utm_source", "type": "STRING"},
    {"name": "_afl_wc_utm_utm_medium", "type": "STRING"},
    {"name": "_afl_wc_utm_utm_campaign", "type": "STRING"},
    {"name": "order_item_id", "type": "INTEGER"},
    {"name": "order_item_name", "type": "STRING"},
    {"name": "order_item_type", "type": "STRING"},
    {"name": "_product_id", "type": "INTEGER"},
    {"name": "_variation_id", "type": "INTEGER"},
    {"name": "_qty", "type": "INTEGER"},
    {"name": "_line_total", "type": "NUMERIC"},
    {"name": "product_categories", "type": "STRING"},
    {"name": "_batched_at", "type": "TIMESTAMP"},
]


def query(start: str, end: str) -> str:
    return f"""
    WITH orders AS (
        SELECT
            p.ID AS id,
            p.post_date,
            MAX(IF(pm.meta_key = '_billing_email', pm.meta_value, NULL)) AS _billing_email,
            MAX(IF(pm.meta_key = '_billing_first_name',pm.meta_value,NULL)) AS _billing_first_name,
            MAX(IF(pm.meta_key = '_billing_last_name', pm.meta_value,NULL)) AS _billing_last_name,
            MAX(IF(pm.meta_key = '_billing_address_1',pm.meta_value,NULL)) AS _billing_address_1,
            MAX(IF(pm.meta_key = '_billing_address_2',pm.meta_value,NULL)) AS _billing_address_2,
            MAX(IF(pm.meta_key = '_billing_city',pm.meta_value,NULL)) AS _billing_city,
            MAX(IF(pm.meta_key = '_billing_state',pm.meta_value,NULL)) AS _billing_state,
            MAX(IF(pm.meta_key = '_billing_postcode',pm.meta_value,NULL)) AS _billing_postcode,
            MAX(IF(pm.meta_key = '_billing_country',pm.meta_value,NULL)) AS _billing_country,
            MAX(IF(pm.meta_key = '_shipping_first_name',pm.meta_value,NULL)) AS _shipping_first_name,
            MAX(IF(pm.meta_key = '_shipping_last_name',pm.meta_value,NULL)) AS _shipping_last_name,
            MAX(IF(pm.meta_key = '_shipping_address_1',pm.meta_value,NULL)) AS _shipping_address_1,
            MAX(IF(pm.meta_key = '_shipping_address_2',pm.meta_value,NULL)) AS _shipping_address_2,
            MAX(IF(pm.meta_key = '_shipping_city',pm.meta_value,NULL)) AS _shipping_city,
            MAX(IF(pm.meta_key = '_shipping_state',pm.meta_value,NULL)) AS _shipping_state,
            MAX(IF(pm.meta_key = '_shipping_postcode',pm.meta_value,NULL)) AS _shipping_postcode,
            MAX(IF(pm.meta_key = '_shipping_country',pm.meta_value,NULL)) AS _shipping_country,
            MAX(IF(pm.meta_key = '_order_shipping',pm.meta_value,NULL)) AS order_shipping,
            MAX(IF(pm.meta_key = '_order_total',pm.meta_value,NULL)) AS order_total,
            MAX(IF(pm.meta_key = '_order_tax',pm.meta_value,NULL)) AS order_tax,
            MAX(IF(pm.meta_key = '_paid_date',pm.meta_value,NULL)) AS paid_date,
            CASE p.post_status
		      WHEN 'wc-completed'  THEN 'Completed'
		      WHEN 'wc-cancelled'  THEN 'Cancelled'
		      WHEN 'wc-delivered'   THEN 'Delivered'
		    ELSE 'Unknown'
		    END AS 'purchase_status',
            MAX(IF(pm.meta_key = '_payment_method',pm.meta_value,NULL)) AS payment_method,
            MAX(IF(pm.meta_key = '_afl_wc_utm_utm_source',pm.meta_value,NULL)) AS _afl_wc_utm_utm_source,
            MAX(IF(pm.meta_key = '_afl_wc_utm_utm_medium',pm.meta_value,NULL)) AS _afl_wc_utm_utm_medium,
            MAX(IF(pm.meta_key = '_afl_wc_utm_utm_campaign',pm.meta_value,NULL)) AS _afl_wc_utm_utm_campaign
        FROM
            d23x_posts p
            JOIN d23x_postmeta pm ON p.ID = pm.post_id
        GROUP BY
            p.ID
        ),
        order_items AS (
            SELECT
                oi.order_id,
                oi.order_item_id,
                oi.order_item_name,
                oi.order_item_type,
                MAX(IF(oim.meta_key = '_product_id',oim.meta_value,NULL)) AS _product_id,
                MAX(IF(oim.meta_key = '_variation_id',oim.meta_value,NULL)) AS _variation_id,
                MAX(IF(oim.meta_key = '_qty',oim.meta_value,NULL)) AS _qty,
                MAX(IF(oim.meta_key = '_line_total',oim.meta_value,NULL)) AS _line_total
            FROM
                d23x_woocommerce_order_items oi
                JOIN d23x_woocommerce_order_itemmeta oim ON oi.order_item_id = oim.order_item_id
            GROUP BY 
                oi.order_id,
                oi.order_item_id,
                oi.order_item_name,
                oi.order_item_type
        ),
        items_categories AS (
        SELECT DISTINCT
            t.name,
            oim.meta_value
        FROM d23x_term_relationships tr
        INNER JOIN d23x_term_taxonomy tt ON tr.term_taxonomy_id = tt.term_taxonomy_id
        INNER JOIN d23x_terms t ON tt.term_id = t.term_id
        INNER JOIN d23x_woocommerce_order_itemmeta oim ON tr.object_id = oim.meta_value
        INNER JOIN d23x_woocommerce_order_items oi ON oim.order_item_id = oi.order_item_id
        INNER JOIN d23x_posts as o ON oi.order_id = o.ID
        WHERE tt.taxonomy = 'product_cat'
            AND oim.meta_key = '_product_id'
            AND o.post_type = 'shop_order'
        )
        SELECT
            o.*,
            oi.order_item_id,
            oi.order_item_name,
            oi.order_item_type,
            oi._product_id,
            oi._variation_id,
            oi._qty,
            oi._line_total,
            ic.name AS product_categories
        FROM
            orders o
            JOIN order_items oi ON o.id = oi.order_id
            LEFT JOIN items_categories ic ON oi._product_id = ic.meta_value
        WHERE
            o.post_date >= '{start}'
            AND o.post_date <= '{end}'
        """


def get_date_range(start: str, end: str) -> tuple[str, str]:
    if start and end:
        return start, end
    results = BQ_CLIENT.query(
        f"SELECT MAX(post_date) AS max_ FROM {DATASET}.{TABLE}"
    ).result()
    _start = [dict(row.items()) for row in results][0]["max_"].strftime(DATE_FORMAT)
    _end = datetime.utcnow().strftime(DATE_FORMAT)
    return _start, _end


def transform(rows: list) -> list:
    safe_float = lambda x: float(x) if x else None
    safe_int = lambda x: int(x) if x else None
    return [
        {
            "order_id": row["id"],
            "post_date": row["post_date"].isoformat(timespec="seconds"),
            "_billing_email": row["_billing_email"],
            "_billing_first_name": row["_billing_first_name"],
            "_billing_last_name": row["_billing_last_name"],
            "_billing_address_1": row["_billing_address_1"],
            "_billing_address_2": row["_billing_address_2"],
            "_billing_city": row["_billing_city"],
            "_billing_state": row["_billing_state"],
            "_billing_postcode": row["_billing_postcode"],
            "_billing_country": row["_billing_country"],
            "_shipping_first_name": row["_shipping_first_name"],
            "_shipping_last_name": row["_shipping_last_name"],
            "_shipping_address_1": row["_shipping_address_1"],
            "_shipping_address_2": row["_shipping_address_2"],
            "_shipping_city": row["_shipping_city"],
            "_shipping_state": row["_shipping_state"],
            "_shipping_postcode": row["_shipping_postcode"],
            "_shipping_country": row["_shipping_country"],
            "order_shipping": safe_float(row["order_shipping"]),
            "order_total": safe_float(row["order_total"]),
            "order_tax": safe_float(row["order_tax"]),
            "paid_date": row["paid_date"],
            "purchase_status": row["purchase_status"],
            "payment_method": row["payment_method"],
            "_afl_wc_utm_utm_source": row["_afl_wc_utm_utm_source"],
            "_afl_wc_utm_utm_medium": row["_afl_wc_utm_utm_medium"],
            "_afl_wc_utm_utm_campaign": row["_afl_wc_utm_utm_campaign"],
            "order_item_id": safe_int(row["order_item_id"]),
            "order_item_name": row["order_item_name"],
            "order_item_type": row["order_item_type"],
            "_product_id": safe_int(row["_product_id"]),
            "_variation_id": safe_int(row["_variation_id"]),
            "_qty": safe_int(row["_qty"]),
            "_line_total": safe_float(row["_line_total"]),
            "product_categories": row["product_categories"],
            "_batched_at": NOW.isoformat(timespec="seconds"),
        }
        for row in rows
    ]


def update():
    BQ_CLIENT.query(
        f"""
    CREATE OR REPLACE TABLE {DATASET}.{TABLE} AS
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER
            (PARTITION BY order_id, order_item_id
            ORDER BY _batched_at DESC) AS row_num
        FROM {DATASET}.{TABLE}
    ) WHERE row_num = 1
    """
    ).result()


def main(request) -> dict:
    request_json: dict = request.get_json()
    print(request_json)

    start, end = get_date_range(
        request_json.get("start"),
        request_json.get("end"),
    )
    rows = transform(
        get(
            request_json["auth"],
            query(start, end),
        )
    )
    output_rows: int = (
        BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}.{TABLE}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=SCHEMA,
            ),
        )
        .result()
        .output_rows
    )
    update()

    response = {
        "start": start,
        "end": end,
        "output_rows": output_rows,
    }
    print(response)
    return response
