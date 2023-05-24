import getpass
import typing
import subprocess
import os.path
import datetime
import sys

import yt.wrapper

# 1. Create map node //home/tpcds on cluster (yt create map_node //home/tpcds).
# 2. Clone https://github.com/gregrahn/tpcds-kit and run make in tools directory.
#    Files dsdgen and tpcds.idx will appear.
#    Upload them into Cypress.
#    cat dsdgen | yt write-file //home/tpcds/dsdgen
#    yt set //home/tpcds/dsdgen/@executable "%true"
#    cat tpcds.idx | yt write-file //home/tpcds/tpcds.idx
# 3. Modify parameters below.
#    DIRECTORY -- directory to generate data in;
#    CLUSTER -- address of the cluster http proxies;
#    SCALE -- size (in gigabytes) of the generated data;
#    PARALLEL -- number of jobs to run, typically should be equal to SCALE;
#    MEDIUM -- medium to generate data on.
# 4. Run script.
DIRECTORY = "//home/tpcds/small"
CLUSTER = "cluster.http.proxies.address:1234"
SCALE = 10
PARALLEL = 10
MEDIUM = "default"

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

TABLES = {
    "store_sales": [
        {"name": "ss_sold_date_sk", "type": "int64"},
        {"name": "ss_sold_time_sk", "type": "int64"},
        {"name": "ss_item_sk", "type": "int64"},
        {"name": "ss_customer_sk", "type": "int64"},
        {"name": "ss_cdemo_sk", "type": "int64"},
        {"name": "ss_hdemo_sk", "type": "int64"},
        {"name": "ss_addr_sk", "type": "int64"},
        {"name": "ss_store_sk", "type": "int64"},
        {"name": "ss_promo_sk", "type": "int64"},
        {"name": "ss_ticket_number", "type": "int64"},
        {"name": "ss_quantity", "type": "int64"},
        {"name": "ss_wholesale_cost", "type": "float"},
        {"name": "ss_list_price", "type": "float"},
        {"name": "ss_sales_price", "type": "float"},
        {"name": "ss_ext_discount_amt", "type": "float"},
        {"name": "ss_ext_sales_price", "type": "float"},
        {"name": "ss_ext_wholesale_cost", "type": "float"},
        {"name": "ss_ext_list_price", "type": "float"},
        {"name": "ss_ext_tax", "type": "float"},
        {"name": "ss_coupon_amt", "type": "float"},
        {"name": "ss_net_paid", "type": "float"},
        {"name": "ss_net_paid_inc_tax", "type": "float"},
        {"name": "ss_net_profit", "type": "float"},
    ],
    "store_returns": [
        {"name": "sr_returned_date_sk", "type": "int64"},
        {"name": "sr_return_time_sk", "type": "int64"},
        {"name": "sr_item_sk", "type": "int64"},
        {"name": "sr_customer_sk", "type": "int64"},
        {"name": "sr_cdemo_sk", "type": "int64"},
        {"name": "sr_hdemo_sk", "type": "int64"},
        {"name": "sr_addr_sk", "type": "int64"},
        {"name": "sr_store_sk", "type": "int64"},
        {"name": "sr_reason_sk", "type": "int64"},
        {"name": "sr_ticket_number", "type": "int64"},
        {"name": "sr_return_quantity", "type": "int64"},
        {"name": "sr_return_amt", "type": "float"},
        {"name": "sr_return_tax", "type": "float"},
        {"name": "sr_return_amt_inc_tax", "type": "float"},
        {"name": "sr_fee", "type": "float"},
        {"name": "sr_return_ship_cost", "type": "float"},
        {"name": "sr_refunded_cash", "type": "float"},
        {"name": "sr_reversed_charge", "type": "float"},
        {"name": "sr_store_credit", "type": "float"},
        {"name": "sr_net_loss", "type": "float"},
    ],
    "catalog_sales": [
        {"name": "cs_sold_date_sk", "type": "int64"},
        {"name": "cs_sold_time_sk", "type": "int64"},
        {"name": "cs_ship_date_sk", "type": "int64"},
        {"name": "cs_bill_customer_sk", "type": "int64"},
        {"name": "cs_bill_cdemo_sk", "type": "int64"},
        {"name": "cs_bill_hdemo_sk", "type": "int64"},
        {"name": "cs_bill_addr_sk", "type": "int64"},
        {"name": "cs_ship_customer_sk", "type": "int64"},
        {"name": "cs_ship_cdemo_sk", "type": "int64"},
        {"name": "cs_ship_hdemo_sk", "type": "int64"},
        {"name": "cs_ship_addr_sk", "type": "int64"},
        {"name": "cs_call_center_sk", "type": "int64"},
        {"name": "cs_catalog_page_sk", "type": "int64"},
        {"name": "cs_ship_mode_sk", "type": "int64"},
        {"name": "cs_warehouse_sk", "type": "int64"},
        {"name": "cs_item_sk", "type": "int64"},
        {"name": "cs_promo_sk", "type": "int64"},
        {"name": "cs_order_number", "type": "int64"},
        {"name": "cs_quantity", "type": "int64"},
        {"name": "cs_wholesale_cost", "type": "float"},
        {"name": "cs_list_price", "type": "float"},
        {"name": "cs_sales_price", "type": "float"},
        {"name": "cs_ext_discount_amt", "type": "float"},
        {"name": "cs_ext_sales_price", "type": "float"},
        {"name": "cs_ext_wholesale_cost", "type": "float"},
        {"name": "cs_ext_list_price", "type": "float"},
        {"name": "cs_ext_tax", "type": "float"},
        {"name": "cs_coupon_amt", "type": "float"},
        {"name": "cs_ext_ship_cost", "type": "float"},
        {"name": "cs_net_paid", "type": "float"},
        {"name": "cs_net_paid_inc_tax", "type": "float"},
        {"name": "cs_net_paid_inc_ship", "type": "float"},
        {"name": "cs_net_paid_inc_ship_tax", "type": "float"},
        {"name": "cs_net_profit", "type": "float"},
    ],
    "catalog_returns": [
        {"name": "cr_returned_date_sk", "type": "int64"},
        {"name": "cr_returned_time_sk", "type": "int64"},
        {"name": "cr_item_sk", "type": "int64"},
        {"name": "cr_refunded_customer_sk", "type": "int64"},
        {"name": "cr_refunded_cdemo_sk", "type": "int64"},
        {"name": "cr_refunded_hdemo_sk", "type": "int64"},
        {"name": "cr_refunded_addr_sk", "type": "int64"},
        {"name": "cr_returning_customer_sk", "type": "int64"},
        {"name": "cr_returning_cdemo_sk", "type": "int64"},
        {"name": "cr_returning_hdemo_sk", "type": "int64"},
        {"name": "cr_returning_addr_sk", "type": "int64"},
        {"name": "cr_call_center_sk", "type": "int64"},
        {"name": "cr_catalog_page_sk", "type": "int64"},
        {"name": "cr_ship_mode_sk", "type": "int64"},
        {"name": "cr_warehouse_sk", "type": "int64"},
        {"name": "cr_reason_sk", "type": "int64"},
        {"name": "cr_order_number", "type": "int64"},
        {"name": "cr_return_quantity", "type": "int64"},
        {"name": "cr_return_amount", "type": "float"},
        {"name": "cr_return_tax", "type": "float"},
        {"name": "cr_return_amt_inc_tax", "type": "float"},
        {"name": "cr_fee", "type": "float"},
        {"name": "cr_return_ship_cost", "type": "float"},
        {"name": "cr_refunded_cash", "type": "float"},
        {"name": "cr_reversed_charge", "type": "float"},
        {"name": "cr_store_credit", "type": "float"},
        {"name": "cr_net_loss", "type": "float"},
    ],
    "web_sales": [
        {"name": "ws_sold_date_sk", "type": "int64"},
        {"name": "ws_sold_time_sk", "type": "int64"},
        {"name": "ws_ship_date_sk", "type": "int64"},
        {"name": "ws_item_sk", "type": "int64"},
        {"name": "ws_bill_customer_sk", "type": "int64"},
        {"name": "ws_bill_cdemo_sk", "type": "int64"},
        {"name": "ws_bill_hdemo_sk", "type": "int64"},
        {"name": "ws_bill_addr_sk", "type": "int64"},
        {"name": "ws_ship_customer_sk", "type": "int64"},
        {"name": "ws_ship_cdemo_sk", "type": "int64"},
        {"name": "ws_ship_hdemo_sk", "type": "int64"},
        {"name": "ws_ship_addr_sk", "type": "int64"},
        {"name": "ws_web_page_sk", "type": "int64"},
        {"name": "ws_web_site_sk", "type": "int64"},
        {"name": "ws_ship_mode_sk", "type": "int64"},
        {"name": "ws_warehouse_sk", "type": "int64"},
        {"name": "ws_promo_sk", "type": "int64"},
        {"name": "ws_order_number", "type": "int64"},
        {"name": "ws_quantity", "type": "int64"},
        {"name": "ws_wholesale_cost", "type": "float"},
        {"name": "ws_list_price", "type": "float"},
        {"name": "ws_sales_price", "type": "float"},
        {"name": "ws_ext_discount_amt", "type": "float"},
        {"name": "ws_ext_sales_price", "type": "float"},
        {"name": "ws_ext_wholesale_cost", "type": "float"},
        {"name": "ws_ext_list_price", "type": "float"},
        {"name": "ws_ext_tax", "type": "float"},
        {"name": "ws_coupon_amt", "type": "float"},
        {"name": "ws_ext_ship_cost", "type": "float"},
        {"name": "ws_net_paid", "type": "float"},
        {"name": "ws_net_paid_inc_tax", "type": "float"},
        {"name": "ws_net_paid_inc_ship", "type": "float"},
        {"name": "ws_net_paid_inc_ship_tax", "type": "float"},
        {"name": "ws_net_profit", "type": "float"},
    ],
    "web_returns": [
        {"name": "wr_returned_date_sk", "type": "int64"},
        {"name": "wr_returned_time_sk", "type": "int64"},
        {"name": "wr_item_sk", "type": "int64"},
        {"name": "wr_refunded_customer_sk", "type": "int64"},
        {"name": "wr_refunded_cdemo_sk", "type": "int64"},
        {"name": "wr_refunded_hdemo_sk", "type": "int64"},
        {"name": "wr_refunded_addr_sk", "type": "int64"},
        {"name": "wr_returning_customer_sk", "type": "int64"},
        {"name": "wr_returning_cdemo_sk", "type": "int64"},
        {"name": "wr_returning_hdemo_sk", "type": "int64"},
        {"name": "wr_returning_addr_sk", "type": "int64"},
        {"name": "wr_web_page_sk", "type": "int64"},
        {"name": "wr_reason_sk", "type": "int64"},
        {"name": "wr_order_number", "type": "int64"},
        {"name": "wr_return_quantity", "type": "int64"},
        {"name": "wr_return_amt", "type": "float"},
        {"name": "wr_return_tax", "type": "float"},
        {"name": "wr_return_amt_inc_tax", "type": "float"},
        {"name": "wr_fee", "type": "float"},
        {"name": "wr_return_ship_cost", "type": "float"},
        {"name": "wr_refunded_cash", "type": "float"},
        {"name": "wr_reversed_charge", "type": "float"},
        {"name": "wr_account_credit", "type": "float"},
        {"name": "wr_net_loss", "type": "float"},
    ],
    "inventory": [
        {"name": "inv_date_sk", "type": "int64"},
        {"name": "inv_item_sk", "type": "int64"},
        {"name": "inv_warehouse_sk", "type": "int64"},
        {"name": "inv_quantity_on_hand", "type": "int64"},
    ],
    "store": [
        {"name": "s_store_sk", "type": "int64"},
        {"name": "s_store_id", "type": "string"},
        {"name": "s_rec_start_date", "type": "date"},
        {"name": "s_rec_end_date", "type": "date"},
        {"name": "s_closed_date_sk", "type": "int64"},
        {"name": "s_store_name", "type": "string"},
        {"name": "s_number_employees", "type": "int64"},
        {"name": "s_floor_space", "type": "int64"},
        {"name": "s_hours", "type": "string"},
        {"name": "S_manager", "type": "string"},
        {"name": "S_market_id", "type": "int64"},
        {"name": "S_geography_class", "type": "string"},
        {"name": "S_market_desc", "type": "string"},
        {"name": "s_market_manager", "type": "string"},
        {"name": "s_division_id", "type": "int64"},
        {"name": "s_division_name", "type": "string"},
        {"name": "s_company_id", "type": "int64"},
        {"name": "s_company_name", "type": "string"},
        {"name": "s_street_number", "type": "string"},
        {"name": "s_street_name", "type": "string"},
        {"name": "s_street_type", "type": "string"},
        {"name": "s_suite_number", "type": "string"},
        {"name": "s_city", "type": "string"},
        {"name": "s_county", "type": "string"},
        {"name": "s_state", "type": "string"},
        {"name": "s_zip", "type": "string"},
        {"name": "s_country", "type": "string"},
        {"name": "s_gmt_offset", "type": "float"},
        {"name": "s_tax_percentage", "type": "float"},
    ],
    "call_center": [
        {"name": "cc_call_center_sk", "type": "int64"},
        {"name": "cc_call_center_id", "type": "string"},
        {"name": "cc_rec_start_date", "type": "date"},
        {"name": "cc_rec_end_date", "type": "date"},
        {"name": "cc_closed_date_sk", "type": "int64"},
        {"name": "cc_open_date_sk", "type": "int64"},
        {"name": "cc_name", "type": "string"},
        {"name": "cc_class", "type": "string"},
        {"name": "cc_employees", "type": "int64"},
        {"name": "cc_sq_ft", "type": "int64"},
        {"name": "cc_hours", "type": "string"},
        {"name": "cc_manager", "type": "string"},
        {"name": "cc_mkt_id", "type": "int64"},
        {"name": "cc_mkt_class", "type": "string"},
        {"name": "cc_mkt_desc", "type": "string"},
        {"name": "cc_market_manager", "type": "string"},
        {"name": "cc_division", "type": "int64"},
        {"name": "cc_division_name", "type": "string"},
        {"name": "cc_company", "type": "int64"},
        {"name": "cc_company_name", "type": "string"},
        {"name": "cc_street_number", "type": "string"},
        {"name": "cc_street_name", "type": "string"},
        {"name": "cc_street_type", "type": "string"},
        {"name": "cc_suite_number", "type": "string"},
        {"name": "cc_city", "type": "string"},
        {"name": "cc_county", "type": "string"},
        {"name": "cc_state", "type": "string"},
        {"name": "cc_zip", "type": "string"},
        {"name": "cc_country", "type": "string"},
        {"name": "cc_gmt_offset", "type": "float"},
        {"name": "cc_tax_percentage", "type": "float"},
    ],
    "catalog_page": [
        {"name": "cp_catalog_page_sk", "type": "int64"},
        {"name": "cp_catalog_page_id", "type": "string"},
        {"name": "cp_start_date_sk", "type": "int64"},
        {"name": "cp_end_date_sk", "type": "int64"},
        {"name": "cp_department", "type": "string"},
        {"name": "cp_catalog_number", "type": "int64"},
        {"name": "cp_catalog_page_number", "type": "int64"},
        {"name": "cp_description", "type": "string"},
        {"name": "cp_type", "type": "string"},
    ],
    "web_site": [
        {"name": "web_site_sk", "type": "int64"},
        {"name": "web_site_id", "type": "string"},
        {"name": "web_rec_start_date", "type": "date"},
        {"name": "web_rec_end_date", "type": "date"},
        {"name": "web_name", "type": "string"},
        {"name": "web_open_date_sk", "type": "int64"},
        {"name": "web_close_date_sk", "type": "int64"},
        {"name": "web_class", "type": "string"},
        {"name": "web_manager", "type": "string"},
        {"name": "web_mkt_id", "type": "int64"},
        {"name": "web_mkt_class", "type": "string"},
        {"name": "web_mkt_desc", "type": "string"},
        {"name": "web_market_manager", "type": "string"},
        {"name": "web_company_id", "type": "int64"},
        {"name": "web_company_name", "type": "string"},
        {"name": "web_street_number", "type": "string"},
        {"name": "web_street_name", "type": "string"},
        {"name": "web_street_type", "type": "string"},
        {"name": "web_suite_number", "type": "string"},
        {"name": "web_city", "type": "string"},
        {"name": "web_county", "type": "string"},
        {"name": "web_state", "type": "string"},
        {"name": "web_zip", "type": "string"},
        {"name": "web_country", "type": "string"},
        {"name": "web_gmt_offset", "type": "float"},
        {"name": "web_tax_percentage", "type": "float"},
    ],
    "web_page": [
        {"name": "wp_web_page_sk", "type": "int64"},
        {"name": "wp_web_page_id", "type": "string"},
        {"name": "wp_rec_start_date", "type": "date"},
        {"name": "wp_rec_end_date", "type": "date"},
        {"name": "wp_creation_date_sk", "type": "int64"},
        {"name": "wp_access_date_sk", "type": "int64"},
        {"name": "wp_autogen_flag", "type": "string"},
        {"name": "wp_customer_sk", "type": "int64"},
        {"name": "wp_url", "type": "string"},
        {"name": "wp_type", "type": "string"},
        {"name": "wp_char_count", "type": "int64"},
        {"name": "wp_link_count", "type": "int64"},
        {"name": "wp_image_count", "type": "int64"},
        {"name": "wp_max_ad_count", "type": "int64"},
    ],
    "warehouse": [
        {"name": "w_warehouse_sk", "type": "int64"},
        {"name": "w_warehouse_id", "type": "string"},
        {"name": "w_warehouse_name", "type": "string"},
        {"name": "w_warehouse_sq_ft", "type": "int64"},
        {"name": "w_street_number", "type": "string"},
        {"name": "w_street_name", "type": "string"},
        {"name": "w_street_type", "type": "string"},
        {"name": "w_suite_number", "type": "string"},
        {"name": "w_city", "type": "string"},
        {"name": "w_county", "type": "string"},
        {"name": "w_state", "type": "string"},
        {"name": "w_zip", "type": "string"},
        {"name": "w_country", "type": "string"},
        {"name": "w_gmt_offset", "type": "float"},
    ],
    "customer": [
        {"name": "c_customer_sk", "type": "int64"},
        {"name": "c_customer_id", "type": "string"},
        {"name": "c_current_cdemo_sk", "type": "int64"},
        {"name": "c_current_hdemo_sk", "type": "int64"},
        {"name": "c_current_addr_sk", "type": "int64"},
        {"name": "c_first_shipto_date_sk", "type": "int64"},
        {"name": "c_first_sales_date_sk", "type": "int64"},
        {"name": "c_salutation", "type": "string"},
        {"name": "c_first_name", "type": "string"},
        {"name": "c_last_name", "type": "string"},
        {"name": "c_preferred_cust_flag", "type": "string"},
        {"name": "c_birth_day", "type": "int64"},
        {"name": "c_birth_month", "type": "int64"},
        {"name": "c_birth_year", "type": "int64"},
        {"name": "c_birth_country", "type": "string"},
        {"name": "c_login", "type": "string"},
        {"name": "c_email_address", "type": "string"},
        {"name": "c_last_review_date_sk", "type": "int64"},
    ],
    "customer_address": [
        {"name": "ca_address_sk", "type": "int64"},
        {"name": "ca_address_id", "type": "string"},
        {"name": "ca_street_number", "type": "string"},
        {"name": "ca_street_name", "type": "string"},
        {"name": "ca_street_type", "type": "string"},
        {"name": "ca_suite_number", "type": "string"},
        {"name": "ca_city", "type": "string"},
        {"name": "ca_county", "type": "string"},
        {"name": "ca_state", "type": "string"},
        {"name": "ca_zip", "type": "string"},
        {"name": "ca_country", "type": "string"},
        {"name": "ca_gmt_offset", "type": "float"},
        {"name": "ca_location_type", "type": "string"},
    ],
    "customer_demographics": [
        {"name": "cd_demo_sk", "type": "int64"},
        {"name": "cd_gender", "type": "string"},
        {"name": "cd_marital_status", "type": "string"},
        {"name": "cd_education_status", "type": "string"},
        {"name": "cd_purchase_estimate", "type": "int64"},
        {"name": "cd_credit_rating", "type": "string"},
        {"name": "cd_dep_count", "type": "int64"},
        {"name": "cd_dep_employed_count", "type": "int64"},
        {"name": "cd_dep_college_count", "type": "int64"},
    ],
    "date_dim": [
        {"name": "d_date_sk", "type": "int64"},
        {"name": "d_date_id", "type": "string"},
        {"name": "d_date", "type": "date"},
        {"name": "d_month_seq", "type": "int64"},
        {"name": "d_week_seq", "type": "int64"},
        {"name": "d_quarter_seq", "type": "int64"},
        {"name": "d_year", "type": "int64"},
        {"name": "d_dow", "type": "int64"},
        {"name": "d_moy", "type": "int64"},
        {"name": "d_dom", "type": "int64"},
        {"name": "d_qoy", "type": "int64"},
        {"name": "d_fy_year", "type": "int64"},
        {"name": "d_fy_quarter_seq", "type": "int64"},
        {"name": "d_fy_week_seq", "type": "int64"},
        {"name": "d_day_name", "type": "string"},
        {"name": "d_quarter_name", "type": "string"},
        {"name": "d_holiday", "type": "string"},
        {"name": "d_weekend", "type": "string"},
        {"name": "d_following_holiday", "type": "string"},
        {"name": "d_first_dom", "type": "int64"},
        {"name": "d_last_dom", "type": "int64"},
        {"name": "d_same_day_ly", "type": "int64"},
        {"name": "d_same_day_lq", "type": "int64"},
        {"name": "d_current_day", "type": "string"},
        {"name": "d_current_week", "type": "string"},
        {"name": "d_current_month", "type": "string"},
        {"name": "d_current_quarter", "type": "string"},
        {"name": "d_current_year", "type": "string"},
    ],
    "household_demographics": [
        {"name": "hd_demo_sk", "type": "int64"},
        {"name": "hd_income_band_sk", "type": "int64"},
        {"name": "hd_buy_potential", "type": "string"},
        {"name": "hd_dep_count", "type": "int64"},
        {"name": "hd_vehicle_count", "type": "int64"},
    ],
    "item": [
        {"name": "i_item_sk", "type": "int64"},
        {"name": "i_item_id", "type": "string"},
        {"name": "i_rec_start_date", "type": "date"},
        {"name": "i_rec_end_date", "type": "date"},
        {"name": "i_item_desc", "type": "string"},
        {"name": "i_current_price", "type": "float"},
        {"name": "i_wholesale_cost", "type": "float"},
        {"name": "i_brand_id", "type": "int64"},
        {"name": "i_brand", "type": "string"},
        {"name": "i_class_id", "type": "int64"},
        {"name": "i_class", "type": "string"},
        {"name": "i_category_id", "type": "int64"},
        {"name": "i_category", "type": "string"},
        {"name": "i_manufact_id", "type": "int64"},
        {"name": "i_manufact", "type": "string"},
        {"name": "i_size", "type": "string"},
        {"name": "i_formulation", "type": "string"},
        {"name": "i_color", "type": "string"},
        {"name": "i_units", "type": "string"},
        {"name": "i_container", "type": "string"},
        {"name": "i_manager_id", "type": "int64"},
        {"name": "i_product_name", "type": "string"},
    ],
    "income_band": [
        {"name": "ib_income_band_sk", "type": "int64"},
        {"name": "ib_lower_bound", "type": "int64"},
        {"name": "ib_upper_bound", "type": "int64"},
    ],
    "promotion": [
        {"name": "p_promo_sk", "type": "int64"},
        {"name": "p_promo_id", "type": "string"},
        {"name": "p_start_date_sk", "type": "int64"},
        {"name": "p_end_date_sk", "type": "int64"},
        {"name": "p_item_sk", "type": "int64"},
        {"name": "p_cost", "type": "float"},
        {"name": "p_response_target", "type": "int64"},
        {"name": "p_promo_name", "type": "string"},
        {"name": "p_channel_dmail", "type": "string"},
        {"name": "p_channel_email", "type": "string"},
        {"name": "p_channel_catalog", "type": "string"},
        {"name": "p_channel_tv", "type": "string"},
        {"name": "p_channel_radio", "type": "string"},
        {"name": "p_channel_press", "type": "string"},
        {"name": "p_channel_event", "type": "string"},
        {"name": "p_channel_demo", "type": "string"},
        {"name": "p_channel_details", "type": "string"},
        {"name": "p_purpose", "type": "string"},
        {"name": "p_discount_active", "type": "string"},
    ],
    "reason": [
        {"name": "r_reason_sk", "type": "int64"},
        {"name": "r_reason_id", "type": "string"},
        {"name": "r_reason_desc", "type": "string"},
    ],
    "ship_mode": [
        {"name": "sm_ship_mode_sk", "type": "int64"},
        {"name": "sm_ship_mode_id", "type": "string"},
        {"name": "sm_type", "type": "string"},
        {"name": "sm_code", "type": "string"},
        {"name": "sm_carrier", "type": "string"},
        {"name": "sm_contract", "type": "string"},
    ],
    "time_dim": [
        {"name": "t_time_sk", "type": "int64"},
        {"name": "t_time_id", "type": "string"},
        {"name": "t_time", "type": "int64"},
        {"name": "t_hour", "type": "int64"},
        {"name": "t_minute", "type": "int64"},
        {"name": "t_second", "type": "int64"},
        {"name": "t_am_pm", "type": "string"},
        {"name": "t_shift", "type": "string"},
        {"name": "t_sub_shift", "type": "string"},
        {"name": "t_meal_time", "type": "string"},
    ],
}

SORTS = {
    "store_sales": ["ss_sold_date_sk"],
    "store_returns": ["sr_returned_date_sk"],
    "catalog_sales": ["cs_sold_date_sk"],
    "catalog_returns": ["cr_returned_date_sk"],
    "web_sales": ["ws_sold_date_sk"],
    "web_returns": ["wr_returned_date_sk"],
}

def make_row(csv, schema):
    row = {}
    values = csv[:-2].split('|')
    assert len(values) == len(schema)
    for index, value in enumerate(values):
        if not value:
            continue
        name, tp = schema[index]["name"], schema[index]["type"]
        if tp == "string":
            val = value
        elif tp == "int64":
            val = int(value)
        elif tp == "float":
            val = float(value)
        elif tp == "date":
            dt = datetime.datetime.strptime(value, "%Y-%m-%d")
            val = (dt - datetime.datetime(1970, 1, 1)).days
            if val < 0:
                val = (dt - datetime.datetime(1900, 1, 1)).days
        else:
            assert False
        row[name] = val
    return row

def execute(cmd):
    p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    p.communicate()

def mapper(input_row):
    mapper_index = input_row["index"]
    eprint("Running dsdgen")
    execute(f"./dsdgen -dir . -force Y -scale {SCALE} -parallel {PARALLEL} -child {mapper_index}")
    for index, table in enumerate(sorted(TABLES.keys())):
        yield yt.wrapper.create_table_switch(index)
        table_file = f"{table}_{mapper_index}_{PARALLEL}.dat"
        eprint("Looking for {}".format(table_file))
        if not os.path.isfile(table_file):
            continue
        eprint("Found table {}".format(table_file))
        with open(table_file, "r", encoding="ISO-8859-1") as f:
            for line in f.readlines():
                if not line:
                    continue
                yield make_row(line, TABLES[table])

if __name__ == "__main__":
    client = yt.wrapper.YtClient(proxy=CLUSTER)
    for table, schema in TABLES.items():
        path = f"{DIRECTORY}/{table}"
        client.create("table", path, attributes={"schema": schema, "primary_medium": MEDIUM, "optimize_for": "scan"}, force=True)

    generator_path = f"{DIRECTORY}/generator"
    client.create("table", generator_path, force=True)
    client.write_table(generator_path, [{"index": index + 1} for index in range(PARALLEL)])
    client.run_map(
        mapper,
        source_table=generator_path,
        destination_table=[f"{DIRECTORY}/{table}" for table in sorted(TABLES.keys())],
        spec={
            "job_count": PARALLEL,
            "mapper": {
                "file_paths": [
                    "//home/tpcds/dsdgen",
                    "//home/tpcds/tpcds.idx",
                ],
                "memory_limit": 5 * 1024**3, # Python one love.
                "tmpfs_size": 5 * 1024**3, # NB: Scale.
                "tmpfs_path": ".",
            },
        },
    )

    for table, columns in SORTS.items():
        client.run_sort(
            f"{DIRECTORY}/{table}",
            sort_by=columns)

