-- Create a Database

Create Database ECommerce ;

Use ECommerce;

-- Create tables 
Create table click_stream 
( session_id varchar(250),
  event_name varchar(250) ,
  event_time DATETIME2(0),
  event_id varchar (250),
  traffic_source varchar(250) ,
  event_metadata varchar(max) )

  Create table customer 
  (customer_id varchar (250) Primary Key,
  first_name varchar (250),
  last_name varchar (250),
  username varchar (250),
  email varchar (250),
  gender varchar (250),
  birthdate date ,
  device_type varchar(250),
  device_id varchar (250),
  device_version varchar (250),
  home_location_lat float,
  home_location_long float,
  home_location varchar(250),
  home_country varchar(250),
  first_join_date date )


Create table product
(id varchar (250),
 gender varchar (250),
 masterCategory varchar (250),
 subCategory varchar (250),
 articleType varchar (250),
 baseColour varchar (250),
 season varchar (250),
 year int ,
 usage varchar (250))


 Create table transactions 
 (created_at DATETIME2(0) ,
 Customer_id varchar(250),
 booking_id varchar (250),
 session_id varchar (250),
 product_metadata varchar(max),
 payment_method varchar (250),
 payment_status varchar (250),
 promo_amount int ,
 promo_code varchar (250),
 shipment_fee int,
 shipment_date_limit varchar (250),
 shipment_location_lat float,
 shipment_location_long float,
 total_amount float )

 -- Upload CSV files into respective tables

 BULK INSERT click_stream
FROM 'D:\Projects\SQL\click_stream.csv\click_stream.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=2,
		FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
)

 BULK INSERT customer
FROM 'D:\Projects\SQL\customer.csv\customer.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=2,
		FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
)

 BULK INSERT product
FROM 'D:\Projects\SQL\product.csv\product.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=2,
		FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
)

 BULK INSERT transactions
FROM 'D:\Projects\SQL\transactions.csv\transactions.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=2,
		FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
)

-- Session-level behavior and funnel analysis using event logs

-- Clean and Prepare the data

-- Checking for duplicate events 
Select event_id, Count(*)
from click_stream
where event_metadata is not NUll 
group by event_id
having count(*) > 1

-- Check for sessions having 1 event 
Select session_id, Count(*)
from click_stream
where event_metadata is not NUll 
group by session_id
having count(*) =1


-- Define Funnel Stages
Select *, 
(case when event_name = 'HOMEPAGE' Then 1
     when event_name = 'SEARCH' Then 2
	 when event_name = 'SCROLL' Then 3
	 when event_name = 'ITEM_DETAIL' Then 4
	 when event_name = 'CLICK' Then 5
	 when event_name = 'ADD_TO_CART' Then 6
	 when event_name = 'PROMO_PAGE' Then 7
	 when event_name = 'ADD_PROMO' Then 8
	 when event_name = 'BOOKING' Then 9
	 end ) as 'event_sequence'
from click_stream
order by session_id, event_sequence;


-- Create Session-Level Funnel Flags

Create View session_funnel as 
SELECT
  session_id,
  Max(CASE WHEN event_name = 'HOMEPAGE' or event_name = 'SEARCH' or event_name = 'CLICK' or event_name = 'ITEM_DETAIL' THEN 1 ELSE 0 END) AS viewed_page,
  Max(CASE WHEN event_name = 'ADD_TO_CART' THEN 1 ELSE 0 END) AS added_to_cart,
  Max(CASE WHEN event_name = 'BOOKING' THEN 1 ELSE 0 END) AS purchased
FROM click_stream
GROUP BY session_id

-- Funnel Conversion 

SELECT
  COUNT(*) AS total_sessions,
  SUM(viewed_page) AS page_view_sessions,
  SUM(added_to_cart) AS cart_sessions,
  SUM(purchased) AS purchase_sessions,
  ROUND(SUM(purchased) * 100.0 / COUNT(*), 2) AS conversion_rate
FROM session_funnel;

-- Funnel Drop-Off Analysis

SELECT 'View to Cart' AS stage, SUM(viewed_page), SUM(added_to_cart), SUM(viewed_page) - SUM(added_to_cart) AS dropoffs
FROM session_funnel

UNION ALL

SELECT 'Cart to Purchase', SUM(added_to_cart), SUM(purchased),SUM(added_to_cart) - SUM(purchased)
FROM session_funnel;



-- Segment Analysis using traffic_source
SELECT
  c.traffic_source,
  COUNT(DISTINCT sf.session_id) AS sessions,
  SUM(sf.purchased) AS purchases,
  ROUND(SUM(sf.purchased) * 100.0 / COUNT(DISTINCT sf.session_id), 2) AS conversion_rate
FROM session_funnel sf
JOIN click_stream as c
  ON sf.session_id = c.session_id
GROUP BY c.traffic_source;


-- Sessions that converted to actual Transaction (Conversion Quality)

SELECT sf.session_id,t.booking_id,t.total_amount,t.payment_status
FROM session_funnel as sf
LEFT JOIN transactions as t
ON sf.session_id = t.session_id;

-- Revenue Analysis by Traffic Source
SELECT
  c.traffic_source,
  COUNT(DISTINCT t.booking_id) AS orders,
  AVG(t.total_amount) AS avg_revenue
FROM click_stream c
JOIN transactions t
  ON c.session_id = t.session_id
GROUP BY c.traffic_source;


