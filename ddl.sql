-- Create Database
CREATE TABLE table_m3(
	"CustomerID" INT,
	"Churn" INT,
	"Tenure" FLOAT,
	"PreferredLoginDevice" VARCHAR,
	"CityTier" INT,
	"WarehouseToHome" FLOAT,
	"PreferredPaymentMode" VARCHAR,
	"Gender" VARCHAR,
	"HourSpendOnApp" FLOAT,
	"NumberOfDeviceRegistered" INT,
	"PreferedOrderCat" VARCHAR,
	"SatisfactionScore" INT,
	"MaritalStatus" VARCHAR,
	"NumberOfAddress" INT,
	"Complain" INT,
	"OrderAmountHikeFromlastYear" FLOAT,
	"CouponUsed" FLOAT,
	"OrderCount" FLOAT,
	"DaySinceLastOrder" FLOAT,
	"CashbackAmount" INT
);

-- Copy data from CSV
COPY table_m3
FROM 'C:\tmp\P2M3_erlangga_jayadipraja_data_raw.csv'
DELIMITER ','
CSV HEADER;