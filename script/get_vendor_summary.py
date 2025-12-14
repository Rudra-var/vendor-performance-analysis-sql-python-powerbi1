import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

# ----------------- logging config -----------------
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)

# ----------------- SQL: build vendor summary -----------------
def create_vendor_summary(conn):
    query = """
    WITH freight_summary AS (
        SELECT 
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    purchase_summary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.PurchasePrice,
            p.Description,
            pp.Volume,
            pp.Price AS ActualPrice,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars)  AS TotalPurchaseDollar
        FROM purchases AS p
        JOIN purchase_prices AS pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.PurchasePrice,
            p.Description,
            pp.Volume,
            pp.Price
    ),
    sales_summary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesPrice)    AS TotalSalesPrice,
            SUM(SalesDollars)  AS TotalSalesDollars,
            SUM(ExciseTax)     AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.Volume,
        ps.ActualPrice,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollar,
        ss.TotalSalesQuantity,
        ss.TotalSalesPrice,
        ss.TotalSalesDollars,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM purchase_summary AS ps
    LEFT JOIN sales_summary AS ss
        ON ps.VendorNumber = ss.VendorNo
       AND ps.Brand       = ss.Brand
    LEFT JOIN freight_summary AS fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollar DESC;
    """

    vendor_sales_summary = pd.read_sql_query(query, conn)
    return vendor_sales_summary

def clean_data(data):

# changing datatype into float---------------

     data['Volume']=data['Volume'].astype('float64')
    
#filling missing values with 0---------------
    
     data.fillna(0,inplace=True)  

#removing spaces from catogrical columns---------------

     vendor_sales_summary['Volume']=vendor_sales_summary['Description'].str.strip()
     vendor_sales_summary['Volume']=vendor_sales_summary['VendorName'].str.strip()

# creating new columns for better understanding---------------

     vendor_sales_summary['GrossProfit']=vendor_sales_summary['TotalSalesDollars']-vendor_sales_summary['TotalPurchaseDollar']
     vendor_sales_summary['ProfitMargin']=(vendor_sales_summary['GrossProfit']/vendor_sales_summary['TotalSalesDollars'])*100
     vendor_sales_summary['StockTurnOver']=vendor_sales_summary['TotalSalesQuantity']/vendor_sales_summary['totalPurchaseQuantity']
     vendor_sales_summary['SalesToPurchaseRatio']=vendor_sales_summary['TotalSalesDollars']/vendor_sales_summary['TotalPurchaseDollar']

     return data

if __name__=='__main__':

#creating database connection 
    conn=sqlite3.connect('inventory.db')

    logging.info('creating vendor summary Table.....')
    summary_data=create_vendor_summary(conn)
    logging.info(summary_data.head())

    logging.info('cleaning Data.....')
    clean_data=clean_data(summary_data)
    logging.info(clean_data.head())


    logging.info('ingesting data.....')
    ingest_db(clean_data,'vendor_sales_summary',conn)
    logging.info('completed')


