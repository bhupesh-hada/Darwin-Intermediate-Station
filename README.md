# Objective
The aim of the project was to design and productionize an interim datastore on cloud to process the point of sales data on a daily basis which aided the business team in making quick decisions and contributed significantly in reducing the data loss

# Background
One of the main problems faced by the client was of data loss. Every market has a specific vendor team which is responsible for managing and sharing the Point of Sales (Transaction) data from different retail stores in the market. The data shared by the vendor team was at a Barcode-date level. But the team shared data only for those invoices where 70% of the Barcodes are mapped with the ones present in the database. For example, if there are 10 list items  sold in a particular invoice and only 6 of the Barcodes have the mapping in the database then entire invoice (all the barcodes in the invoice) will get discarded. One of the main reason for the missing mapping of the barcode was that the product might be newly introduced in the market. However, this resulted in the discrepancy in the actual and calculated market share for the client.

# Solution
Setup a process flow on Microsoft Azure. The point of sales transaction data was uploaded in the sftp server by the market's vendor team. Created triggers to move those files from sftp server to azure blob storage. Further created data pipelines to load the data into Mysql Database by flagging the unmapped barcodes and perform data checks on the raw data. Subsequently performed data transformation on the filtered data to generate aggregate reports.  
