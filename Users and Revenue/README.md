**1. Data Input**
- Flood-It! is a puzzle game available on both Android and iOS platforms. The app uses the standard Google Analytics gaming app implementation through Firebase. The Flood-It dataset, available through the firebase-public-project BigQuery project, contains a sample of obfuscated BigQuery event export data for 114 days.
  
**2. Data Output**
- User metrics statistics: number of active users, new users, and removed users, categorized by dimensions such as date, country, and app version.
- Revenue metrics statistics: total revenue and breakdown of revenue by sources, categorized by dimensions such as date, country, and app version.
  
**3. Data Processing**
Step 1: Create a raw table with the following details:
+ Combine data from 114 tables corresponding to 114 days by using a wildcard table.
+ Filter the required fields such as event_date, event_timestamp, event_name, etc.
+ Use the UNNEST function to convert an ARRAY into a set of rows.
+ Transform data types to appropriate formats.
Step 2: Create a transformed table with the following details:
+ Include dimensions such as event_date, user_id, country, and app_version.
+ Add indicators such as is_remove_users, is_new_user and calculated metrics such as revenue, ad_revenue, and iap_revenue.
Step 3: Create a final summary table with 3 dimensions and 5 metrics:
+ Dimensions: event_date, country, and app_version.
+ Metrics: Number of active users, number of users who removed the app, number of new users, total revenue, ad revenue, and IAP revenue.
