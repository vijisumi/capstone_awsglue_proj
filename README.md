Project Details:

Crawler will scan the customer and geo location data and put update the glue catalog.
Phase 1 - Glue Job will run. It will read the customer data set and cleanse the data
Phase 2 - Glue Job will triggered once the Phase 1 succeeded. This will read the cleansed data and do joins/transformations as per requirement and put it in s3 bucket.
Set up workflow to trigger this sequence and execution completed successfully.
