Project to develop a data pipeline.
- Load anime data from a csv file using pandas
- Transform resulting dataframe to match database schema
- Input data into MySQL database

Pipeline queries the database and returns graphs for the following
- Bar chart of each genre and its average rating
- Scatterplot of each year and its average rating. Plots a line of best fit.

To run the pipeline
- Run mal_schema.sql in a MySQL instance to create necessary database and user.
- Input 'python project_pipeline.py' into terminal. (Ensure necessary libraries are installed)
- Resulting graphs present in the 'out' folder.
