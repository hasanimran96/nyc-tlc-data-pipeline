Dear Hasan,

Thank you for the call we had today. As promised, I'm sending you an assignment to complete at home.
Conditions

The deadline of the task is one week upon receiving this task to submit your solution (July 29, eob). If you need more time, please let us know. You will find the task below. If you have any questions, please don't hesitate to ask my colleague Witold (in Cc).

Background information
The city of New York provides historical data of "The New York City Taxi and Limousine Commission" (TLC). Your colleagues from the data science team want to create various evaluations and predictions based on this data. Depending on their different use cases, they need the output data in a row-oriented and column-oriented format. So they approach to you and ask for your help. Your colleagues only rely on a frequent output of the datasets in these two formats, so you have a free choice of your specific technologies.

Your tasks
• Build a data pipeline that imports the TLC datasets for at least the last three years
• Enhance your pipeline in that way that it automatically imports future datasets
• Convert the input datasets to a column-oriented (e.g. Parquet) and a row-oriented format (e.g. Avro)
• Import or define a schema definition
• Structure the data in that way so that the data science team is able to perform their predictions:
  o The input data is spread over several files, including separate files for “Yellow” and “Green” taxis. Does it make sense to merge those input file into one?
  o You will notice that the input data contains “date and time” columns. Your colleagues want to evaluate data also on hour-level and day of week-level. Does that affect your output-structure in some way?

• To determine the correctness of your output datasets, your colleagues want you to write the following queries:
  o The average distance driven by yellow and green taxis per hour
  o Day of the week in 2019 and 2020 which has the lowest number of single rider trips
  o The top 3 of the busiest hours

Your deliverables
• Description and definition of the single steps of your pipeline (Source code of the pipeline, if present)
• Definition of your output datasets. The formats including the schema definition (data types, names, etc)
• Code of the queries (SQL or other query languages)
• Instructions on how to run and deploy your pipeline
• Document all necessary steps and decisions, in a understandable and reproducible manner

Bonus
• Your data scientists want to make future predictions based on weather conditions. How would you expand your pipeline to help your colleagues with this task?
• Another colleague approaches to you. He is an Excel guru and makes all kind of stuff using this tool forever. So he needs all the available taxi trip records in the XLSX format. Can you re-use your current pipeline? How does this output compares to your existing formats? Do you have performance concerns?

We will evaluate your solution based on:
• Correctness
• Maintainability (readable & reusable code, tests)
• Pipeline design (Conventions & best practices)

You can either create a private repository on a service like BitBucket or GitHub and share access with us, or upload your code e.g. to Google Drive and share the archive / folder with us.
Looking forward to receiving your solution!

Cheers,
Elisa
