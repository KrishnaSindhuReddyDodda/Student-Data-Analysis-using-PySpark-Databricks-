# Student-Data-Analysis-using-PySpark-Databricks

For this mini-project, I used student.csv as an input file that has following columns (Age, Gender, Name, Course, Roll, Marks, Email)
+ Initially I Created a cluster on the Databrick platform, in that I created a notebook named First Spark code.
+ Now I uploaded my input file student.csv into data source in the following sequence 
 (Data => default => create table => drop files to upload => now copy the path of this file from     the bottom => Go to DBFS => File store => tables => select the file_name which you have created).
+ After creating my Notebook, I read this input file in the RDD(Resilient Distributed Datasets)
+ Now I tried to perform the following analytics on this data
**======Show the number of students in the file
======Show the total marks achieved by Female and Male students
======Show the total number of students that have passed and failed (50+ marks are required to pass       the course)
======Show the total number of students enrolled per course
======Show the total marks that students have achieved per course
======Show the average marks that students have achieved per course
======Show the minimum and maximum marks achieved per course
======Show the average age of male and female students
**
