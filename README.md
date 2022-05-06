# Student-Data-Analysis-using-PySpark-Databricks

For this mini-project, I used student.csv as an input file that has following columns (Age, Gender, Name, Course, Roll, Marks, Email)
1) Initially I Created a cluster on the Databrick platform, in that I created a notebook named First Spark code.
2) Now I uploaded my input file student.csv into data source in the following sequence 
   (Data => default => create table => drop files to upload => now copy the path of this file from the bottom => Go to DBFS => File store => tables => select the      file_name which you have created).
3) After creating my Notebook, I read this input file in the RDD(Resilient Distributed Datasets)
4) Now I tried to perform the following analytics on this data
5)    a)Show the number of students in the file
6)    b) Show the total marks achieved by Female and Male students
7)    c) Show the total number of students that have passed and failed (50+ marks are required to pass the course)
8)    d) Show the total number of students enrolled per course
9)    e) Show the total marks that students have achieved per course
10)   f) Show the average marks that students have achieved per course
11)   g) Show the minimum and maximum marks achieved per course
12)   h) Show the average age of male and female students

