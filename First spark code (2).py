# Databricks notebook source


# Show the number of students in the file

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("mini-project")
sc = SparkContext.getOrCreate(conf=conf)
rdd=sc.textFile('/FileStore/tables/student.csv')
headers=rdd.first()
total_students=rdd.filter(lambda x: x!=headers)
total_students.count()


# COMMAND ----------

# Show the total marks acheived by Female and Male students

rdd=sc.textFile('/FileStore/tables/student.csv')
headers=rdd.first()
rdd=rdd.filter(lambda x: x!=headers)
total_marks_achieved_by_male_female = rdd.map(lambda x : (x.split(",")[1], (int(x.split(",")[5]))))
total_marks_achieved_by_male_female.reduceByKey(lambda x,y : x+y).collect()

# COMMAND ----------

# Show the total number of students that have passed and failed (50+ marks are required to pass the course)

rdd=sc.textFile('/FileStore/tables/student.csv')
headers=rdd.first()
rdd3=rdd.filter(lambda x: x!=headers)
total_number_of_students_passed=rdd3.filter(lambda x: int(x.split(",")[5]) > 50).count()
total_number_of_students_failed=rdd3.filter(lambda x: int(x.split(",")[5]) <= 50).count()
print(total_number_of_students_passed   ,    total_number_of_students_failed)

# COMMAND ----------

# Show the total number of students enrolled per course

rdd=sc.textFile('/FileStore/tables/student.csv')
headers=rdd.first()
rdd=rdd.filter(lambda x: x!=headers)
total_students_enrolled_per_course=rdd.map(lambda x: (x.split(",")[3],1))
total_students_enrolled_per_course.reduceByKey(lambda x,y : x+y).collect()

# COMMAND ----------

# Show the total marks that students have achieved per course

rdd=sc.textFile('/FileStore/tables/student.csv')
headers=rdd.first()
rdd=rdd.filter(lambda x: x!=headers)
total_marks_per_course=rdd.map(lambda x: (x.split(",")[3],int(x.split(",")[5])))
total_marks_per_course.reduceByKey(lambda x,y : x+y).collect()

# COMMAND ----------

# Show the average marks that students have achieved per course

rdd=sc.textFile('/FileStore/tables/student.csv')
headers=rdd.first()
rdd=rdd.filter(lambda x: x!=headers)
rdd6=rdd.map(lambda x: (x.split(",")[3],(int(x.split(",")[5]),1)))
marks_per_course = rdd6.reduceByKey(lambda x,y : (x[0]+y[0] , x[1]+y[1]))
Average_marks_per_course = marks_per_course.map(lambda x : (x[0],x[1][0]/x[1][1]))
Average_marks_per_course.collect()

# COMMAND ----------

#Show the minimum marks achieved per course

rdd=sc.textFile('/FileStore/tables/student.csv')
headers=rdd.first()
rdd=rdd.filter(lambda x: x!=headers)
rdd7=rdd.map(lambda x: (x.split(",")[3],(int(x.split(",")[5]))))
min_mark_per_course = rdd7.reduceByKey(lambda x,y : x if x<y else y)
min_mark_per_course.collect()

# COMMAND ----------

# Show the maximum marks achieved per course

max_mark_per_course = rdd7.reduceByKey(lambda x,y : x if x>y else y)
max_mark_per_course.collect()

# COMMAND ----------

# Show the average age of male and female students

rdd=sc.textFile('/FileStore/tables/student.csv')
headers=rdd.first()
rdd=rdd.filter(lambda x: x!=headers)
rdd8=rdd.map(lambda x: (x.split(",")[1],(int(x.split(",")[0]),1)))
rdd8=rdd8.reduceByKey(lambda x,y: (x[0]+y[0] , x[1]+y[1]))
avg_age_of_genders = rdd8.mapValues(lambda x: (x[0]/x[1]))
avg_age_of_genders.collect()
