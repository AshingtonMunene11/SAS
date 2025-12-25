DATA mydata;
SET data/employees.csv;
WHERE age > 30;
KEEP name, age, income;
RENAME income=salary;
RUN;
