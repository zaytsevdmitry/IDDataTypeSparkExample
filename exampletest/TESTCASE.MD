# Agreements
test suit files must be placed in the same directory
IDDataTypeSparkExample.jar should be placed in the directory above 
```
 ../IDDataTypeSparkExample.jar \
```

# Files
* testcase.bash - runs the specified test case 
* testcase_coumn_all.csv - case with all columns  ( bigint,decimal, string-bigint,string-uid,timestamp )
* testcase_coumn_single.csv - all columns will be deleted except id

# Example
make the file executable
```
> chmod -x ./testcase.bash
```
```
> ./testcase.bash  testcase_coumn_all.csv
```
or 
```
> ./testcase.bash  testcase_coumn_single.csv
```