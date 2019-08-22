# Linear Regression : Static & Streaming

Apply linear regression on a static csv folder
then by looking at a folder that keep being updated.

# Build Jar 
````
sbt assembly
````

# Submit Batch
````
spark-submit --class SparkBatch target/scala-2.11/food-growth-assembly-1.0.jar
````

# UDAF
Spark custom aggregation function to compute slopes of each growth line. Extend the Spark class UserDefinedAggregateFunction.
Check Growth.scala

# Results
## Static
FoodGrowth.scala is the static implementation of computation of growth.

Method computeAndOrderGrowth return a dataset with the largest to the smallest growth ordered.

Output :

|                area|                item| growth|
|--------------------|--------------------|-------|
|     China, mainland|                Beer|[0.148]|
|     China, mainland| Oranges, Mandarines|[0.108]|
|     China, mainland| Apples and products|  [0.1]|
|     China, mainland|     Molluscs, Other|[0.096]|
|     China, mainland| Alcoholic Beverages|[0.096]|
|     China, mainland|Fruits - Excludin...|[0.086]|
|     China, mainland|Cassava and products|[0.085]|
|     China, mainland|     Freshwater Fish|[0.085]|
|     China, mainland|Aquatic Products,...|[0.081]|
|     China, mainland|      Aquatic Plants| [0.08]|
|     China, mainland|        Poultry Meat|[0.076]|
|     China, mainland|       Fruits, Other|[0.074]|
|     China, mainland|                Meat| [0.07]|
|              Brazil|                Beer|[0.068]|
|               India|Tomatoes and prod...|[0.068]|
|     China, mainland|       Fish, Seafood|[0.062]|
|Iran (Islamic Rep...|Fruits - Excludin...|[0.061]|
|     China, mainland|             Pigmeat|[0.058]|
|              Brazil| Alcoholic Beverages|[0.057]|
|               India|Potatoes and prod...|[0.056]|

only showing top 20 rows

## Streaming

FoodGrowthStreaming.scala is the static implementation of the computation of growth.

Method computeAndOrderGrowthStreaming return a dataset with the largest to the smallest growth ordered. Growth will be calculated in any file you add to the folder.

Output:
-------------------------------------------
Batch: 0
-------------------------------------------

|item                   |area                      |growth |
|-----------------------|--------------------------|-------|
|Beer                   |China, mainland           |[0.148]|
|Oranges, Mandarines    |China, mainland           |[0.108]|
|Apples and products    |China, mainland           |[0.1]  |
|Molluscs, Other        |China, mainland           |[0.096]|
|Alcoholic Beverages    |China, mainland           |[0.096]|
|Fruits - Excluding Wine|China, mainland           |[0.086]|
|Freshwater Fish        |China, mainland           |[0.085]|
|Cassava and products   |China, mainland           |[0.085]|
|Aquatic Products, Other|China, mainland           |[0.081]|
|Aquatic Plants         |China, mainland           |[0.08] |
|Poultry Meat           |China, mainland           |[0.076]|
|Fruits, Other          |China, mainland           |[0.074]|
|Meat                   |China, mainland           |[0.07] |
|Tomatoes and products  |India                     |[0.068]|
|Beer                   |Brazil                    |[0.068]|
|Fish, Seafood          |China, mainland           |[0.062]|
|Fruits - Excluding Wine|Iran (Islamic Republic of)|[0.061]|
|Pigmeat                |China, mainland           |[0.058]|
|Alcoholic Beverages    |Brazil                    |[0.057]|
|Potatoes and products  |India                     |[0.056]|

only showing top 20 rows

