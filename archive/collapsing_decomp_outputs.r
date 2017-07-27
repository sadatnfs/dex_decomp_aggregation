
#### Notebook code in R script

## Bring in library
rm(list=ls())

require(foreign)
require(readstata13)
require(abind)
require(data.table)
require(reshape2)
require(parallel)

## Bring in output data (from mean version)
mean_output <- data.table(read.dta13("/home/j/Project/IRH/DEX/USA/_explore/decomp/data/compiled_results.dta"))

## Drop the first variable
mean_output <- mean_output[, acause_age_sex_function:= NULL]

## Convert to full on long first
mean_output_long <- melt(mean_output, id.vars = c("function", "acause", "sex", "age"), value.name = "data")

## Rename the column function
colnames(mean_output_long) <- c("fn", "acause", "sex", "age", "variable", "data")
head(mean_output_long)

## Play around with data as "draws"
mean_output_long_1 <- mean_output_long[, list(fn, acause, sex, age, variable, draws="draw_1", data=data)]
mean_output_long_2 <- mean_output_long[, list(fn, acause, sex, age, variable, draws="draw_2", data=data)]
mean_output_long_3 <- mean_output_long[, list(fn, acause, sex, age, variable, draws="draw_3", data=data)]
mean_output_long_4 <- mean_output_long[, list(fn, acause, sex, age, variable, draws="draw_4", data=data)]

## row bind by grep
mean_output_long_draws <- do.call(rbind, lapply(ls(pattern = "mean_output_long_"), get))

draws <- unique(mean_output_long_draws[, draws])

## Convert to array, parallelizing over draws

#### Time it
system.time(mean_output_array_parallel <- mclapply(draws, 
                                                   function(x) acast(data = mean_output_long_draws[draws==paste0(x)],
                           formula =  fn ~ acause ~ sex ~ age ~ variable ~ draws, value.var="data"),
    mc.cores=8, mc.preschedule=F))
    
    
## Array bind across the draw dimension (draw dim = 6)
mean_output_array_parallel_bind<-abind(mean_output_array_parallel, along = 6)
                                                       
# Check for discrepancy (there's be NAs because we don't have a perfectly rectangular dataset)
str(mean_output_array_parallel_bind)    

## We can collapse in any dimension we want 
## basically, apply the sum over the dimensions we want to keep
## And remove the dimension we want summed over

## e.g. c(2,3,4,5,6) means sum over the function dimension only and keep the rest intact

## Let's sum up all the functions for e.g.
mean_over_fn <- apply(mean_output_array_parallel_bind, c(2,3,4,5,6),  mean, na.rm=T)

# Orig for debugging purposes
# mean_output_array <- acast(data = mean_output_long,
#                      formula =  fn ~ acause ~ sex ~ age ~ variable , value.var="data")
# str(mean_output_array)

# mean_test <- apply(mean_output_array, c(2,3,4,5), mean, na.rm=T)
