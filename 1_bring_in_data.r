

rm(list=ls())
require(doParallel)

require(foreign)
require(readstata13)
# require(abind)
# require(rlist)
require(data.table)

require(parallel)
# require(feather)


## Create metadata for one

if(Sys.info()[1]=="Windows") {
  loc <- "H:/"
} else {
  loc <- "/homes/sadatnfs"
}

x = 1
d1<-fread(paste0(loc,"/dex_comps/compiled_results_",x,".dta"))
d1 <- d1[, draw:= x]

d2 <- melt(d1, id.vars = c("function", "acause", "sex", "age", "draw"), value.name = "data")
colnames(d2) <- c("fn", "acause", "sex", "age", "draw", "variable", "data")
# head(d2)

## Encode to numerics
varnames <- unique(d2[, .(fn, acause, variable)])
varnames <-varnames[, fn_num := as.integer(as.factor(fn))]
varnames <-varnames[, ac_num := as.integer(as.factor(acause))]
varnames <-varnames[, var_num := as.integer(as.factor(variable))]
# head(varnames)

### A version with no variable name
varnames_2 <- unique(varnames[, .(fn, fn_num, acause, ac_num)])
rm(d1)
rm(d2)


### Testing var
N <- 1000


cl <- makeCluster(26)

clusterEvalQ(cl, library("data.table"))
clusterEvalQ(cl, library("readstata13"))
clusterExport(cl, "varnames_2")
clusterExport(cl, "varnames")
clusterExport(cl, "loc")





# system.time(wide_on_draws <- parLapply(cl, c(0:N), function(x) {
#   
#   ## Bring in data
#   # tmp<- data.table(read.dta13(paste0(loc,"/dex_comps/compiled_results_", x, ".dta")))
#   tmp <- fread(paste0(loc,"/dex_comps/compiled_results_", x, ".dta"))
#   
#   ## Int to save space
#   tmp <- tmp[, age:= as.integer(age)]
#   tmp <- tmp[, sex:= as.integer(sex)]
#   
#   ## Rename out function because function
#   colnames(tmp)[4] <- "fn"
#   
#   ## Bring draw up front
#   #     setcolorder(tmp, c("draw", colnames(tmp)[1:dim(tmp)[2]-1]))
#   
#   
#   ## Convert to matrix
#   #     tmp <- as.matrix(tmp)
#   
#   ## Melt
#   tmp <- melt(tmp, id.vars = c("fn", "acause", "age", "sex"), value.name = paste0("d_",x), variable.name = "variable")
#   
#   ## Merge in metadata
#   tmp <- merge(varnames, tmp , by=c("fn", "acause", "variable"), all.y=T)
#   
#   
#   #     ## Drop crap
#   tmp <- tmp[, fn:=NULL]
#   tmp <- tmp[, acause:=NULL]
#   tmp <- tmp[, variable:=NULL]
#   
#   if(x %% 100 == 0 & x>0) {
#     print(paste0("Draw: ", x, ", ", Sys.time()))
#   }
#   return(tmp)
#   
#   ## Convert to an array
#   #     reshape2::acast(tmp, fn_num ~ ac_num ~ age ~ sex ~ var_num ~ draw , value.var="data")
#   
# }))

# stopCluster(cl)





#### Long on draws


# cl <- makeCluster(30)
# 
# clusterEvalQ(cl, library("data.table"))
# clusterEvalQ(cl, library("readstata13"))
# clusterExport(cl, "varnames_2")
# clusterExport(cl, "varnames")
# clusterExport(cl, "loc")

system.time(long_on_all <- parLapply(cl, c(1:N), function(x) {
  
  ## Bring in data
  # tmp<- data.table(read.dta13(paste0(loc,"/dex_comps/compiled_results_", x, ".dta")))
  tmp <- fread(paste0(loc,"/dex_comps/compiled_results_", x, ".dta")) 
  
  
  ## Int to save space
  tmp <- tmp[, age:= as.integer(age)]
  tmp <- tmp[, sex:= as.integer(sex)]
  
  ## Rename out function because function
  colnames(tmp)[4] <- "fn"
  
  ## Bring draw up front
  #     setcolorder(tmp, c("draw", colnames(tmp)[1:dim(tmp)[2]-1]))
  
  
  ## Convert to matrix
  #     tmp <- as.matrix(tmp)
  
  ## Melt
  tmp <- melt(tmp, id.vars = c("fn", "acause", "age", "sex"), value.name = paste0("data"), variable.name = "variable")
  
  ## Merge in metadata
  # tmp <- merge(varnames, tmp , by=c("fn", "acause", "variable"), all.y=T)
  
  
  #     ## Drop crap
  # tmp <- tmp[, fn:=NULL]
  # tmp <- tmp[, acause:=NULL]
  # tmp <- tmp[, variable:=NULL]
  
  if(x %% 100 == 0 & x>0) {
    print(paste0("Draw: ", x, ", ", Sys.time()))
  }
  return(tmp[,draw:=x])
  
  ## Convert to an array
  #     reshape2::acast(tmp, fn_num ~ ac_num ~ age ~ sex ~ var_num ~ draw , value.var="data")
  
}))

stopCluster(cl)



## Row bind (2)
# system.time(long_binded <- do.call(rbind, long_on_all))
system.time(long_binded <- rbindlist(long_on_all))
rm(long_on_all)


## Merge the wide version (1)
# system.time(wide_merged<-Reduce(function(x, y) merge(x, y, by=c("fn_num", "ac_num", "var_num", "age", "sex"), all=T), wide_on_draws))
# rm(wide_on_draws)


## dcast into wide by draws
system.time(wide_merged <- dcast(long_binded,  fn + acause + age + sex + variable ~ draw,
                                 value.var = "data"))
colnames(wide_merged) <- c("fn", "acause", "age", "sex", "variable", paste0("d_",1:1000))



####### NOW: Merge the metadata with the long binded

# system.time(long_binded_string <- merge(long_binded, varnames, by = c("fn_num", "ac_num", "var_num"), all.x=T))

## And with the wide 
# system.time(wide_merged_string <- merge(wide_merged, varnames, by = c("fn_num", "ac_num", "var_num"), all.x=T))



# long_binded_string <- long_binded_string[, fn_num:= NULL]
# long_binded_string <- long_binded_string[, ac_num:= NULL]
# long_binded_string <- long_binded_string[, var_num:= NULL]

# fwrite(wide_merged, file = paste0(loc, "/dex_comps/wide_merged_string.csv"))
# fwrite(long_binded, file = paste0(loc, "/dex_comps/long_binded_string.csv"))

fwrite(wide_merged, file = paste0(loc, "/dex_comps/wide_merged_string_clu.csv"))







