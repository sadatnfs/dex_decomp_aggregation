
rm(list=ls())
require(foreign)
require(readstata13)
require(data.table)
require(doParallel)


j <- ifelse(Sys.info()[1]=="Windows", "J:/", "/home/j")

## Bind 
filez <- c("dex__5.dta","dex__0.dta",
           "dex__80.dta","dex__45.dta","dex__50.dta",
           "dex__60.dta","dex__75.dta","dex__40.dta",
           "dex__55.dta","dex__65.dta","dex__70.dta",
           "dex__15.dta","dex__1.dta","dex__30.dta",
           "dex__25.dta","dex__10.dta","dex__35.dta",
           "dex__20.dta")


### GBD

cl <- makeCluster(26)

clusterEvalQ(cl, library("data.table"))
clusterEvalQ(cl, library("readstata13"))

clusterExport(cl, "filez")
clusterExport(cl, "j")


system.time(all_my_DEX <- parLapply(cl, filez, function(x) {
  
  ## Bring in data
  
  tmp <- data.table(read.dta13(paste0(j,"/temp/sadatnfs/dex_decomp_chunk/",x)) )
  
}
))


all_my_binds <- rbindlist(all_my_DEX)

stopCluster(cl)

rm(all_my_DEX)



system.time(write.dta(all_my_binds, file = paste0(j,"/temp/sadatnfs/dex_decomp_chunk/dex_all.dta")))





#### GBD

gbd_filez <- paste0("gbd_",c(0:9), ".dta")

cl <- makeCluster(20)

clusterEvalQ(cl, library("data.table"))
clusterEvalQ(cl, library("readstata13"))

clusterExport(cl, "gbd_filez")
clusterExport(cl, "j")


system.time(all_my_GBD <- parLapply(cl, gbd_filez, function(x) {
  
  ## Bring in data
  
  tmp <- data.table(read.dta13(paste0(j,"/temp/sadatnfs/dex_decomp_chunk/",x)) )
  
}
))


system.time(all_my_geebz <- rbindlist(all_my_GBD))

stopCluster(cl)

rm(all_my_GBD)



system.time(write.dta(all_my_geebz, file = paste0(j,"/temp/sadatnfs/dex_decomp_chunk/gbd_all.dta")))




#### Bring in the data if not working from memory

## all_my_geebz <- data.table(read.dta13(paste0(j,"/temp/sadatnfs/dex_decomp_chunk/gbd_all.dta")))

## all_my_binds <- data.table(read.dta13(paste0(j,"/temp/sadatnfs/dex_decomp_chunk/dex_all.dta")))


### Left Join GBD on to DEX

system.time(dex_gbd <- merge(all_my_binds , all_my_geebz, by=c( "year" , "age",  "sex",  "acause",  "rank"), all.x=T))

fwrite(dex_gbd, paste0(j,"/temp/sadatnfs/dex_gbd_merged_3.csv"))



