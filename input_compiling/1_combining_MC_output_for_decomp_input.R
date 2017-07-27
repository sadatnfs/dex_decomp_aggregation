
rm(list=ls())
require(foreign)
require(readstata13)
require(data.table)
require(foreach)
require(doParallel)

#### GBD

## Bring in metadata for all ages
all_age_vector <- c("age_5.dta","age_0.dta",
	"age_80.dta","age_45.dta","age_50.dta",
	"age_60.dta","age_75.dta","age_40.dta",
	"age_55.dta","age_65.dta","age_70.dta",
	"age_15.dta","age_1.dta","age_30.dta",
	"age_25.dta","age_10.dta","age_35.dta",
	"age_20.dta")


system.time(all_ages <- foreach(i=all_age_vector) %do% {

meta <- data.table(read.dta13(paste0("/home/j/temp/sadatnfs/dex_age_file_name/",i)))

meta_files <- meta[, file_name]


cl <- makeCluster(26)

clusterEvalQ(cl, library("data.table"))
clusterEvalQ(cl, library("readstata13"))

clusterExport(cl, "meta_files")



system.time(long_on_all <- parLapply(cl, meta_files, function(x) {
  
  ## Bring in data

  tmp <- data.table(read.dta13(paste0("/ihme/dex/usa/10_resources/13_other/data/sorted/",x)) )
  
  }
  ))

system.time(el_longo <- rbindlist(long_on_all))


stopCluster(cl)


write.dta(el_longo, file = paste0("/homes/sadatnfs/crap/dex_" , i, ".dta"))
write.dta(el_longo, file = paste0("/ihme/dex/usa/10_resources/13_other/data/chunks/dex_" , i, ".dta"))



return(el_longo)

}
)


## Bind DEX chunks
filez <- c("dex__5.dta","dex__0.dta",
	"dex__80.dta","dex__45.dta","dex__50.dta",
	"dex__60.dta","dex__75.dta","dex__40.dta",
	"dex__55.dta","dex__65.dta","dex__70.dta",
	"dex__15.dta","dex__1.dta","dex__30.dta",
	"dex__25.dta","dex__10.dta","dex__35.dta",
	"dex__20.dta")



cl <- makeCluster(30)

clusterEvalQ(cl, library("data.table"))
clusterEvalQ(cl, library("readstata13"))

clusterExport(cl, "filez")



system.time(all_my_DEX <- parLapply(cl, filez, function(x) {
  
  ## Bring in data

  tmp <- data.table(read.dta13(paste0("/ihme/dex/usa/10_resources/13_other/data/chunks/",x)) )
  
  }
  ))


all_my_binds <- rbindlist(all_my_DEX)

stopCluster(cl)


rm(all_my_DEX)


# el_longo <- rbindlist(all_ages)

# write.dta(el_longo, file = paste0("/homes/sadatnfs/crap/all_ages_rbinded.dta"))
# write.dta(el_longo, file = paste0("/ihme/dex/usa/10_resources/13_other/data/chunks/all_ages_rbinded.dta"))


### GBD

gbd_file_names <- data.table(read.dta13("/home/j/temp/sadatnfs/gbd_file_names.dta"))

gbd_files <- gbd_file_names[,counter]


cl <- makeCluster(30)

clusterEvalQ(cl, library("data.table"))
clusterEvalQ(cl, library("readstata13"))

clusterExport(cl, "gbd_files")


system.time(all_my_geebuz <- parLapply(cl, gbd_files, function(x) {
  
  ## Bring in data

  tmp <- data.table(read.dta13(paste0("/ihme/dex/usa/10_resources/13_other/data/sorted/",x)) )
  
  return(tmp)

  }
  ))


all_my_geebuz_binded <- rbindlist(all_my_geebuz)



stopCluster(cl)



write.dta(all_my_geebuz_binded, file = paste0("/homes/sadatnfs/crap/gbd_all_binded.dta"))
write.dta(all_my_geebuz_binded, file = paste0("/ihme/dex/usa/10_resources/13_other/data/chunks/gbd_all_binded.dta"))



### File exists??

system.time(fileEx <- parLapply(cl, gbd_files, function(x) {
  

f <- paste0("/ihme/dex/usa/10_resources/13_other/data/sorted/",x)
if (file.exists(f)) {
  return(data.table(a = paste0(x), b = 1))
}

else {
	return(data.table(a = paste0(x), b = 0))
}

}))


fileExbinded <- rbindlist(fileEx)











