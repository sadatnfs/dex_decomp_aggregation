
require(foreign)
require(readstata13)
require(data.table)


j <- ifelse(Sys.info()[1]=="Windows", "J:/", "/home/j")


gbd <- data.table(read.dta13("J:/Project/IRH/DEX/USA/10_resources/13_other/data/final/prev_and_pop_from_GBD.dta"))
 

 gbd_all <- gbd[year>1995 & year<2014,c("year", "age_id", "age", "sex", "acause",  "population", paste0("draw_", c(0:999)))]

 colnames(gbd_all) <- c("year", "age_id", "age", "sex", "acause", "population", c(0:999))

gbd_all_melt <- melt(gbd_all, id.vars=c("year", "age_id", "age", "sex", "acause",  "population"), value.name = "data", variable.name="draw")


## Sort descending

gbd_all_melt <- gbd_all_melt[order(acause, age, sex, year, -data)]
colnames(gbd_all_melt)[8] <- "cases"


## Create a new "rank"
gbd_all_melt <- gbd_all_melt[, rank:= c(1:1000), 
                             by = c("year", "age_id", "age", "sex", "acause")]
gbd_all_melt[, draw:=NULL]


write.dta(gbd_all_melt, file = "J:/temp/sadatnfs/dex_decomp_chunk/im_so_gbd.dta")



### Bring in DEX data
system.time(all_my_DEX <- data.table(read.dta13(paste0(j,"/temp/sadatnfs/dex_decomp_chunk/dex_all.dta"))))

rm(gbd_all)
rm(gbd)

### Make a merge variable indicator
all_my_DEX[, merge_D:=1]
gbd_all_melt[, merge_G:=2]


### Left Join GBD on to DEX

system.time(dex_gbd <- merge(all_my_DEX , gbd_all_melt, by=c( "year" , "age",  "sex",  "acause",  "rank"), all.x=T))

