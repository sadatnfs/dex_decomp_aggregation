	
require(foreign)
require(readstata13)
require(data.table)
require(doParallel)

j <- ifelse(Sys.info()[1]=="Windows", "J:/", "/home/j/")


acauses <- data.table(read.dta13("J:/Project/IRH/DEX/USA/_explore/decomp/acause.dta"))
acauses <- acauses[, acause]




cl <- makeCluster(26)

clusterEvalQ(cl, library("data.table"))
clusterEvalQ(cl, library("readstata13"))

clusterExport(cl, "acauses")
clusterExport(cl, "j")



system.time(all_acauses <- parLapply(cl, acauses, function(x) {
  
  ## Bring in data

  tmp <- fread(paste0(j,"/Project/IRH/DEX/USA/10_resources/13_other/data/acauses/",x, ".csv"))


  


  }

  ))



system.time(el_longo <- rbindlist(all_acauses))

stopCluster(cl)

fwrite(el_longo, file = paste0(j,"/Project/IRH/DEX/USA/10_resources/13_other/data/draws/all_acause_with_z_scores.csv"))

write.dta(el_longo, file = paste0(j,"/Project/IRH/DEX/USA/10_resources/13_other/data/draws/all_acause_with_z_scores.dta"))

save(el_longo, file = paste0(j,"/Project/IRH/DEX/USA/10_resources/13_other/data/draws/all_acause_with_z_scores.Rdata"))





### Histogram

require(ggplot2)
require(Cairo)


### Melt on the varnames (keeping only what we want first)
el_longo_small <- el_longo[, c("function", "acause", "age", "sex", "year", "draw", "exp_z", "vol_z", "case_z")]
system.time(el_longo_small <- melt(el_longo_small, 
						id.vars= c("function", "acause", "age", "sex", "year", "draw"),
						variable.name = "metric", value.name = "z_score"))

colnames(el_longo_small)[1]<- "fn"

### Let's make 6 panel histograms (6 fns) * 155 causes * 3 metrics

### Testing:

ac <- "neo_lymphoma"


plot <- function(ac) {
	ggplot(el_longo_small[acause==paste0(ac)]) + geom_histogram(aes(x = z_score), alpha=0.5) +
	facet_grid(metric ~ fn)

}

setwd("J:/temp/sadatnfs/dex_decomp_chunk")

Cairo(file="histo.pdf", type="pdf", dpi=80, width=1600, height=900, onefile=TRUE) 

for(ac in acauses) {
plotz <- 	ggplot(el_longo_small[acause==paste0(ac)]) +
	 geom_histogram(aes(x = z_score), alpha=0.5) + 
	 ggtitle(paste0(ac)) +
	facet_grid(metric ~ fn)


plot(plotz)
}


graphics.off()































