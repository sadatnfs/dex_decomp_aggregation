

rm(list=ls())
# require(doParallel)

require(foreign)
require(readstata13)
# require(abind)
# require(rlist)
require(data.table)

# require(parallel)
require(feather)
require(Rcpp)
# require(microbenchmark)
# require(RcppRoll)

## Create metadata for one

if(Sys.info()[1]=="Windows") {
  loc <- "H:/"
  jloc <- "J:/Project/IRH/DEX/USA/_explore/decomp/results_stats"
   jloc_d <- "J:/Project/IRH/DEX/USA/_explore/decomp"

} else {
  loc <- "/homes/sadatnfs"
  jloc <- "/home/j/Project/IRH/DEX/USA/_explore/decomp/results_stats"
  jloc_d <- "/home/j/Project/IRH/DEX/USA/_explore/decomp"
}

setwd(jloc)

## Bring in the draws (wide by draws)

wide_merged_string <- data.table(read_feather(paste0(jloc_d,"/wide_merged_string_clu.feather")))


# long_binded <- fread(paste0(loc,"/dex_comps/long_binded_string.csv"))


# Collapsed across all dimensions (one line); all UIs are 95% 

#### the following three only deals with "final_*

# by cause, by type of care; (2)
# (additive; sum away ages and sexes (keep cause and/OR keep care), keep variable)

# by cause-type of care (1)
#(additive, sum away ages and sexes; id.vars = cause, fn, var); 

# by  type-age (binary) (65+ vs under 65) ; (1)
# (sum away sexes and causes) 


### NOT THIS ONE DOE
# by time period (96-02, 02-08, 08-13) (1) (chunks 6+6+5)-by type of care
### (sum away acause, age, sex)
## (sum by chunks to get "3 final_* vars")


# Henceforth:
# by acause-function
# by acause
# by function
# by age65 (65+, < 65)
# by time period
# by nothing (the six-row data frame we talked about yesterday -- collapsed across all dimensions)
# 
# 
# one version with nothing dropped
# and one version with renal dropped for ages under 15




############################# (2) Collapse the functions 



#### Get the specific percentiles



melted_huge_finals <- wide_merged_string[variable == "final_effect" |
                                           variable == "final_epi_rate_effect" |
                                           variable == "final_pop_frac_effect" | 
                                           variable == "final_population_effect" | 
                                           variable == "final_price_effect" | 
                                           variable == "final_util_rate_effect" , ]


# draws <- paste0("d_",c(1:1000))



###################### TRUNCATE EVERYTHING FIRST

melted_huge_finals[, d_529:=NULL]
melted_huge_finals[, d_579:=NULL]
melted_huge_finals[, d_504:=NULL]
melted_huge_finals[, d_537:=NULL]
melted_huge_finals[, d_306:=NULL]
melted_huge_finals[, d_304:=NULL]
melted_huge_finals[, d_129:=NULL]
melted_huge_finals[, d_337:=NULL]
melted_huge_finals[, d_387:=NULL]
melted_huge_finals[, d_506:=NULL]
melted_huge_finals[, d_154:=NULL]
melted_huge_finals[, d_104:=NULL]
melted_huge_finals[, d_535:=NULL]
melted_huge_finals[, d_654:=NULL]
melted_huge_finals[, d_604:=NULL]
melted_huge_finals[, d_606:=NULL]
melted_huge_finals[, d_156:=NULL]
melted_huge_finals[, d_106:=NULL]
melted_huge_finals[, d_637:=NULL]
melted_huge_finals[, d_687:=NULL]
melted_huge_finals[, d_676:=NULL]
melted_huge_finals[, d_576:=NULL]
melted_huge_finals[, d_330:=NULL]
melted_huge_finals[, d_187:=NULL]
melted_huge_finals[, d_954:=NULL]
melted_huge_finals[, d_643:=NULL]
melted_huge_finals[, d_678:=NULL]
melted_huge_finals[, d_132:=NULL]
melted_huge_finals[, d_550:=NULL]
melted_huge_finals[, d_937:=NULL]
melted_huge_finals[, d_376:=NULL]
melted_huge_finals[, d_979:=NULL]
melted_huge_finals[, d_929:=NULL]
melted_huge_finals[, d_174:=NULL]
melted_huge_finals[, d_274:=NULL]
melted_huge_finals[, d_874:=NULL]
melted_huge_finals[, d_74:=NULL]
melted_huge_finals[, d_124:=NULL]
melted_huge_finals[, d_24:=NULL]
melted_huge_finals[, d_424:=NULL]
melted_huge_finals[, d_224:=NULL]
melted_huge_finals[, d_924:=NULL]
melted_huge_finals[, d_324:=NULL]
melted_huge_finals[, d_922:=NULL]
melted_huge_finals[, d_419:=NULL]
melted_huge_finals[, d_301:=NULL]
melted_huge_finals[, d_635:=NULL]
melted_huge_finals[, d_345:=NULL]
melted_huge_finals[, d_989:=NULL]
melted_huge_finals[, d_99:=NULL]
melted_huge_finals[, d_849:=NULL]
melted_huge_finals[, d_948:=NULL]
melted_huge_finals[, d_89:=NULL]
melted_huge_finals[, d_867:=NULL]
melted_huge_finals[, d_130:=NULL]
melted_huge_finals[, d_499:=NULL]
melted_huge_finals[, d_649:=NULL]
melted_huge_finals[, d_651:=NULL]
melted_huge_finals[, d_14:=NULL]
melted_huge_finals[, d_949:=NULL]
melted_huge_finals[, d_999:=NULL]
melted_huge_finals[, d_968:=NULL]
melted_huge_finals[, d_876:=NULL]
melted_huge_finals[, d_349:=NULL]
melted_huge_finals[, d_437:=NULL]
melted_huge_finals[, d_799:=NULL]
melted_huge_finals[, d_249:=NULL]
melted_huge_finals[, d_199:=NULL]
melted_huge_finals[, d_78:=NULL]


melted_huge_finals <- melt(melted_huge_finals, id.vars = c("fn", "acause", "sex", "age", "variable"),
                           value.name = "data", variable.name = "draw")

melted_final_vars <- melted_huge_finals

rm(melted_huge_finals)


# melted_huge_finals <- melted_huge_finals[, lower:=quantile(data, .01, na.rm=TRUE),by=c("fn", "acause", "sex", "age", "variable")]
# melted_huge_finals <- melted_huge_finals[, upper:=quantile(data, .99, na.rm=TRUE),by=c("fn", "acause", "sex", "age", "variable")]

# melted_huge_finals_trunc <- melted_huge_finals[data >= lower & data<=upper]

# melted_final_vars <- melted_huge_finals_trunc



### Let's z-score things using fn and draw specific median and mad

# fn_draw_z_score <- melted_huge_finals[, list(sum_data = sum(data, na.rm=T)), by=c("fn", "draw", "variable")]

# fn_draw_z_score <- fn_draw_z_score[, list(draw = draw, sum_data = sum_data, median = median(sum_data, na.rm=T), mad=mad(sum_data, na.rm=T)), by=c("fn", "variable")]
# fn_draw_z_score[, z_score := (sum_data - median)*0.6745/mad]


# ## Order
# fn_draw_z_score<-fn_draw_z_score[order(z_score)]

# ## Save
# write.dta(fn_draw_z_score, file = "z_score_of_output_by_fn_draw.dta")


### Keep only the stubs with final_ prefix

# only_final_vars <- wide_merged_string[variable == "final_effect" |
#                                         variable == "final_epi_rate_effect" |
#                                         variable == "final_pop_frac_effect" | 
#                                         variable == "final_population_effect" | 
#                                         variable == "final_price_effect" | 
#                                         variable == "final_util_rate_effect" , ]



## Melt is best
# melted_final_vars <- melt(only_final_vars, id.vars = c("fn", "acause", "sex", "age", "variable"), measure.vars = draws, value.name = "data", variable.name = "draw")
# melted_final_vars[, variable:= as.character(variable)]


# rm(melted_huge_finals)
# rm(only_final_vars)


#### DT where we keep everything:

# final_by_all <- melted_final_vars[, list(sum_data = sum(data, na.rm=T)), by=c("acause","fn", "sex", "age", "variable", "draw")]
final_by_all_stats <- melted_final_vars[, list(mean= mean(data), 
                                               lower=quantile(data, 0.025, na.rm=TRUE),
                                               upper=quantile(data, 0.975, na.rm=TRUE)), 
                                        by=c("acause","fn", "sex", "age", "variable")]





#### DT where we summed away fn, age, sex:

final_by_acause_variable <- melted_final_vars[, list(sum_data = sum(data, na.rm=T)), by=c("acause", "variable", "draw")]
final_by_acause_variable_stats <- final_by_acause_variable[, list(mean= mean(sum_data), 
                                                                  lower=quantile(sum_data, 0.025, na.rm=TRUE),
                                                                  upper=quantile(sum_data, 0.975, na.rm=TRUE)), by=c("acause", "variable")]




#### DT where we summed away acause, age, sex:

final_by_fn_variable <- melted_final_vars[, list(sum_data = sum(data, na.rm=T)), by=c("fn", "variable", "draw")]
final_by_fn_variable_stats <- final_by_fn_variable[, list(mean= mean(sum_data), 
                                                          lower=quantile(sum_data, 0.025, na.rm=TRUE),
                                                          upper=quantile(sum_data, 0.975, na.rm=TRUE)), by=c("fn", "variable")]



#### DT where we summed away age, sex:

final_by_acause_fn_variable <- melted_final_vars[, list(sum_data = sum(data, na.rm=T)), by=c("acause", "fn", "variable", "draw")]
final_by_acause_fn_variable_stats <- final_by_acause_fn_variable[, list(mean= mean(sum_data), 
                                                                        lower=quantile(sum_data, 0.025, na.rm=TRUE),
                                                                        upper=quantile(sum_data, 0.975, na.rm=TRUE)), by=c("acause", "fn", "variable")]


#### DT where we sum away sexes and causes, but have just two values for ages  65+ vs <65)
melted_final_vars[age<65, age_below_65:=1]
melted_final_vars[age>=65, age_below_65:=0]

# no_renal_melt[age<=65, age_below_65:=1]
# no_renal_melt[age>65, age_below_65:=0]


final_by_age65_fn_variable <- melted_final_vars[, list(sum_data = sum(data, na.rm=T)), by=c("age_below_65", "fn", "variable", "draw")]
final_by_age65_fn_variable_stats <- final_by_age65_fn_variable[, list(mean= mean(sum_data), 
                                                                      lower=quantile(sum_data, 0.025, na.rm=TRUE),
                                                                      upper=quantile(sum_data, 0.975, na.rm=TRUE)), by=c("age_below_65", "fn", "variable")]

### One version where we sum away everything!
final_by_variable <- melted_final_vars[, list(sum_data = sum(data, na.rm=T)), by=c("variable", "draw")]
final_by_variable_stats <- final_by_variable[, list(mean= mean(sum_data), 
                                                    lower=quantile(sum_data, 0.025, na.rm=TRUE),
                                                    upper=quantile(sum_data, 0.975, na.rm=TRUE)), 
                                             by=c("variable")]




#### CVD: 10 cvd vs non cvd
melted_final_vars[, cvd:=0]
melted_final_vars[grep("cvd", acause), cvd:=1]



final_by_cvd_variable <- melted_final_vars[, list(sum_data = sum(data, na.rm=T)), by=c("cvd", "variable", "draw")]
final_by_cvd_variable_stats <- final_by_cvd_variable[, list(mean= mean(sum_data), 
                                                            lower=quantile(sum_data, 0.025, na.rm=TRUE),
                                                            upper=quantile(sum_data, 0.975, na.rm=TRUE)), by=c("cvd", "variable")]



## Save these bad boys out in DTAs
write.dta(final_by_all_stats, file = "final_by_all_stats.dta")

write.dta(final_by_acause_variable_stats, file = "final_by_acause_variable_stats.dta")
write.dta(final_by_fn_variable_stats, file = "final_by_fn_variable_stats.dta")
write.dta(final_by_acause_fn_variable_stats, file = "final_by_acause_fn_variable_stats.dta")
write.dta(final_by_age65_fn_variable_stats, file = "final_by_age65_fn_variable_stats.dta")
write.dta(final_by_variable_stats, file = "final_by_variable_stats.dta")
write.dta(final_by_cvd_variable_stats, file = "final_by_cvd_variable_stats.dta")



#### Save some draws wide
final_by_variable_wide <- dcast(final_by_variable, variable ~ draw, value.var = "sum_data",  measure.vars = draws)
write.dta(final_by_variable_wide, file = "final_by_variable_draws.dta")


final_by_fn_variable_wide <- dcast(final_by_fn_variable, fn + variable ~ draw, value.var = "sum_data",  measure.vars = draws)
write.dta(final_by_fn_variable_wide, file = "final_by_fn_variable_draws.dta")


final_by_acause_fn_variable_wide <- dcast(final_by_acause_fn_variable, fn + acause + variable ~ draw, value.var = "sum_data",  measure.vars = draws)
write.dta(final_by_acause_fn_variable_wide, file = "final_by_acause_fn_variable_draws.dta")





















##### EL ESPECIALO

# by time period (96-02, 02-08, 08-13) (1) (chunks 6+6+5)-by type of care
### (sum away acause, age, sex)
## (sum by chunks to get "3 final_* vars")


collapse_the_timez_huge <- wide_merged_string[  variable != "final_effect" &
                                                  variable != "final_epi_rate_effect" &
                                                  variable != "final_pop_frac_effect" & 
                                                  variable != "final_population_effect" & 
                                                  variable != "final_price_effect" & 
                                                  variable != "final_util_rate_effect" , ]

rm(wide_merged_string)


## Drop the draws which died from thresholds
collapse_the_timez_huge[, d_529:=NULL]
collapse_the_timez_huge[, d_579:=NULL]
collapse_the_timez_huge[, d_504:=NULL]
collapse_the_timez_huge[, d_537:=NULL]
collapse_the_timez_huge[, d_306:=NULL]
collapse_the_timez_huge[, d_304:=NULL]
collapse_the_timez_huge[, d_129:=NULL]
collapse_the_timez_huge[, d_337:=NULL]
collapse_the_timez_huge[, d_387:=NULL]
collapse_the_timez_huge[, d_506:=NULL]
collapse_the_timez_huge[, d_154:=NULL]
collapse_the_timez_huge[, d_104:=NULL]
collapse_the_timez_huge[, d_535:=NULL]
collapse_the_timez_huge[, d_654:=NULL]
collapse_the_timez_huge[, d_604:=NULL]
collapse_the_timez_huge[, d_606:=NULL]
collapse_the_timez_huge[, d_156:=NULL]
collapse_the_timez_huge[, d_106:=NULL]
collapse_the_timez_huge[, d_637:=NULL]
collapse_the_timez_huge[, d_687:=NULL]
collapse_the_timez_huge[, d_676:=NULL]
collapse_the_timez_huge[, d_576:=NULL]
collapse_the_timez_huge[, d_330:=NULL]
collapse_the_timez_huge[, d_187:=NULL]
collapse_the_timez_huge[, d_954:=NULL]
collapse_the_timez_huge[, d_643:=NULL]
collapse_the_timez_huge[, d_678:=NULL]
collapse_the_timez_huge[, d_132:=NULL]
collapse_the_timez_huge[, d_550:=NULL]
collapse_the_timez_huge[, d_937:=NULL]
collapse_the_timez_huge[, d_376:=NULL]
collapse_the_timez_huge[, d_979:=NULL]
collapse_the_timez_huge[, d_929:=NULL]
collapse_the_timez_huge[, d_174:=NULL]
collapse_the_timez_huge[, d_274:=NULL]
collapse_the_timez_huge[, d_874:=NULL]
collapse_the_timez_huge[, d_74:=NULL]
collapse_the_timez_huge[, d_124:=NULL]
collapse_the_timez_huge[, d_24:=NULL]
collapse_the_timez_huge[, d_424:=NULL]
collapse_the_timez_huge[, d_224:=NULL]
collapse_the_timez_huge[, d_924:=NULL]
collapse_the_timez_huge[, d_324:=NULL]
collapse_the_timez_huge[, d_922:=NULL]
collapse_the_timez_huge[, d_419:=NULL]
collapse_the_timez_huge[, d_301:=NULL]
collapse_the_timez_huge[, d_635:=NULL]
collapse_the_timez_huge[, d_345:=NULL]
collapse_the_timez_huge[, d_989:=NULL]
collapse_the_timez_huge[, d_99:=NULL]
collapse_the_timez_huge[, d_849:=NULL]
collapse_the_timez_huge[, d_948:=NULL]
collapse_the_timez_huge[, d_89:=NULL]
collapse_the_timez_huge[, d_867:=NULL]
collapse_the_timez_huge[, d_130:=NULL]
collapse_the_timez_huge[, d_499:=NULL]
collapse_the_timez_huge[, d_649:=NULL]
collapse_the_timez_huge[, d_651:=NULL]
collapse_the_timez_huge[, d_14:=NULL]
collapse_the_timez_huge[, d_949:=NULL]
collapse_the_timez_huge[, d_999:=NULL]
collapse_the_timez_huge[, d_968:=NULL]
collapse_the_timez_huge[, d_876:=NULL]
collapse_the_timez_huge[, d_349:=NULL]
collapse_the_timez_huge[, d_437:=NULL]
collapse_the_timez_huge[, d_799:=NULL]
collapse_the_timez_huge[, d_249:=NULL]
collapse_the_timez_huge[, d_199:=NULL]
collapse_the_timez_huge[, d_78:=NULL]


collapse_the_timez_huge <- collapse_the_timez_huge[variable == "population_effect_12" | variable == "population_effect_23" | variable == "population_effect_34" | variable == "population_effect_45" | variable == "population_effect_56" | variable == "population_effect_67", effect:= "population_effect_1"]
collapse_the_timez_huge <- collapse_the_timez_huge[variable == "population_effect_78" | variable == "population_effect_89" | variable == "population_effect_910"  | variable == "population_effect_1011"  | variable == "population_effect_1112" | variable == "population_effect_1213", effect:= "population_effect_2"]
collapse_the_timez_huge <- collapse_the_timez_huge[variable == "population_effect_1314" | variable == "population_effect_1415" | variable == "population_effect_1516" | variable == "population_effect_1617" | variable == "population_effect_1718", effect:= "population_effect_3"]




collapse_the_timez_huge <- collapse_the_timez_huge[variable == "pop_frac_effect_12" | variable == "pop_frac_effect_23" | variable == "pop_frac_effect_34" | variable == "pop_frac_effect_45" | variable == "pop_frac_effect_56" | variable == "pop_frac_effect_67" , effect:= "pop_frac_effect_1"]
collapse_the_timez_huge <- collapse_the_timez_huge[variable == "pop_frac_effect_78" | variable == "pop_frac_effect_89" | variable == "pop_frac_effect_910" | variable == "pop_frac_effect_1011" | variable == "pop_frac_effect_1112" | variable == "pop_frac_effect_1213" , effect:= "pop_frac_effect_2"]
collapse_the_timez_huge <- collapse_the_timez_huge[variable == "pop_frac_effect_1314" | variable == "pop_frac_effect_1415" | variable == "pop_frac_effect_1516" | variable == "pop_frac_effect_1617" | variable == "pop_frac_effect_1718" , effect:= "pop_frac_effect_3"]




collapse_the_timez_huge <- collapse_the_timez_huge[variable == "epi_rate_effect_12" | variable == "epi_rate_effect_23" | variable == "epi_rate_effect_34" | variable == "epi_rate_effect_45" | variable == "epi_rate_effect_56" | variable == "epi_rate_effect_67", effect:= "epi_rate_effect_1"]
collapse_the_timez_huge <- collapse_the_timez_huge[variable == "epi_rate_effect_78" | variable == "epi_rate_effect_89" | variable == "epi_rate_effect_910" | variable == "epi_rate_effect_1011" | variable == "epi_rate_effect_1112" | variable == "epi_rate_effect_1213", effect:= "epi_rate_effect_2"]
collapse_the_timez_huge <- collapse_the_timez_huge[variable == "epi_rate_effect_1314" | variable == "epi_rate_effect_1415" | variable == "epi_rate_effect_1516" | variable == "epi_rate_effect_1617" | variable == "epi_rate_effect_1718", effect:= "epi_rate_effect_3"]





collapse_the_timez_huge <- collapse_the_timez_huge[variable == "util_rate_effect_12" | variable == "util_rate_effect_23"  | variable == "util_rate_effect_34" | variable == "util_rate_effect_45" | variable == "util_rate_effect_56" | variable == "util_rate_effect_67", effect:= "util_rate_effect_1"]
collapse_the_timez_huge <- collapse_the_timez_huge[variable == "util_rate_effect_78" | variable == "util_rate_effect_89" | variable == "util_rate_effect_910" | variable == "util_rate_effect_1011" | variable == "util_rate_effect_1112" | variable == "util_rate_effect_1213", effect:= "util_rate_effect_2"]
collapse_the_timez_huge <- collapse_the_timez_huge[variable == "util_rate_effect_1314" | variable == "util_rate_effect_1415" | variable == "util_rate_effect_1516" | variable == "util_rate_effect_1617" | variable == "util_rate_effect_1718", effect:= "util_rate_effect_3"]




collapse_the_timez_huge <- collapse_the_timez_huge[variable == "price_effect_12"  | variable == "price_effect_23" | variable == "price_effect_34" | variable == "price_effect_45" | variable == "price_effect_56" | variable == "price_effect_67", effect:= "price_effect_1"]
collapse_the_timez_huge <- collapse_the_timez_huge[variable == "price_effect_78" | variable == "price_effect_89" | variable == "price_effect_910"  | variable == "price_effect_1011"  | variable == "price_effect_1112" | variable == "price_effect_1213", effect:= "price_effect_2"]
collapse_the_timez_huge <- collapse_the_timez_huge[variable == "price_effect_1314" | variable == "price_effect_1415" | variable == "price_effect_1516" | variable == "price_effect_1617" | variable == "price_effect_1718", effect:= "price_effect_3"]

collapse_the_timez_huge[, variable:=NULL]



#### Make a final effect 1 2 and 3 variable
collapse_the_timez_huge <- collapse_the_timez_huge[effect == "price_effect_1" |
                                                     effect == "util_rate_effect_1" | effect == "epi_rate_effect_1" | effect == "pop_frac_effect_1" | 
                                                     effect == "population_effect_1" , final_effect:= "final_effect_1"]


collapse_the_timez_huge <- collapse_the_timez_huge[effect == "price_effect_2" |
                                                     effect == "util_rate_effect_2" | effect == "epi_rate_effect_2" | effect == "pop_frac_effect_2" | 
                                                     effect == "population_effect_2" , final_effect:= "final_effect_2"]


collapse_the_timez_huge <- collapse_the_timez_huge[effect == "price_effect_3" |
                                                     effect == "util_rate_effect_3" | effect == "epi_rate_effect_3" | effect == "pop_frac_effect_3" | 
                                                     effect == "population_effect_3" , final_effect:= "final_effect_3"]



##### FINAL EFFECT BLOCK


### Melt the draws long

system.time(FINAL_collapse_the_timez_huge_trunc <- melt(collapse_the_timez_huge[, effect:=NULL], 
                                                        id.vars = c("fn", "acause", "sex", "age", "final_effect"), 
                                                        value.name = "data", variable.name = "draw"))



#### let's sum away fn, age, sex, effect (JUST ONE LINE PER ACAUSE)
total_acause_FINAL_effect_summed_meltz_collapse_the_timez_huge_finals_trunc <- FINAL_collapse_the_timez_huge_trunc[, list(data = sum(data, na.rm=T)), 
                                                                                                         by=c("acause" ,    "draw")]


time_effects_by_total_acause_FINAL <- total_acause_FINAL_effect_summed_meltz_collapse_the_timez_huge_finals_trunc[, list(means = mean(data, na.rm=T), 
                                                                                                     lower = quantile(data, .025, na.rm=T),
                                                                                                     upper = quantile(data, .975, na.rm=T)), by=c("acause")  ]


write.dta(time_effects_by_total_acause_FINAL, file = "time_effects_by_total_acause_FINAL.dta")


#### let's sum away acause, age, sex (JUST FOR FINAL EFFECT DAMMIT)
fn_FINAL_effect_summed_meltz_collapse_the_timez_huge_finals_trunc <- FINAL_collapse_the_timez_huge_trunc[, list(data = sum(data, na.rm=T)), 
                                                                                                         by=c("fn" ,   "final_effect", "draw")]


time_effects_by_fn_FINAL <- fn_FINAL_effect_summed_meltz_collapse_the_timez_huge_finals_trunc[, list(means = mean(data, na.rm=T), 
                                                                                                     lower = quantile(data, .025, na.rm=T),
                                                                                                     upper = quantile(data, .975, na.rm=T)), by=c("fn", "final_effect")  ]



write.dta(time_effects_by_fn_FINAL, file = "time_effects_by_fn_FINAL.dta")


#### let's sum away fn, age, sex (JUST FOR FINAL EFFECT DAMMIT)
acause_FINAL_effect_summed_meltz_collapse_the_timez_huge_finals_trunc <- FINAL_collapse_the_timez_huge_trunc[, list(data = sum(data, na.rm=T)), 
                                                                                                         by=c("acause" ,   "final_effect", "draw")]


time_effects_by_acause_FINAL <- acause_FINAL_effect_summed_meltz_collapse_the_timez_huge_finals_trunc[, list(means = mean(data, na.rm=T), 
                                                                                                     lower = quantile(data, .025, na.rm=T),
                                                                                                     upper = quantile(data, .975, na.rm=T)), by=c("acause", "final_effect")  ]



write.dta(time_effects_by_acause_FINAL, file = "time_effects_by_acause_FINAL.dta")




# draws <- paste0("d_",c(1:1000))

### Melt the draws long
system.time(collapse_the_timez_huge_trunc <- melt(collapse_the_timez_huge, id.vars = c("fn", "acause", "sex", "age", "effect"), 
                                                  value.name = "data", variable.name = "draw"))


#### let's sum away acause, age, sex
fn_effect_summed_meltz_collapse_the_timez_huge_finals_trunc <- collapse_the_timez_huge_trunc[, list(data = sum(data, na.rm=T)), 
                                                                                             by=c("fn" ,   "effect", "draw")]


#### let's sum away fn, age, sex
acause_effect_summed_meltz_collapse_the_timez_huge_finals_trunc <- collapse_the_timez_huge_trunc[, list(data = sum(data, na.rm=T)), 
                                                                                                 by=c("acause" ,   "effect", "draw")]



#### let's sum away  age, sex
acause_fn_effect_summed_meltz_collapse_the_timez_huge_finals_trunc <- collapse_the_timez_huge_trunc[, list(data = sum(data, na.rm=T)), 
                                                                                                    by=c("acause", "fn", "effect", "draw")]


### Mean and UIs

### Functions


time_effects_by_fn <- fn_effect_summed_meltz_collapse_the_timez_huge_finals_trunc[, list(means = mean(data, na.rm=T), 
                                                                                         lower = quantile(data, .025, na.rm=T),
                                                                                         upper = quantile(data, .975, na.rm=T)), by=c("fn", "effect")  ]


write.dta(time_effects_by_fn, file = "time_effects_by_fn.dta")


### ACAUSE 

time_effects_by_acause <- acause_effect_summed_meltz_collapse_the_timez_huge_finals_trunc[, list(means = mean(data, na.rm=T), 
                                                                                                 lower = quantile(data, .025, na.rm=T),
                                                                                                 
                                                                                                 upper = quantile(data, .975, na.rm=T)), by=c("acause", "effect")  ]
write.dta(time_effects_by_acause, file = "time_effects_by_acause.dta")



### ACAUSE - FN

time_effects_by_acause_fn <- acause_fn_effect_summed_meltz_collapse_the_timez_huge_finals_trunc[, list(means = mean(data, na.rm=T), 
                                                                                                       lower = quantile(data, .025, na.rm=T),
                                                                                                       
                                                                                                       upper = quantile(data, .975, na.rm=T)), by=c("acause", "fn",  "effect")  ]
write.dta(time_effects_by_acause_fn, file = "time_effects_by_acause_fn.dta")

### By just effect

## Sum first
effect_summed_meltz_collapse_the_timez_huge_finals_trunc <- fn_effect_summed_meltz_collapse_the_timez_huge_finals_trunc[,list(data = sum(data, na.rm=T)), 
                                                                                                                        by=c("effect", "draw")]

time_effects <- effect_summed_meltz_collapse_the_timez_huge_finals_trunc[, list(means = mean(data, na.rm=T), 
                                                                                lower = quantile(data, .025, na.rm=T),
                                                                                upper = quantile(data, .975, na.rm=T)), by=c( "effect")  ]


write.dta(time_effects, file = "time_effects.dta")






