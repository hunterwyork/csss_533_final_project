library(data.table)
library(magrittr)
library(haven)
library(foreign)
library(dplyr)
library(stringr)
library(ggplot2)
library(parallel)

#pull in location hierarchy 
locs <- fread("<<<Path to locs.csv>>>")

#RR Estimation of Childhood mortality from Complete birth histories
##Hunter York
#Arguments
cores <- 6

###load cbh files
files <- ("<<<Path to data>>>")

data.list <- mclapply(files, function(file) {
  data <- read.dta(file)
},mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores))


merged_data <- data.table(rbindlist(data.list,fill=T))

###
merged_data <- merged_data[,.(nid, hh_id, strata, psu, admin_1,
                              ihme_loc_id, year_start, 
                              age_of_death_number, age_of_death_units, 
                              child_age_at_death_months, child_age_at_death_raw,
                              child_alive, child_dob_cmc, child_id, child_sex,
                              maternal_ed_yrs, mother_id, wealth_index_dhs,
                              wealth_index_dhs_continuous,
                              age_month, pweight, urban, mother_age_years,
                              paternal_ed_yrs, int_month, int_year,
                              mother_height_cm_1d, interview_date_cmc, contraceptive_type,
                              geospatial_id)]

#############################################
#create outcome identifiers#
#############################################
merged_data[child_age_at_death_months < 180, death_under_15 := 1]
merged_data[!(child_age_at_death_months < 180), death_under_15 := 0]
merged_data[child_age_at_death_months < 120, death_under_10 := 1]
merged_data[!(child_age_at_death_months < 120), death_under_10 := 0]
merged_data[child_age_at_death_months < 60, death_under_5 := 1]
merged_data[!(child_age_at_death_months < 60), death_under_5 := 0]
merged_data[child_age_at_death_months < 12, death_under_1 := 1]
merged_data[!(child_age_at_death_months < 12), death_under_1 := 0]
merged_data[child_age_at_death_months < 180 & child_age_at_death_months >= 60, mort10q5 := 1]
merged_data[!(child_age_at_death_months < 180 & child_age_at_death_months >= 60), mort10q5 := 0]
merged_data[child_age_at_death_months < 60, mort5q0 := 1]
merged_data[!(child_age_at_death_months < 60), mort5q0 := 0]
merged_data[mort5q0 == 1, mort10q5 := NA]
merged_data[is.na(child_age_at_death_months), mort10q5 := 0]
merged_data[is.na(child_age_at_death_months), mort5q0 := 0]


####################################
#create demographic identifiers
####################################
merged_data[, age_year := floor(age_month / 12)]
merged_data[,interview_year := 1900+ floor((interview_date_cmc -1)/12)]
merged_data[, year_of_birth := 1900 + floor((child_dob_cmc -1)/12)]
merged_data[,mother_age_at_birth := mother_age_years - age_year]

merged_data[child_age_at_death_months == 6000, child_age_at_death_months := NA]
merged_data[maternal_ed_yrs > 25, maternal_ed_yrs := NA]
merged_data[maternal_ed_yrs > 18, maternal_ed_yrs := 18]
merged_data[paternal_ed_yrs > 25, paternal_ed_yrs := NA]
merged_data[paternal_ed_yrs > 18, paternal_ed_yrs := 18]

######################################################################
#tabulate total person years (to death or to interview, depending)
######################################################################
merged_data[,birthtointerview_cmc := interview_date_cmc - child_dob_cmc]
merged_data[child_alive == "Yes", person_months := birthtointerview_cmc]
merged_data[child_alive == "No", person_months := child_age_at_death_months]
merged_data[, person_years := person_months/12]
merged_data <- merged_data[!ihme_loc_id %in% unique(merged_data[is.na(child_alive)]$ihme_loc_id)]


#create specific person year caps for different demographic bins
##assume all babies with person years 0 lived to be a day  old
merged_data[person_years == 0, person_years := .002739]

##lowercase wealth var
merged_data[, wealth_index_dhs := tolower(wealth_index_dhs)]

#mother_id_var
merged_data[, mother_id_var := paste(ihme_loc_id, year_start, hh_id, psu, strata,mother_id, sep = "_")]
merged_data[, mother_id_var := as.numeric(as.factor(mother_id_var))]

#create survey id
merged_data[, survey_id := paste0(ihme_loc_id, year_start)]

###make sure age var is limited when the child dies)
merged_data[age_month > child_age_at_death_months,age_month := child_age_at_death_months]
merged_data[,age_year := floor(age_month/12)]

#fix wealth variable
merged_data[wealth_index_dhs == 1, wealth_index_dhs := "poorest"]
merged_data[wealth_index_dhs == 2, wealth_index_dhs := "poorer"]
merged_data[wealth_index_dhs == 3, wealth_index_dhs := "middle"]
merged_data[wealth_index_dhs == 4, wealth_index_dhs := "richer"]
merged_data[wealth_index_dhs == 5, wealth_index_dhs := "richest"]
merged_data[, wealth_index_dhs := relevel(as.factor(wealth_index_dhs), ref = "middle")]

#create decade child born variable
merged_data[,child_year_born := 1900 + floor((child_dob_cmc -1)/12)]
merged_data[, decade_child_born := floor(child_year_born/10)*10]

#create table of available data

table <- merged_data[,.(`Number of Surveys` = length(unique(survey_id)),
                        `Total Mothers` = length(unique(mother_id_var)),
                        `Total Live Births` = .N,
                        `Total Under-5 Deaths` = sum(death_under_5, na.rm = T),
                        `Total Under-15 Deaths` = sum(death_under_15, na.rm = T)), by = .(ihme_loc_id)]
table <- merge(locs[,.(ihme_loc_id, location_name)], table, by = "ihme_loc_id")
table$ihme_loc_id <- NULL
table %>% setnames("location_name", "Location")

#append totals to the bottom
table_totals <- table[,lapply(.SD, sum, na.rm = T), .SDcols = names(table)[names(table) != "Location"]]
table_totals <- cbind(data.table(Location = length(table$Location)), table_totals)
table <- rbind(table, table_totals, fill = T)


rownames(table) <- NULL

write.csv(table[!is.na(`Total Under-15 Deaths`)],  "<<<Path to table>>>")


#now create some life-table-esque measurements
#for each 1-year interval from 1 to 15, 
#tabulate mortality rate by country

get_mort_rate <- function(i, data){
  c.year_start <- i
  #subset to children who survived to beginning of exposure interval
  entry_cohort <- data[age_month >= i*12]
  #calculate person-years lived in interval
  entry_cohort[,person_years_int := person_years - i]
  entry_cohort[person_years_int > 1, person_years_int := 1]
  #create indicator for if child died in interval
  entry_cohort[child_age_at_death_months >=(i*12) & child_age_at_death_months <((i+1)*12), child_death_int := 1]
  entry_cohort[is.na(child_death_int), child_death_int := 0]
  #calculate mortality rate within interval
  tabs <- entry_cohort[,.(mortality_rate = sum(child_death_int*pweight)/sum(person_years_int*pweight),
                          total_children_obs = .N), by = .(ihme_loc_id, decade_child_born)]
  tabs[, year_start := i]
  return(tabs)
}

mort_rates <- rbindlist(lapply(1:14,get_mort_rate, data = merged_data))
#make a heat map of 1980 birth-cohort, since that has the bulk of the children
ihme_loc_relev <- mort_rates[year_start == 5 & decade_child_born == 1980] %>% .[order(mortality_rate)] %>% .[,ihme_loc_id]
mort_rates[, ihme_loc_id := factor(ihme_loc_id, levels = ihme_loc_relev)]
ggplot(mort_rates[decade_child_born == 1980 & year_start >= 5 & total_children_obs > 5000], aes(ihme_loc_id, year_start, fill= (mortality_rate+.000001)*1000)) + 
  geom_tile() +
  scale_fill_gradient(low="blue", high="red", limits = c(0,12)) + 
  scale_y_continuous(breaks = seq(5,14,1)) + theme_bw() +
  labs(x = "ISO3", y = "Interval Start (in Years)", fill = "Mortality Rate (per 1,000 person-years)") + 
  theme(legend.position = "bottom") + 
  ggtitle("Mortality Rates for ages 5-15, in the 1980-1989 Birth Cohort")

#now do it by educational attainment

get_mort_rate_ed <- function(i, data){
  c.year_start <- i
  #subset to children who survived to beginning of exposure interval
  entry_cohort <- data[age_month >= i*12]
  #calculate person-years lived in interval
  entry_cohort[,person_years_int := person_years - i]
  entry_cohort[person_years_int > 1, person_years_int := 1]
  #create indicator for if child died in interval
  entry_cohort[child_age_at_death_months >=(i*12) & child_age_at_death_months <((i+1)*12), child_death_int := 1]
  entry_cohort[is.na(child_death_int), child_death_int := 0]
  #create rounded maternal_ed
  entry_cohort[, maternal_ed_yrs_round := floor(maternal_ed_yrs/3)*3]
  entry_cohort[maternal_ed_yrs_round==18, maternal_ed_yrs_round := 15]
  #calculate mortality rate within interval
  tabs <- entry_cohort[!ihme_loc_id %in% c("TUR", "PER", "ZWE") &
                         ihme_loc_id %in% mort_rates[decade_child_born == 1980 & year_start >= 5 &
                                                       total_children_obs > 5000]$ihme_loc_id,
                       .(mortality_rate = sum(child_death_int*pweight)/sum(person_years_int*pweight),
                          total_children_obs = .N), by = .(decade_child_born, maternal_ed_yrs_round)]
  tabs[, year_start := i]
  return(tabs)
}

mort_rates_by_ed <- rbindlist(lapply(0:14,get_mort_rate_ed, data = merged_data))
#make a heat map of 1980 birth-cohort, since that has the bulk of the children
ggplot(mort_rates_by_ed[decade_child_born == 1980 & year_start >= 5 & !is.na(maternal_ed_yrs_round)], 
       aes(maternal_ed_yrs_round, year_start, fill= (mortality_rate+.000001)*1000)) + 
  geom_tile() +
  scale_fill_gradient(low="blue", high="red", limits = c(0,12)) + 
  scale_y_continuous(breaks = seq(5,14,1)) + 
  scale_x_continuous(breaks = seq(0,17,3), labels = paste0(seq(0,15,3), " to ", c(seq(3,15,3),19)-1))+
  theme_bw() +
  labs(x = "Maternal Education (In Years)", y = "Interval Start (in Years)", fill = "Mortality Rate (per 1,000 person-years)") + 
  theme(legend.position = "bottom") + 
  ggtitle("Mortality Rates for ages 5-15, in the 1980-1989 Birth Cohort")

#explore age reporting heaping
mort_rates_by_ed[, maternal_ed_yrs_binned := factor(maternal_ed_yrs_round, levels = seq(0,15,3), labels = paste0(seq(0,15,3), " to ", c(seq(3,15,3),19)-1))]
paste0(seq(0,15,3), " to ", c(seq(3,15,3),19)-1)
ggplot(mort_rates_by_ed[decade_child_born == 1980 & year_start >= 0 &maternal_ed_yrs_round <=9& !is.na(maternal_ed_yrs_round)]) + 
  geom_line(aes(x = year_start, y = mortality_rate*1000, color = maternal_ed_yrs_binned)) +
  xlab("Age") + ylab("Mortality Rate (per 1,000 person-years") + labs(color = "Maternal Education") + 
  theme_bw() + scale_y_continuous(trans = "log10") + 
  scale_x_continuous(breaks = seq(0,15,1))+
  ggtitle("Mortality Rates by Educational Attainment")
