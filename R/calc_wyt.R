#!usr/bin/env Rscript
#
# Purpose: Given a date (YYYY-MM-DD) return the corresponding Water Year
#
# Notes: 
#   The term U.S.Geological Survey "water year" in reports that deal with surface-water supply 
#   is defined as the 12-month period October 1, for any given year through September 30, 
#   of the following year. The water year is designated by the calendar year in which it ends 
#   and which includes 9 of the 12 months. Thus, the year ending September 30, 1999 is called the "1999" water year.
#  
# Author: John R. Brandon, PhD (ICF)
#         john.brandon at icf.com
# Date: 2022-07-13
library(pacman)
p_load(here, tidyverse, lubridate, waterYearType)

# Function ---------------------------------------------------------------------
calc_wyt = function(dates, wyt_indices = waterYearType::water_year_indices){
  # Expects `date` to be a vector as argument
  # Also assumes wyt_indices are retrieved from the `waterYearType` package from CDEC
  sac_wyt = wyt_indices %>% 
    filter(location == "Sacramento Valley") %>% 
    select(WY, Yr_type)
  dat = tibble(dates) %>% 
    mutate(mm = month(dates),
           mm_abb = month.abb[mm],
           offset_yr = ifelse(mm_abb %in% c("Oct", "Nov", "Dec"), 1, 0),
           cal_yr = year(dates),
           wy_yr = cal_yr + offset_yr)
  dat %>% 
    left_join(sac_wyt, by = c("wy_yr" = "WY"))
}

foo = calc_wyt(date = fish_days$DetectDate, wyt = water_year_indices)
foo  # Check
