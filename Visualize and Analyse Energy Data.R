### Set Directory ###

getwd()
setwd("/Users/sergiorobledo/Desktop")

###                         ###
### Install Packages Needed ###
###                         ###

install.packages("RMySQL")
library(RMySQL)
install.packages("dplyr")
library(dplyr)
install.packages("lubridate")
library(lubridate)
install.packages("tidyr")
library(tidyr)
install.packages("ggplot2")
library(ggplot2)
install.packages("forecast")
library(forecast)

###                 ###
### Load & Query DS ###
###                 ###

con = dbConnect(MySQL(),user='deepAnalytics',password='Sqltask1234!',dbname='dataanalytics2018',host='data-analytics-2018.cbrosir2cswx.us-east-1.rds.amazonaws.com')
dbListTables(con)
dbListFields(con,'iris')

dbListFields(con,'yr_2006')
yr_2006 <- dbGetQuery(con, "SELECT Date,Time,Sub_metering_1,Sub_metering_2,Sub_metering_3 FROM yr_2006")
yr_2007 <- dbGetQuery(con, "SELECT Date,Time,Sub_metering_1,Sub_metering_2,Sub_metering_3 FROM yr_2007")
yr_2008 <- dbGetQuery(con, "SELECT Date,Time,Sub_metering_1,Sub_metering_2,Sub_metering_3 FROM yr_2008")
yr_2009 <- dbGetQuery(con, "SELECT Date,Time,Sub_metering_1,Sub_metering_2,Sub_metering_3 FROM yr_2009")
yr_2010 <- dbGetQuery(con, "SELECT Date,Time,Sub_metering_1,Sub_metering_2,Sub_metering_3 FROM yr_2010")

yr_full <- bind_rows(yr_2007,yr_2008,yr_2009,yr_2010)
class(yr_full)
str(yr_full)

###                ###
### Pre-process DS ###
###                ###

yr_full <- yr_full %>% 
  unite(DateTime, Date, Time, sep = " ",remove = FALSE)
yr_full$DateTime <- as.POSIXct(yr_full$DateTime,
                               format= "%Y-%m-%d %H:%M:%S",
                               tz = "America/New_York")
str(yr_full)

###           ###
### Filtering ###
###           ###

yr_daily <- yr_full %>% 
  mutate(Year = year(DateTime), Month = month(DateTime), Day = day(DateTime)) %>% 
  filter(Date >= "2007-01-01" & Date <= "2010-11-25") %>% 
  group_by(Year,Month,Day) %>% 
  summarize(SM1 = round(sum(Sub_metering_1 / 1000, na.rm = TRUE),3),
            SM2 = round(sum(Sub_metering_2 / 1000, na.rm = TRUE),3),
            SM3 = round(sum(Sub_metering_3 / 1000, na.rm = TRUE),3),
            DateTime = first(DateTime)) %>% 
  filter(!is.na(Day))
str(yr_daily)

Yr.sum.wide <- yr_full %>% 
  mutate(Year = year(DateTime)) %>% 
  filter(Year==2007 | Year==2008 | Year==2009) %>% 
  group_by(Year) %>% 
  summarize(SM1 = round(sum(Sub_metering_1 / 1000, na.rm = TRUE),3),
            SM2 = round(sum(Sub_metering_2 / 1000, na.rm = TRUE),3),
            SM3 = round(sum(Sub_metering_3 / 1000, na.rm = TRUE),3),
            DateTime = first(DateTime))
str(Yr.sum.wide)
# Energy Consumption (kWh) Yearly #
# Year | SM1 | SM2 | SM3  #
# 2007 | 643 | 854 | 3022 #
# 2008 | 585 | 662 | 3180 #
Yr.sum.long <- gather(Yr.sum.wide,key = "Sub_Meter",value = "Energy", SM1:SM3)
str(Yr.sum.long)

Weekday.average.wide <- yr_daily %>% 
  mutate(wDay = wday(DateTime,1)) %>% 
  filter(Month == 12 | Month == 1 | Month == 2) %>% 
  group_by(wDay) %>% 
  summarize(SM1 = round(mean(SM1),3),
            SM2 = round(mean(SM2),3),
            SM3 = round(mean(SM3),3),
            DateTime = first(DateTime))
Weekday.average.wide
# Weekday Energy Consumption Average (kWh) #
# Weekday | SM1 | SM2 | SM3   #
# Sunday  |3.151|3.248|10.214 #
# Monday  |1.190|0.817|10.977 #
Weekday.average.long <- gather(Weekday.average.wide,key = "Sub_Meter",value = "Energy", SM1:SM3)

Hourly.sum.wide <- yr_full %>% 
  mutate(Year = year(DateTime),Month = month(DateTime),Day = day(DateTime), Hour = hour(DateTime)) %>% 
  filter(Year != 2006) %>% 
  group_by(Year,Month,Day,Hour) %>% 
  summarize(SM1 = round(sum(Sub_metering_1 / 1000, na.rm = TRUE),3),
            SM2 = round(sum(Sub_metering_2 / 1000, na.rm = TRUE),3),
            SM3 = round(sum(Sub_metering_3 / 1000, na.rm = TRUE),3),
            DateTime = first(DateTime)) %>% 
  filter(!is.na(Day))
Hourly.sum.wide

Jan10Hourly.average.wide <- Hourly.sum.wide %>% 
  filter(Year == 2010 & Month == 1) %>% 
  group_by(Hour) %>% 
  summarize(SM1 = round(mean(SM1),3),
            SM2 = round(mean(SM2),3),
            SM3 = round(mean(SM3),3),
            DateTime = first(DateTime)) %>% 
  filter(!is.na(Hour))
Jan10Hourly.average.wide
# Average Energy Consumption (kWh) per Hour in January 2010 #
# Hour | SM1 | SM2 | SM3  #
# 0    |0.052|0.018|0.408 #
# 1    |0.000|0.015|0.304 #
Jan10Hourly.average.long <- gather(Jan10Hourly.average.wide,key = "Sub_Meter",value = "Energy", SM1:SM3)

FirstMonthDayNoon.wide <- yr_full %>% 
  mutate(Year = year(DateTime),Month = month(DateTime), Day = day(DateTime), Hour = hour(DateTime), Minute = minute(DateTime)) %>% 
  filter(Year == 2009 & Day == 1 & Hour == 12 & Minute == 0) %>% 
  group_by(Month) %>% 
  summarize(SM1 = Sub_metering_1,
            SM2 = Sub_metering_2,
            SM3 = Sub_metering_3)
FirstMonthDayNoon.wide
# Energy Consumption (Wh) on the first of every month of 2009 at noon #
# Month | SM1 | SM2 | SM3 #
# 1     | 0   | 0   | 0   #
# 2     | 38  | 0   | 0   #
# 3     | 1   | 2   | 18  #
FirstMonthDayNoon.long <- gather(FirstMonthDayNoon.wide,key = "Sub_Meter",value = "Energy", SM1:SM3)

###      ###
### Plot ###
###      ###

ggplot(Yr.sum.long,mapping = aes(Sub_Meter,Energy,group=Year,fill=factor(Year)))+
  geom_col(position = "dodge")+
  scale_fill_discrete(name = "Year")+
  labs(title="Annual Energy Consumption",x="Sub Meter", y="Energy Consumption (kWh)")

ggplot(Weekday.average.long,mapping = aes(wDay,Energy,group=Sub_Meter))+
  geom_line(aes(color=Sub_Meter))+
  geom_point(aes(color=Sub_Meter))+
  scale_color_discrete(name="Sub Meter")+
  labs(y="Energy Consumption (kWh)",title="Winter Weekday Average Energy Consumption")


ggplot(Jan10Hourly.average.long,mapping = aes(Hour,Energy,group=Sub_Meter))+
  geom_line(aes(color=Sub_Meter))+
  geom_point(aes(color=Sub_Meter))+
  labs(x="Hour",y="Energy Consumption (kWh)",title="Hourly Average Energy Consumption from January 2010")+
  scale_color_discrete(name="Sub Meter")

ggplot(FirstMonthDayNoon.long,mapping = aes(Month,Energy,group=Sub_Meter))+
  geom_line(aes(color=Sub_Meter))+
  geom_point(aes(color=Sub_Meter))+
  labs(title="1st of the Month Energy Consumption @ Noon in 2009",x="Month",y="Energy Consumption (Wh)")+
  scale_x_continuous(breaks = seq(1,12,1))+
  scale_color_discrete(name="Sub Meter")

###          ###
### Forecast ###
###          ###

### Forecasting 2010-2011 based on 2007-2009 Total Energy Consumption in kWh ###
yr_SM3 <- Yr.sum.wide %>% 
  select(-SM1,-SM3)
ts_SM3 <- ts(yr_SM3$SM2,start = c(2007,1))
fitSM3 <- tslm(ts_SM3 ~ trend)
summary(fitSM3)
forecastfitSM3 <- forecast(fitSM3, h=2)
plot(forecastfitSM3)
summary(forecastfitSM3)
# Annual Confidence Level: 85% #
# Point Forecast for 2010 & 2011 Energy Consumption #
# 2010 | 3786.881 #
# 2011 | 4053.869 #

### Forecasting 2010 November thru 2011 December based on 2007 January thru 2010 October Energy Consumption in kWh ###
month_SM3 <- yr_daily %>% 
  filter(Year == 2007 | Year == 2008 | Year == 2009 | Year == 2010 & Month != 11) %>% 
  group_by(Month,Year) %>% 
  summarize(SM3 = round(sum(SM3, na.rm = TRUE),3),
            Date = as.Date((format(first(DateTime),"%Y-%m-%d")))) %>% 
  arrange(Date)
ts_monthSM3 <- ts(month_SM3$SM3, start=c(2007,1),end =c(2010,10),frequency = 12)
str(ts_monthSM3)
fitSM3_month <- tslm(ts_monthSM3 ~ trend + season)
summary(fitSM3_month)
forecastfitSM3_month <- forecast(fitSM3_month, h= 14)
summary(forecastfitSM3_month)
plot(forecastfitSM3_month)
# Point Forecast for Monthly Energy Consumption #
# Nov 2010 | 0.3517 #
# Dec 2010 | 0.3970 #
# Jan 2011 | 0.3995 #
# Feb 2011 | 0.3660 #

###               ###
### Decomposition ###
###               ###

### Plots & Outputs in Excel File ###

componentsSM3_monthly <- decompose(ts_monthSM3)
plot(componentsSM3_monthly)
summary(componentsSM3_monthly)

SM3_hourly_Feb10 <- Hourly.sum.wide %>% 
  filter(Year == 2010 & Month == 2) %>% 
  group_by(Hour,Day) %>% 
  summarize(SM3 = round(sum(SM3 / 1000, na.rm = TRUE),3),
            DateTime = first(DateTime)) %>% 
  arrange(DateTime)
str(SM3_hourly_Feb10)
tsSM3_hourly_Feb10 <- ts(SM3_hourly_Feb10$SM3,start = 1,end=29,frequency = 24)
componentsSM3_hourly <- decompose(tsSM3_hourly_Feb10)
plot(componentsSM3_hourly)
summary(componentsSM3_hourly)
str(componentsSM3_hourly)

###    ###
### HW ###
###    ###

### Plots on PDF ###

### plotForecastErrors Function from LBR ###
plotForecastErrors <- function(forecasterrors)
{
  # make a histogram of the forecast errors:
  mybinsize <- IQR(forecasterrors)/4
  mysd   <- sd(forecasterrors)
  mymin  <- min(forecasterrors) - mysd*5
  mymax  <- max(forecasterrors) + mysd*3
  # generate normally distributed data with mean 0 and standard deviation mysd
  mynorm <- rnorm(10000, mean=0, sd=mysd)
  mymin2 <- min(mynorm)
  mymax2 <- max(mynorm)
  if (mymin2 < mymin) { mymin <- mymin2 }
  if (mymax2 > mymax) { mymax <- mymax2 }
  # make a red histogram of the forecast errors, with the normally distributed data overlaid:
  mybins <- seq(mymin, mymax, mybinsize)
  hist(forecasterrors, col="red", freq=FALSE, breaks=mybins)
  # freq=FALSE ensures the area under the histogram = 1
  # generate normally distributed data with mean 0 and standard deviation mysd
  myhist <- hist(mynorm, plot=FALSE, breaks=mybins)
  # plot the normal curve as a blue line on top of the histogram of forecast errors:
  points(myhist$mids, myhist$density, type="l", col="blue", lwd=2)
}


### Winter 09/10 Subset ###
Winter09_10 <- yr_daily %>% 
  filter(Year == 2009 & Month == 12 | Year == 2010 & Month %in% c(1,2)) %>%
  group_by(Year, Month, Day) %>% 
  summarize(SM3 = round(SM3,3))
ts_Winter <- ts(Winter09_10$SM3,
                frequency = 7)
ts_Winter_components <- decompose(ts_Winter)
ts_Winter_ns <- ts_Winter - ts_Winter_components$seasonal
HW_Winter <- HoltWinters(ts_Winter_ns,gamma = FALSE, beta = FALSE)
plot(HW_Winter)
HWforecast_Winter <- forecast(HW_Winter, h = 30)
plot(HWforecast_Winter)
acf(HWforecast_Winter$residuals, na.action = na.pass, lag.max = 20)
Box.test(HWforecast_Winter$residuals, lag = 20, type = "Ljung-Box")
plot.ts(HWforecast_Winter$residuals)
plotForecastErrors(na.exclude(HWforecast_Winter$residual))
# Box Test Metrics #
# X-squared: 18.323 # 
# p-value: 0.5662 #

### Spring 2010 Subset ###
Spring10 <- yr_daily %>%
  filter(Year == 2010 & Month %in% c(3,4,5)) %>% 
  group_by(Year, Month, Day) %>% 
  summarize(SM3 = round(SM3,3))
ts_Spring <- ts(Spring10$SM3,
                frequency = 7)
ts_Spring_components <- decompose(ts_Winter)
ts_Spring_ns <- ts_Spring - ts_Spring_components$seasonal
HW_Spring <- HoltWinters(ts_Spring_ns, gamma = FALSE, beta = FALSE)
plot(HW_Spring)
HWforecast_Spring <- forecast(HW_Spring, h = 30)
plot(HWforecast_Spring)
acf(HWforecast_Spring$residuals, na.action = na.pass, lag.max = 20)
Box.test(HWforecast_Spring$residuals,lag = 20, type = "Ljung-Box")
plot.ts(HWforecast_Spring$residuals)
plotForecastErrors(na.exclude(HWforecast_Spring$residuals))
# Box Test Metrics #
# X-squared: 30.205 #
# p-value: 0.0666 #

### Summer 2010 Subset ###
Summer10 <- yr_daily %>% 
  filter(Year == 2010 & Month %in% c(6,7,8)) %>% 
  group_by(Year, Month, Day) %>% 
  summarize(SM3 = round(SM3,3))
ts_Summer <- ts(Summer10$SM3,
                frequency = 7)
ts_Summer_components <- decompose(ts_Summer)
ts_Summer_ns <- ts_Summer - ts_Summer_components$seasonal
HW_Summer <- HoltWinters(ts_Summer_ns, gamma = FALSE, beta = FALSE)
plot(HW_Summer)
HWforecast_Summer <- forecast(HW_Summer, h = 30)
plot(HWforecast_Summer)
acf(HWforecast_Summer$residuals, na.action = na.pass, lag.max = 20)
Box.test(HWforecast_Summer$residuals, lag = 20, type = "Ljung-Box")
plot.ts(HWforecast_Summer$residuals)
plotForecastErrors(na.exclude(HWforecast_Summer$residuals))
# Box Test Metrics #
# X-squared: 10.262 #
# p-value: 0.9632 #

### Fall 2010 Subset ### 
Fall10 <- yr_daily %>%
  filter(Year == 2010 & Month %in% c(9,10,11)) %>% 
  group_by(Year, Month, Day) %>% 
  summarize(SM3 = round(SM3,3))
ts_Fall <- ts(Fall10$SM3,
              frequency = 7)
ts_Fall_components <- decompose(ts_Fall)
ts_Fall_ns <- ts_Fall - ts_Fall_components$seasonal
HW_Fall <- HoltWinters(ts_Fall_ns, gamma = FALSE, beta = FALSE)
plot(HW_Fall)
HWforecast_Fall <- forecast(HW_Fall, h = 30)
plot(HWforecast_Fall)
acf(HWforecast_Fall$residuals, na.action = na.pass, lag.max = 20)
Box.test(HWforecast_Fall$residuals, lag = 20, type = "Ljung-Box")
plot.ts(HWforecast_Fall$residuals)
plotForecastErrors(na.exclude(HWforecast_Fall$residuals))
# Box Test Metrics #
# X-squared: 17.913 #
# p-value: 0.5931 #