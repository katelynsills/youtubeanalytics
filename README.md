Python Youtube Analytics Download Script
---

Feel free to use this code to use Youtube Analytics API to download data from your channel. If you do use it, do me a favor and let me know at katelynsills@gmail.com.

Below are SQL statements for creating the tables.

***
The 6 Tables:

1. VidGeneralMetrics
2. VidInsightPlaybackLocationType
3. VidInsightTrafficSourceType
4. VidAgeGroupGener
5. VidSharingService
6. VidCountry

 
VidGeneralMetrics
 
CREATE TABLE VidGeneralMetrics
(
Day datetime,
VideoID varchar(32),
Views int,
Comments int, 
FavoritesAdded int, 
FavoritesRemoved int,
Likes int,
Dislikes int,
Shares int,
EstimatedMinutesWatched int,
AverageViewDuration int,
AverageViewPercentage int,
AnnotationClickThroughRate int,
AnnotationCloseRate int,
SubscribersGained int,
SubscribersLost int,
Uniques int
);

                                               
 
VidInsightPlaybackLocationType
 
CREATE TABLE VidInsightPlaybackLocationType
(
Day datetime,
VideoID varchar(32),
InsightPlaybackLocationType varchar(128),
Views int,
EstimatedMinutesWatched int
);
 
Dimensions = "day,insightPlaybackLocationType"
Metrics =  "views,estimatedMinutesWatched"
Filters = video_id
 
VidInsightTrafficSourceType
 
CREATE TABLE VidInsightTrafficSourceType
(
Day datetime,
VideoID varchar(32),
InsightTrafficSourceType varchar(128),
Views int,
EstimatedMinutesWatched int
);
 
Dimensions = "day,insightTrafficSourceType"
Metrics = "views,estimatedMinutesWatched"
Filters = video_id
 
VidAgeGroupGender
 
CREATE TABLE VidAgeGroupGender
(
Month int,
Year int,
VideoID varchar(32),
AgeGroup varchar(128),
Gender varchar(128),
ViewerPercentage float
);
 
Dimensions = "ageGroup,gender"
Metrics= "viewerPercentage"
Filters = video_id
Loop by startdate and enddate to get individual days
 
VidSharingService
 
CREATE TABLE VidSharingService
(
Month int,
Year int,
VideoID varchar(32),
SharingService varchar(128),
Shares int
);
 
Dimension: sharingService
Metrics: shares
Filters = video_id
Loop by startdate and enddate to get individual days
 
VidCountry
 
CREATE TABLE VidCountry
(
Month int,
Year int,
VideoID varchar(32),
Country varchar(128),
Views int,
Comments int, 
FavoritesAdded int, 
FavoritesRemoved int,
Likes int,
Dislikes int,
Shares int,
EstimatedMinutesWatched int,
AverageViewDuration int,
AverageViewPercentage int,
AnnotationClickThroughRate int,
AnnotationCloseRate int,
SubscribersGained int,
SubscribersLost int
);
 
Dimensions: Country
Metrics: "views,comments,favoritesAdded,favoritesRemoved,likes,dislikes,shares,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,annotationClickThroughRate,annotationCloseRate,subscribersGained,subscribersLost"
Filters = video_id
Loop by startdate and enddate to get individual days

 
 
CREATE TABLE VidAgeGroup
(
Month int,
Year int,
AgeGroup varchar(128),
Gender varchar(128),
ViewerPercentage float
);
 
CREATE TABLE MonthlyViews ( Month int, Year int, ChannelID varchar(32), ChannelName varchar(128), Views int);
 
 
CREATE TABLE VideoInfo (VideoID varchar(32), Name varchar(128), PublishedDate datetime);
 
CREATE TABLE TwitterFollowers (Day datetime, FollowersCount int);
 
CREATE TABLE YoutubeOverall (Day datetime, subscriberCount int, totalUploadViews int);
 
