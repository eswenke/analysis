# Impact of Payment vs No Payment on Steam Game Reviews
Ethan Swenke

## Idea
In the Steam reviews dataset, there are several columns that give details about the author of every review for any given game. An interesting column that caught my eye was the 'steam_purchased' and 'received_for_free' columns, which indicated if the author has spent money on the game. I was intrigued by the idea of not purchasing a game and the effects this would have on the overall positive or negative reviews of games. I ended up collecting data on the games that at least 100 reviews and an average positive review rating of between 60 and 40 percent. My thought behind this was to look at games that weren't necessarily reviewed overwhelmingly positive so that the idea of spending money on the game might actually influence the review a little more than a game that is enjoyed by all regardless. Unfortunately we only know if the review was positive or not, so there won't be a sliding scale to a game's reception from the audience, but we can get a good idea. My initial hypothesis was that the reviewers who did not pay for the game would generally give more positive reviews on average because there is less of a pressure when purchasing the game to and expecting enjoyment.

## Implementation
In order to analyze this, I used DuckDB and ran a series of queries on the Steam reviews dataset to get all games with more than 100 reviews and an average positive rating (APR) between 40 and 60 percent. Each review had a binary upvote or downvote on the game, so I calculated average positive rating by seeing how many reviews had an upvote attached and then taking the average from all reviews for each game. With this set of games, I created dataframes containing the APR for those who had the 'steam_purchased' and 'received_for_free' boxes checked in such ways that the author either paid for the game or did not (there was some extra logic here to consider with there being a difference between someone who checked the 'received_for_free' box but still said they purchased the game and vice versa). With the two dataframes of those who purchased the game they reviewed and those who did not, I created a joined dataframe to better visualize the differences in APR and ran some analysis on it.

## Results
### Percentiles
I ended up finding statistics on a variety of different filtered versions of the joined dataframe to find results that could describe how the data looked with things like the top 30 most reviewed games and games with reviews more than 1000, 10000, and 50000.

To begin, though, lets look at the average APR difference between those that purchased their games and those that did not (for the entire joined dataframe)

- APR of reviewers who purchased: 53.816771
- APR of reviewers who did not purchased: 50.915774
- Difference: 2.900997

So those who did not pay for their games and essentially got them for free actually had an APR of nearly 3% less than those that did purchase the game. This number fluctuates between 2.5% and 3.1% depending on how you filter the joined dataframe as I mentioned above, but the result is consistently less than the APR of those who purchased their games. Here are some more specifics on the difference based on different filters:

- Difference when filtering on games with > 1000 reviews: 2.574916
- Difference when filtering on games with > 10000 reviews: 2.925769
- Difference when filtering on games with > 50000 reviews: 2.390343
- Difference when filtering on top 30 most reviewed games: 3.175662

Here are the top 3 top differences for games where purchasers enjoyed the game less than those who didn't purchase, and vice versa (no review filtering so as to consider the extremes):

Games where purchasers enjoyed the game less and the difference in APR:
1. Sid Meier’s Civilization® VI Leader Pass: 30.959453% lower APR
2. Airport Firefighters - The Simulation: 23.720893 lower APR
3. Company of Heroes 2 - Ardennes Assault: 23.427967 lower APR

Games where the purchasers enjoyed the game more and the difference in APR:
1. The Anacrusis: 42.429116% higher APR
2. Grapple: 38.770448% higher APR
3. Capcom Arcade Stadium: 34.293878% higher APR

So even the outliers for APR difference show that those who got the game for free are, on average, more dissatisfied than those who purchased the game.

And finally, here is a bar chart of the top 30 most reviewed games and the APR differences between those who did not purchase and those who did purchase. Those values below the x axis are the negative APR differences, which signify those who did not purchase had an average APR of that much percent lower than those who did purchase.

![alt text](image.png)

As you can see, there is a trend supporting the findings that those who got games for free were far more likely to leave a negative review.

## Conclusion
The analysis on this steam reviews dataset contradicts my initial hypothesis. I thought those who got games for free might enjoy the games more due to less pressure to enjoy the game, but I was proven wrong. Those who did not pay for the games, on average, got less enjoyment out of them as indicated by their reviews. I believe the reason for this is actually quite simple. When I consider the psychology behind this, my thought is that those who do pay for the game actually subconsciously convince themselves to like their games just a little bit more because they have to justify the payment they made. Those who did not pay have no reason to justify their own enjoyment of the game, and thus are more likely to dislike their playtime and leave a negative review. This makes sense in retrospect, although is hard to prove without taking a look at the reviews themselves. This is outside the current scope of the analysis and would require a lot more time, given that not every review will even mention explicitly that they didn't pay (and use this to show they had no need for justification), and so that is not always a confirmed reason to consider for less enjoyment. The visualization of this was difficult because of the small difference in APR, although the numbers speak for themselves. Either way, this was an informative analysis with a conclusion that suggests a psychological hypothesis that can apply to just about any purchaseable experience.