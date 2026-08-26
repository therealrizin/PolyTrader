Create table of 300s, with separated % chance of happening. For example 450k cases were 0,1% or more, than with 1m of records it will be 40% chance. But only 20k were 1% or more so it will be 2% chance. Limit with 3%? I guess 3% change in 5 min is rare occurence so it will be sufficient

	-3% and worse | -2,9%- | ... | 2,9%+ | 3%+
1s
2s
...
300s

2 weeks of data for ~1,2 mil feeds. Shows latest trend, 1 mil records should be enough to predict (it will be over 1 mil for each Xs variation since it's moving windows)

So... take price from 5 main sources, gather historical data, then get 60s avarage, compare to 60s avarage I have access via polymarket, set direction, add bonus (or minus) for difference between my data and 60s avg from polymarket, predict price change of avarage 60s change, check current stakes on polymarket. If chanceOfHappening > treshold (probably around 25% to do not risk 1% occurences with 1000% theoretical profit, maybe if it will be winning will change it later) + expectedProfitFromBet > threshold (probably 20% at the start to avoid bad beats) then put a bet.

Betting feed later:
based on historical data + polymarket fees set best treshold for occurence + expectedProfit to maximize winnings