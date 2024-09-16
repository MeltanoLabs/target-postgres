# target-postgres Performance Analysis

Just a POC right now.

Main goal is to lay out an objective way to do performance analysis with target-postgres, and hopefuly the ground work for others if they want to do analysis with their target's. 

Main points:
1. We need something to comapre to. For postgres we have native import commands that are well optimized. We will use this as a baseline. 
1. Relative speed is the metric to focus on. If we focus on absolute speed then there's a bunch of hardware consideration that we are not trying to solve here (Would need to consider how paralleization fits into the mix here if we go there)

# Why do this work?
1. Without it we are guessing at what can help improve performance, this gives us a more objective way to pick what we should focus on 

# How to run
1. `./prep.sh` gets the data together for you in the right place
2. `./speed_compare.sh meltano_import.sh native_import.sh` runs each and gives you a nice time comparisons

# Results on my slow machine



# Other questions / concerns
1. `COPY` is single threaded, there's no reason we need to stick to a single thread. https://github.com/dimitri/pgloader is much faster. We should try this out as well
1. `prep.sh`'s tap-csv step runs to give us a data.singer file (jsonl output from the tap) this takes an extremely long time to run for one million records
kk
