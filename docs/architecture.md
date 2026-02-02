# Architecture justification

## ERD

Cf. **er_diagram** png for the entity relationship diagram.

## Why the star scheme?

I chose the star scheme for this project mainly due to the following considerations:

- it allows better **analytical queries performance** which is highlighted throughout the project's guidance as one of its key points. Moreover, the aforementioned queries will be easier to write since the fact table directly connects to all dimensions (less joins to perform vs. the snowflake scheme);
- it is **easier to implement** (big deal for me considering a tight schedule of the master's :) as well as to maintain by simply adding new dimensions and/or reusing the existing ones for new fact tables;
- given the Drug Store's dataset size, **redundancy** caused by the use of the star scheme can be seen as negligible. The dimensions' denormalization benefits are in this case more important than the storage cost (besides, nowadays the value of the storage cost is often perceived as trivial).
