# Knowledgebase Experimentation for the Environmental Enforcement Watch project

This project develops a workflow to build the fundamental data the EEW project is integrating and making sense of from the USEPA's ECHO platform and other sources. It operates against the https://eew-edgi.wikibase.cloud instance of Wikibase. Read more about the project on that wikibase instance's front page.

## Re-use of these codes

Anyone is welcome to take and build on anything I'm doing here - it is Unlicensed. I'm not currently building any of this as a deployable codebase, and you won't be able to run everything here unless you build it to operate on your own Wikibase instance (local of wikibase.cloud). I use environment variables in whatever platform I'm executing this on (currently a Pangeo environment via the ESIPLab) to store access information. You'd have to rework that according to your own preferences.

## Some principles I'm figuring out here

My notebooks will have specific text and notes on what I'm working through. I sometimes come back to the readme with things I've worked out as general principles.

* We need a few hard and fast constraints to make all of the automated parts of this work.
    * Every source needs to be an item that can be pointed to with sufficient detailed characteristics to link to how things came to be in the knowledgebase. I'm trying to drive actual processing from the content of the items to make this reality.
    * It's better to break processes up for clarity and simplicity. Do one thing and one thing only, leaving other processing for subsequent operations. For instance, bring in county records based solely on what is in the specific source. That's complex enough in that we have to stitch together 55 individual data tables pulled from web pages. We can then run further processing to add in the state/territory linkage as its own "linking microservice." The information is in the source tables in the form of code values, but it's cleaner to leave off inter-KB linking as its own thing.
* Sometimes "simplistic" data access methods are okay
    * It initially seemed illogical to use what amounts to a web scraping method to get tabular data from the U.S. Census TIGER data as opposed to using their web services. However, in exploring those services, I found them to be in the same kind of shape that I've found elsewhere - they are fundamentally GIS services set up and tuned to drive GIS applications; they are not data distribution or general access data services. Sure, I could write code that would use the ArcGIS Server REST APIs to return JSON and process that just fine. But that requires a bunch of complicated parameterization and fundamentally isn't very different from any other HTTP call. We might also find that those services are even more "brittle" in terms of ongoing change than the cached HTML tables I ended up using. Since those pages with single HTML tables are actually advertised on the web site as a point of data access, it seems like a reasonanble way to go about this. I did use the specialization of Pandas read_html method here, which is a nontrivial dependency, but it works and makes things fairly efficient.
