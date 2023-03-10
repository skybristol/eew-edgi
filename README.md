# Knowledgebase Experimentation for the Environmental Enforcement Watch project

This project develops a workflow to build the fundamental data the EEW project is integrating and making sense of from the USEPA's ECHO platform and other sources. It operates against a wikibase.cloud instance of Wikibase.

Some rules I'm figuring out here:

* We need a few hard and fast constraints to make all of the automated parts of this work.
    * Every source needs to be an item that can be pointed to with sufficient detailed characteristics to link to how things came to be in the knowledgebase. I'm trying to drive actual processing from the content of the items to make this reality.
    * It's better to break processes up for clarity and simplicity. Do one thing and one thing only, leaving other processing for subsequent operations. For instance, bring in county records based solely on what is in the specific source. That's complex enough in that we have to stitch together 55 individual data tables pulled from web pages. We can then run further processing to add in the state/territory linkage as its own "linking microservice." The information is in the source tables in the form of code values, but it's cleaner to leave off inter-KB linking as its own thing.
