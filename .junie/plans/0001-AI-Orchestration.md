# AI Orchestration

## Introduction

Because the job of a software developer recently has
drastically shifted from writing code to writing essays,
and because I have a pretty vague idea of how the
development process that I aim to implement will look
like, and because I'm not familiar with any 
"best-practice" projects where it's already set up, and
I don't want to give the initiative in a such important
architectural decision to the AI agents themselves,
I'm starting this essay.

The goal, however, is not writing itself. The goal is
to set up a nice, effective, transparent, cheap,
flexible, extendable process for the development
of [my-handicapped-pet.io](https://my-handicapped-pet.io),
that also can be adapted for other projects of similar
size.

Because I already had some problems with interpretation
of the documents written for both humans and AI agents,
I feel a necessity to clarify prepositions,
like in legal documents. "I" means me, Ilya L.,
the solo (as for 2026-09-30) developer of this project.
"You", if encountered anywhere in the text, likely
means the metaphorical Socratic conversation peer,
but can be defaulted to any individual reader,
never mind human or bot.

However, if you are an AI agent, editing this document,
please follow the following guidelines:
 - edit only sections which title specifically says
*Written by the AI Agent*;
 - keep nice 60-character-per-line formatting,
unless for long links or stuff styled as `code`;
 - be high-level. Implementation details can be further
clarified in the tasks.

Beside of the necessity, this writing is inspired
by a few Internet posts, among them 
[Chad Arimura's blog post](https://chad.cm/posts/2026-8-11-my-agent-setup)
that I encountered on Hacker News, and the diary
of [Nancy Sadkov](https://lj.rossia.org/users/nancygold/)
who writes, beside of her magnificent life experience,
about the revival of her old software projects with the
power of the modern agentic coding.

## Current Setup and Requirements

Currently I use an oldschool way of doing agentic coding:
open chat in my IDE, ask the AI agent to write
requirements for a new feature or change request,
or to analyze and edit a document I've already started.
Then, after we've got detailed enough requirements,
I ask to write the code, to test it internally with unit
tests, then I test it end-to-end (normally, manually),
deploy to the staging and then to the prod.

While this process speeds up development significantly,
it leaves clear room for improvement. I'll list 
one-by-one items that I wish to improve, and also
common requirements.

1. The project consists of several repos (the full list
is within GitHub 
[my-handicapped-pet](https://github.com/orgs/my-handicapped-pet/repositories)
organization). AI agent within the IDE is scoped to 
a single IDE's project, therefore, to a single repo. 
The obvious solution is to open the IDE with the root
that contains all repos, but it's not necessarily
the best. The goal is to keep interfaces between
the parts of the project (e.g. between backend
and frontend) clear i.e. minimalistic in terms 
of technical design and effective in terms of runtime.
So, it's probably better to keep a separate agent for
each repo, and allow them to communicate with each other.
2. Planning. Currently, planning is started by me with
a high-level or sometimes more detailed proposition,
then I together with the AI agent update the docs
back and forth recursively. Basically, this process
is synchronous and requires human attendance. I still
want a human to be mandatory in the workflow before
implementation, but maybe introducing another agent
can reduce the number of iterations and give the
higher quality design for the human review.
Then, the reviewer can accept the design, criticize it,
or amend or rewrite it manually.
3. Marketing. This might be not an obvious part
of the development workflow, but AI agents can search
the web, find similar projects (competitors, or
projects to merge/integrate with), potential users,
potential use cases, potential places 
to promote the project. Also, the marketing agent
may suggest features to the planning agent.
4. Durability. Asynchronous and independent agentic work 
will require hosting agents in the cloud; the developer
workstation must be removed from the loop. So, we'll
need some service or solution to host the agents.
5. Messaging. The AI agents must be able to communicate
to each other; the developers must be able to read their
communications and communicate to them; the AI agents
must be able to summon the developer when they need
human attention. Therefore, we need to set up 
some bot-friendly messenger.
6. Documentation. After the feature is implemented, its
documentation must follow. A bunch of MD files is
technically OK, but in the current structure it is
just a task log, not organized around the project 
structure.
7. Price. This is a non-commercial pet project. 
Currently it costs me about $30 per month for the AWS
hosting. If AI agent infrastructure adds some expenses,
they should be the same order of magnitude.
8. Deployments. Currently deployments are triggered
by pushing to the `develop` branch. We can keep this
logic, if the AI agents are able to make pull requests.
9. Testing. After deployment to the staging, we can
trigger a specific testing AI agent, that plans and
executes end-to-end tests. A more sophisticated
alternative is for the agents to create a temporary
dev environment in their own cloud.

The first implementation mustn't necessarily implement
all requirements listed above. We can start with some
reasonable setup that implements a part of them.

## Implementation Proposals (Written by the AI Agent)
